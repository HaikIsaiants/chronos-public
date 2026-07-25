package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HaikIsaiants/chronos/internal/bench"
	"github.com/HaikIsaiants/chronos/internal/core"
)

func main() {
	var coordinator string
	var namespaceCount int
	var workflows int
	var concurrency int
	var taskType string
	var prefix string
	var wait bool
	var localExecution bool
	var batchSize int
	var startIndex uint64
	var timeout time.Duration
	flag.StringVar(&coordinator, "coordinator", "", "coordinator HTTP URL")
	flag.IntVar(&namespaceCount, "namespaces", 3, "namespace count")
	flag.IntVar(&workflows, "workflows", 1000, "workflow count")
	flag.IntVar(&concurrency, "concurrency", 16, "submission concurrency")
	flag.StringVar(&taskType, "task-type", "work", "task type")
	flag.StringVar(&prefix, "prefix", "load", "request id prefix")
	flag.BoolVar(&wait, "wait", false, "wait for workflow completion")
	flag.BoolVar(&localExecution, "local-execution", false, "execute deterministic workflows through the local execution API")
	flag.IntVar(&batchSize, "batch-size", int(bench.WorkloadBatchSize), "workflows per local execution batch")
	flag.Uint64Var(&startIndex, "start-index", 0, "first deterministic workflow index")
	flag.DurationVar(&timeout, "timeout", 2*time.Minute, "end-to-end timeout")
	flag.Parse()
	if coordinator == "" || namespaceCount < 1 || workflows < 1 || concurrency < 1 || taskType == "" || prefix == "" || timeout <= 0 || batchSize < 1 || uint64(batchSize)*bench.WorkloadCommandsPerWorkflow > 256 {
		fmt.Fprintln(os.Stderr, "usage: chronos-load -coordinator <url> [-namespaces n] [-workflows n] [-concurrency n] [-task-type type]")
		os.Exit(2)
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client := &http.Client{Timeout: 30 * time.Second}
	completed := 0
	transitions := uint64(0)
	var err error
	if localExecution {
		transitions, err = execute(ctx, client, coordinator, startIndex, workflows, concurrency, batchSize)
		completed = workflows
	} else {
		err = submit(ctx, client, coordinator, namespaceCount, workflows, concurrency, taskType, prefix)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if wait && !localExecution {
		if err := waitCompleted(ctx, client, coordinator, namespaceCount, workflows, prefix); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		completed = workflows
	}
	duration := time.Since(started)
	if localExecution {
		fmt.Printf("workflows=%d transitions=%d duration=%s workflows_per_second=%.3f transitions_per_second=%.3f\n", workflows, transitions, duration.Round(time.Millisecond), float64(workflows)/duration.Seconds(), float64(transitions)/duration.Seconds())
		return
	}
	fmt.Printf("submitted=%d completed=%d duration=%s workflows_per_second=%.3f\n", workflows, completed, duration.Round(time.Millisecond), float64(workflows)/duration.Seconds())
}

type executionBatch struct {
	start uint64
	count uint64
}

func execute(ctx context.Context, client *http.Client, coordinator string, start uint64, workflows, concurrency, batchSize int) (uint64, error) {
	jobs := make(chan executionBatch)
	var transitions atomic.Uint64
	var failed atomic.Bool
	var first error
	var mu sync.Mutex
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batch := range jobs {
				if failed.Load() {
					continue
				}
				commands := bench.WorkloadBatch(batch.start, batch.count)
				data, err := json.Marshal(struct {
					Commands []core.Command `json:"commands"`
				}{Commands: commands})
				if err == nil {
					var request *http.Request
					request, err = http.NewRequestWithContext(ctx, http.MethodPost, coordinator+"/v1/batches", bytes.NewReader(data))
					if err == nil {
						request.Header.Set("Content-Type", "application/json")
						var response *http.Response
						response, err = client.Do(request)
						if err == nil {
							var body struct {
								Results []core.Result `json:"results"`
							}
							decodeErr := json.NewDecoder(response.Body).Decode(&body)
							response.Body.Close()
							if decodeErr != nil {
								err = decodeErr
							} else if response.StatusCode != http.StatusOK {
								err = fmt.Errorf("batch starting at %d returned %s", batch.start, response.Status)
							} else if len(body.Results) != len(commands) {
								err = fmt.Errorf("batch starting at %d returned %d of %d results", batch.start, len(body.Results), len(commands))
							} else {
								count := uint64(0)
								for _, result := range body.Results {
									count += uint64(len(result.Events))
								}
								expected := batch.count * bench.WorkloadTransitionsPerWorkflow
								if count != expected {
									err = fmt.Errorf("batch starting at %d produced %d of %d transitions", batch.start, count, expected)
								} else {
									transitions.Add(count)
								}
							}
						}
					}
				}
				if err != nil && failed.CompareAndSwap(false, true) {
					mu.Lock()
					first = err
					mu.Unlock()
				}
			}
		}()
	}
	remaining := workflows
	index := start
	for remaining > 0 && !failed.Load() {
		count := min(remaining, batchSize)
		jobs <- executionBatch{start: index, count: uint64(count)}
		index += uint64(count)
		remaining -= count
	}
	close(jobs)
	workers.Wait()
	mu.Lock()
	defer mu.Unlock()
	return transitions.Load(), first
}

func submit(ctx context.Context, client *http.Client, coordinator string, namespaceCount, workflows, concurrency int, taskType, prefix string) error {
	jobs := make(chan int)
	var failed atomic.Bool
	var first error
	var mu sync.Mutex
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				if failed.Load() {
					continue
				}
				definition := core.WorkflowDefinition{
					Name: "load", Namespace: fmt.Sprintf("namespace-%d", index%namespaceCount),
					Tasks: []core.TaskDefinition{{
						ID: "task", Type: taskType,
						Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
					}},
				}
				command := core.Command{Kind: core.CommandSubmit, RequestID: fmt.Sprintf("%s-%d", prefix, index), Definition: &definition}
				data, err := json.Marshal(command)
				if err == nil {
					var request *http.Request
					request, err = http.NewRequestWithContext(ctx, http.MethodPost, coordinator+"/v1/commands", bytes.NewReader(data))
					if err == nil {
						request.Header.Set("Content-Type", "application/json")
						var response *http.Response
						response, err = client.Do(request)
						if err == nil {
							body, readErr := io.ReadAll(response.Body)
							response.Body.Close()
							if readErr != nil {
								err = readErr
							} else if response.StatusCode != http.StatusOK {
								err = fmt.Errorf("submission %d returned %s: %s", index, response.Status, body)
							}
						}
					}
				}
				if err != nil && failed.CompareAndSwap(false, true) {
					mu.Lock()
					first = err
					mu.Unlock()
				}
			}
		}()
	}
	for index := range workflows {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	mu.Lock()
	defer mu.Unlock()
	return first
}

func waitCompleted(ctx context.Context, client *http.Client, coordinator string, namespaceCount, workflows int, prefix string) error {
	pending := make(map[string]struct{}, workflows)
	for index := range workflows {
		workflowID := core.WorkflowID(fmt.Sprintf("namespace-%d", index%namespaceCount), fmt.Sprintf("%s-%d", prefix, index))
		pending[workflowID] = struct{}{}
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for len(pending) > 0 {
		for workflowID := range pending {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, coordinator+"/v1/workflows/"+workflowID, nil)
			if err != nil {
				return err
			}
			response, err := client.Do(request)
			if err != nil {
				return err
			}
			var body struct {
				State *core.WorkflowState `json:"state"`
				Found bool                `json:"found"`
			}
			err = json.NewDecoder(response.Body).Decode(&body)
			response.Body.Close()
			if err != nil || response.StatusCode != http.StatusOK || !body.Found || body.State == nil {
				return fmt.Errorf("workflow %s read failed: %s", workflowID, response.Status)
			}
			switch body.State.Status {
			case core.WorkflowCompleted:
				delete(pending, workflowID)
			case core.WorkflowFailed, core.WorkflowCancelled, core.WorkflowCompensated:
				return fmt.Errorf("workflow %s ended as %s", workflowID, body.State.Status)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}
