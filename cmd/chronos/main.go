package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/HaikIsaiants/chronos-public/internal/core"
)

type runOutput struct {
	WorkflowID string              `json:"workflow_id"`
	StateHash  string              `json:"state_hash"`
	ReplayHash string              `json:"replay_hash"`
	State      *core.WorkflowState `json:"state"`
	Projection core.Projection     `json:"projection"`
	Metrics    core.Metrics        `json:"metrics"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var output any
	var err error
	switch os.Args[1] {
	case "run":
		if len(os.Args) != 3 {
			usage()
			os.Exit(2)
		}
		output, err = run(os.Args[2])
	case "submit":
		output, err = remoteSubmit(os.Args[2:])
	case "inspect":
		output, err = remoteInspect(os.Args[2:])
	case "events":
		err = remoteEvents(os.Args[2:], os.Stdout)
	case "cancel":
		output, err = remoteCancel(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if output == nil {
		return
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: chronos <run|submit|inspect|events|cancel> [options]")
}

type apiClient struct {
	base string
	http *http.Client
}

type apiCommandResponse struct {
	Results       []core.Result      `json:"results,omitempty"`
	Commits       []apiCommandCommit `json:"commits,omitempty"`
	LeaderID      uint64             `json:"leader_id,omitempty"`
	AppliedIndex  uint64             `json:"applied_index,omitempty"`
	CommittedTerm uint64             `json:"committed_term,omitempty"`
	Code          string             `json:"code,omitempty"`
	Message       string             `json:"message,omitempty"`
}

type apiCommandCommit struct {
	RequestID     string `json:"request_id"`
	AppliedIndex  uint64 `json:"applied_index"`
	CommittedTerm uint64 `json:"committed_term"`
}

type apiWorkflowResponse struct {
	State      *core.WorkflowState `json:"state,omitempty"`
	Projection core.Projection     `json:"projection"`
	Found      bool                `json:"found"`
	LeaderID   uint64              `json:"leader_id,omitempty"`
	Code       string              `json:"code,omitempty"`
	Message    string              `json:"message,omitempty"`
}

type apiError struct {
	status int
	text   string
}

func (e apiError) Error() string {
	return e.text
}

func remoteSubmit(arguments []string) (apiCommandResponse, error) {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	coordinator := flags.String("coordinator", "http://127.0.0.1:7101", "coordinator URL")
	requestID := flags.String("request-id", "", "stable request identifier")
	if err := flags.Parse(arguments); err != nil {
		return apiCommandResponse{}, err
	}
	if flags.NArg() != 1 || strings.TrimSpace(*requestID) == "" {
		return apiCommandResponse{}, fmt.Errorf("submit requires --request-id and one workflow file")
	}
	data, err := os.ReadFile(flags.Arg(0))
	if err != nil {
		return apiCommandResponse{}, err
	}
	var definition core.WorkflowDefinition
	if err := json.Unmarshal(data, &definition); err != nil {
		return apiCommandResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response apiCommandResponse
	err = newAPIClient(*coordinator).json(ctx, http.MethodPost, "/v1/commands", core.Command{
		Kind: core.CommandSubmit, RequestID: *requestID, At: time.Now().UnixMilli(), Definition: &definition,
	}, &response)
	return response, err
}

func remoteInspect(arguments []string) (apiWorkflowResponse, error) {
	flags := flag.NewFlagSet("inspect", flag.ContinueOnError)
	coordinator := flags.String("coordinator", "http://127.0.0.1:7101", "coordinator URL")
	if err := flags.Parse(arguments); err != nil {
		return apiWorkflowResponse{}, err
	}
	if flags.NArg() != 1 {
		return apiWorkflowResponse{}, fmt.Errorf("inspect requires one workflow id")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return newAPIClient(*coordinator).workflow(ctx, flags.Arg(0))
}

func remoteCancel(arguments []string) (apiCommandResponse, error) {
	flags := flag.NewFlagSet("cancel", flag.ContinueOnError)
	coordinator := flags.String("coordinator", "http://127.0.0.1:7101", "coordinator URL")
	requestID := flags.String("request-id", "", "stable request identifier")
	reason := flags.String("reason", "", "cancellation reason")
	if err := flags.Parse(arguments); err != nil {
		return apiCommandResponse{}, err
	}
	if flags.NArg() != 1 || strings.TrimSpace(*requestID) == "" || strings.TrimSpace(*reason) == "" {
		return apiCommandResponse{}, fmt.Errorf("cancel requires --request-id, --reason, and one workflow id")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response apiCommandResponse
	err := newAPIClient(*coordinator).json(ctx, http.MethodPost, workflowPath(flags.Arg(0))+"/cancel", map[string]string{
		"request_id": *requestID, "reason": *reason,
	}, &response)
	return response, err
}

func remoteEvents(arguments []string, output io.Writer) error {
	flags := flag.NewFlagSet("events", flag.ContinueOnError)
	coordinator := flags.String("coordinator", "http://127.0.0.1:7101", "coordinator URL")
	after := flags.Uint64("after", 0, "last observed workflow sequence")
	follow := flags.Bool("follow", true, "wait for new events")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("events requires one workflow id")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	client := newAPIClient(*coordinator)
	cursor := *after
	for {
		ended, err := client.events(ctx, flags.Arg(0), &cursor, *follow, output)
		if !*follow || ended {
			return err
		}
		if err != nil {
			var serverError apiError
			if errors.As(err, &serverError) {
				return err
			}
		}
		workflow, inspectErr := client.workflow(ctx, flags.Arg(0))
		if inspectErr == nil && workflow.State != nil && terminal(workflow.State.Status) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func newAPIClient(address string) *apiClient {
	return &apiClient{base: strings.TrimRight(address, "/"), http: &http.Client{}}
}

func (c *apiClient) json(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError(response)
	}
	if output == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(io.LimitReader(response.Body, 32<<20)).Decode(output)
}

func (c *apiClient) workflow(ctx context.Context, workflowID string) (apiWorkflowResponse, error) {
	var response apiWorkflowResponse
	err := c.json(ctx, http.MethodGet, workflowPath(workflowID), nil, &response)
	return response, err
}

func (c *apiClient) events(ctx context.Context, workflowID string, cursor *uint64, follow bool, output io.Writer) (bool, error) {
	query := url.Values{}
	query.Set("after", strconv.FormatUint(*cursor, 10))
	query.Set("follow", strconv.FormatBool(follow))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+workflowPath(workflowID)+"/events?"+query.Encode(), nil)
	if err != nil {
		return false, err
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Last-Event-ID", strconv.FormatUint(*cursor, 10))
	response, err := c.http.Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, responseError(response)
	}
	return consumeEvents(response.Body, cursor, output)
}

func consumeEvents(input io.Reader, cursor *uint64, output io.Writer) (bool, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	var data string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if data == "" {
				continue
			}
			var event core.Event
			if err := json.Unmarshal([]byte(data), &event); err != nil {
				return false, err
			}
			data = ""
			// The cursor filters replayed events after a stream reconnect.
			if event.Sequence <= *cursor {
				continue
			}
			encoded, err := json.Marshal(event)
			if err != nil {
				return false, err
			}
			if _, err := fmt.Fprintln(output, string(encoded)); err != nil {
				return false, err
			}
			*cursor = event.Sequence
			if terminalWorkflowEvent(event.Kind) {
				return true, nil
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			value := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data != "" {
				data += "\n"
			}
			data += value
		}
	}
	return false, scanner.Err()
}

func responseError(response *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	var body struct {
		Message string `json:"message"`
	}
	json.Unmarshal(data, &body)
	message := strings.TrimSpace(body.Message)
	if message == "" {
		message = strings.TrimSpace(string(data))
	}
	if message == "" {
		message = response.Status
	}
	return apiError{status: response.StatusCode, text: message}
}

func workflowPath(workflowID string) string {
	return "/v1/workflows/" + url.PathEscape(workflowID)
}

func run(path string) (runOutput, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return runOutput{}, err
	}
	var definition core.WorkflowDefinition
	if err := json.Unmarshal(data, &definition); err != nil {
		return runOutput{}, err
	}
	engine := core.NewEngine()
	clock, err := core.NewVirtualClock(0)
	if err != nil {
		return runOutput{}, err
	}
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "cli-submit", At: clock.Now(), Definition: &definition})
	if err != nil {
		return runOutput{}, err
	}
	workflowID := submit.WorkflowID
	for {
		state, _ := engine.State(workflowID)
		if terminal(state.Status) {
			break
		}
		if timer, exists := dueTimer(state, clock.Now()); exists {
			if err := fireTimer(engine, clock, workflowID, timer); err != nil {
				return runOutput{}, err
			}
			continue
		}
		tasks := runnableTasks(state)
		if len(tasks) == 0 {
			timer, exists := nextTimer(state)
			if !exists {
				return runOutput{}, fmt.Errorf("workflow cannot progress")
			}
			if timer.Deadline > clock.Now() {
				if err := clock.Advance(timer.Deadline - clock.Now()); err != nil {
					return runOutput{}, err
				}
			}
			if err := fireTimer(engine, clock, workflowID, timer); err != nil {
				return runOutput{}, err
			}
			continue
		}
		for _, taskID := range tasks {
			state, _ = engine.State(workflowID)
			task := state.Tasks[taskID]
			if !runnable(task) {
				continue
			}
			if err := clock.Advance(1); err != nil {
				return runOutput{}, err
			}
			leaseMillis := int64(1000)
			if task.Definition.TimeoutMillis > 0 && task.Definition.TimeoutMillis < leaseMillis {
				leaseMillis = task.Definition.TimeoutMillis
			}
			start, err := engine.Handle(core.Command{
				Kind: core.CommandStart, RequestID: core.LeaseRequestID(workflowID, taskID, task.Attempt+1), WorkflowID: workflowID,
				TaskID: taskID, WorkerID: "local", At: clock.Now(), LeaseUntil: clock.Now() + leaseMillis,
			})
			if err != nil {
				return runOutput{}, err
			}
			lease, exists := eventOfKind(start.Events, core.EventTaskStarted)
			if !exists {
				return runOutput{}, fmt.Errorf("task %s did not start", taskID)
			}
			_, err = engine.Handle(core.Command{
				Kind: core.CommandComplete, RequestID: core.CompletionRequestID(lease.AttemptID, lease.Fence), WorkflowID: workflowID,
				TaskID: taskID, AttemptID: lease.AttemptID, WorkerID: lease.WorkerID,
				Fence: lease.Fence, At: clock.Now(), Output: map[string]string{"value": taskID}, Fanout: localFanout(task),
			})
			if err != nil {
				return runOutput{}, err
			}
		}
	}
	state, _ := engine.State(workflowID)
	hash, err := core.StateHash(state)
	if err != nil {
		return runOutput{}, err
	}
	replayed, err := core.Replay(engine.Journal())
	if err != nil {
		return runOutput{}, err
	}
	replayedState, _ := replayed.State(workflowID)
	replayHash, err := core.StateHash(replayedState)
	if err != nil {
		return runOutput{}, err
	}
	if hash != replayHash {
		return runOutput{}, fmt.Errorf("replay hash mismatch")
	}
	projection, _ := engine.Projection(workflowID)
	return runOutput{WorkflowID: workflowID, StateHash: hash, ReplayHash: replayHash, State: state, Projection: projection, Metrics: engine.Metrics()}, nil
}

func terminal(status core.WorkflowStatus) bool {
	switch status {
	case core.WorkflowCompleted, core.WorkflowFailed, core.WorkflowCancelled, core.WorkflowCompensated:
		return true
	default:
		return false
	}
}

func terminalWorkflowEvent(kind core.EventKind) bool {
	switch kind {
	case core.EventWorkflowCompleted, core.EventWorkflowFailed, core.EventWorkflowCancelled, core.EventWorkflowCompensated:
		return true
	default:
		return false
	}
}

func runnableTasks(state *core.WorkflowState) []string {
	var tasks []string
	for id, task := range state.Tasks {
		if runnable(task) {
			tasks = append(tasks, id)
		}
	}
	sort.Strings(tasks)
	return tasks
}

func runnable(task core.TaskState) bool {
	return task.Status == core.TaskReady || task.Status == core.TaskCompensating && task.WorkerID == ""
}

func nextTimer(state *core.WorkflowState) (core.TimerState, bool) {
	var selected core.TimerState
	found := false
	for _, timer := range state.Timers {
		if timer.Status != core.TimerScheduled {
			continue
		}
		if !found || timer.Deadline < selected.Deadline || timer.Deadline == selected.Deadline && timer.ID < selected.ID {
			selected = timer
			found = true
		}
	}
	return selected, found
}

func dueTimer(state *core.WorkflowState, now int64) (core.TimerState, bool) {
	timer, exists := nextTimer(state)
	return timer, exists && timer.Deadline <= now
}

func fireTimer(engine *core.Engine, clock *core.VirtualClock, workflowID string, timer core.TimerState) error {
	_, err := engine.Handle(core.Command{
		Kind: core.CommandFireTimer, RequestID: core.TimerRequestID(timer.ID), WorkflowID: workflowID,
		TimerID: timer.ID, At: clock.Now(),
	})
	return err
}

func localFanout(task core.TaskState) []core.FanoutItem {
	if task.Definition.Fanout == nil {
		return nil
	}
	count := task.Definition.Fanout.MaxItems
	if count > 2 {
		count = 2
	}
	items := make([]core.FanoutItem, count)
	for index := range items {
		key := fmt.Sprintf("item-%d", index+1)
		items[index] = core.FanoutItem{Key: key, Payload: map[string]string{"value": key}}
	}
	return items
}

func eventOfKind(events []core.Event, kind core.EventKind) (core.Event, bool) {
	for _, event := range events {
		if event.Kind == kind {
			return event, true
		}
	}
	return core.Event{}, false
}
