package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/bench"
	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestSubmitGeneratesBoundedNamespaceLoad(t *testing.T) {
	var mu sync.Mutex
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var command struct {
			RequestID  string `json:"request_id"`
			Definition struct {
				Namespace string `json:"namespace"`
			} `json:"definition"`
		}
		if err := json.NewDecoder(request.Body).Decode(&command); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests[command.RequestID]++
		requests[command.Definition.Namespace]++
		mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte("{\"results\":[{\"workflow_id\":\"workflow\"}]}"))
	}))
	defer server.Close()
	if err := submit(context.Background(), server.Client(), server.URL, 3, 12, 4, "work", "load"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for index := range 12 {
		if requests["load-"+strconv.Itoa(index)] != 1 {
			t.Fatalf("request %d count is %d", index, requests["load-"+strconv.Itoa(index)])
		}
	}
	for index := range 3 {
		if requests["namespace-"+strconv.Itoa(index)] != 4 {
			t.Fatalf("namespace %d count is %d", index, requests["namespace-"+strconv.Itoa(index)])
		}
	}
}

func TestWaitCompletedUsesDeterministicWorkflowIDs(t *testing.T) {
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		reads++
		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte("{\"found\":true,\"state\":{\"status\":\"completed\"}}"))
	}))
	defer server.Close()
	if err := waitCompleted(context.Background(), server.Client(), server.URL, 3, 12, "done"); err != nil {
		t.Fatal(err)
	}
	if reads != 12 {
		t.Fatalf("read %d workflows", reads)
	}
}

func TestExecuteBatchesAndCountsTransitions(t *testing.T) {
	engine := core.NewEngine()
	requests := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Commands []core.Command `json:"commands"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		requests++
		results := make([]core.Result, 0, len(body.Commands))
		for _, command := range body.Commands {
			result, err := engine.Handle(command)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			results = append(results, result)
		}
		writer.Header().Set("Content-Type", "application/json")
		json.NewEncoder(writer).Encode(struct {
			Results []core.Result `json:"results"`
		}{Results: results})
	}))
	defer server.Close()
	transitions, err := execute(context.Background(), server.Client(), server.URL, 10, 31, 3, int(bench.WorkloadBatchSize))
	if err != nil {
		t.Fatal(err)
	}
	if transitions != 31*bench.WorkloadTransitionsPerWorkflow || requests != 3 {
		t.Fatalf("unexpected execution result: transitions=%d requests=%d", transitions, requests)
	}
}
