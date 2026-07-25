package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestRunReleaseWorkflow(t *testing.T) {
	output, err := run(filepath.Join("..", "..", "examples", "release.json"))
	if err != nil {
		t.Fatal(err)
	}
	if output.State.Status != core.WorkflowCompleted || output.StateHash != output.ReplayHash {
		t.Fatalf("unexpected output: %+v", output)
	}
	if output.Projection.Summary.Completed != 8 || output.Metrics.CommandsAccepted != 18 || output.Metrics.Transitions != 38 {
		t.Fatalf("unexpected counters: %+v %+v", output.Projection, output.Metrics)
	}
	inputs := output.State.Tasks["aggregate"].Inputs
	first := core.FanoutTaskID(output.WorkflowID, "discover-tests", "item-1")
	second := core.FanoutTaskID(output.WorkflowID, "discover-tests", "item-2")
	if inputs["discover-tests"]["value"] != "discover-tests" || inputs[first]["value"] != first || inputs[second]["value"] != second {
		t.Fatalf("upstream outputs missing: %v", inputs)
	}
}

func TestCLIEndToEnd(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "chronos")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	if runtime.GOOS == "windows" {
		goBinary += ".exe"
	}
	build := exec.Command(goBinary, "build", "-o", binary, "./cmd/chronos")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v %s", err, output)
	}
	example := filepath.Join(root, "examples", "release.json")
	firstOut, firstErr, err := execute(binary, "run", example)
	if err != nil || len(firstErr) != 0 {
		t.Fatalf("valid run failed: %v %s", err, firstErr)
	}
	secondOut, secondErr, err := execute(binary, "run", example)
	if err != nil || len(secondErr) != 0 || !bytes.Equal(firstOut, secondOut) {
		t.Fatalf("repeat run differed: %v %s", err, secondErr)
	}
	var output runOutput
	if err := json.Unmarshal(firstOut, &output); err != nil {
		t.Fatal(err)
	}
	if output.State.Status != core.WorkflowCompleted || output.StateHash != output.ReplayHash {
		t.Fatalf("invalid process output: %+v", output)
	}
	cycle := filepath.Join(t.TempDir(), "cycle.json")
	if err := os.WriteFile(cycle, []byte(`{"name":"cycle","namespace":"test","tasks":[{"id":"a","type":"test","dependencies":["a"],"retry":{"max_attempts":1,"backoff_multiplier":1}}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := execute(binary, "run", cycle)
	if exitCode(err) != 1 || len(stdout) != 0 || len(stderr) == 0 {
		t.Fatalf("invalid workflow exit mismatch: %v %q %q", err, stdout, stderr)
	}
	stdout, stderr, err = execute(binary)
	if exitCode(err) != 2 || len(stdout) != 0 || len(stderr) == 0 {
		t.Fatalf("usage exit mismatch: %v %q %q", err, stdout, stderr)
	}
}

func execute(binary string, arguments ...string) ([]byte, []byte, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command := exec.Command(binary, arguments...)
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func exitCode(err error) int {
	if exitError, ok := err.(*exec.ExitError); ok {
		return exitError.ExitCode()
	}
	if err == nil {
		return 0
	}
	return -1
}

func TestRunRejectsCycle(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{
		Name: "cycle", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "a", Type: "test", Dependencies: []string{"b"}, Retry: retry},
			{ID: "b", Type: "test", Dependencies: []string{"a"}, Retry: retry},
		},
	}
	data, err := json.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cycle.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(path); err == nil {
		t.Fatal("cycle accepted")
	}
}

func TestRunShortTimeout(t *testing.T) {
	definition := core.WorkflowDefinition{
		Name: "short-timeout", Namespace: "test",
		Tasks: []core.TaskDefinition{{
			ID: "task", Type: "test", TimeoutMillis: 1,
			Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
		}},
	}
	data, err := json.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "short-timeout.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := run(path)
	if err != nil {
		t.Fatal(err)
	}
	if output.State.Status != core.WorkflowCompleted || output.StateHash != output.ReplayHash {
		t.Fatalf("unexpected output: %+v", output)
	}
}

func TestRemoteCommands(t *testing.T) {
	definition := core.WorkflowDefinition{
		Name: "remote", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
	path := filepath.Join(t.TempDir(), "workflow.json")
	data, _ := json.Marshal(definition)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	state := &core.WorkflowState{ID: "workflow", Status: core.WorkflowRunning}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/commands":
			var command core.Command
			if err := json.NewDecoder(request.Body).Decode(&command); err != nil || command.RequestID != "submit" || command.Definition.Name != "remote" {
				t.Errorf("invalid submission: %+v %v", command, err)
			}
			json.NewEncoder(writer).Encode(apiCommandResponse{Results: []core.Result{{WorkflowID: "workflow"}}, AppliedIndex: 2})
		case request.Method == http.MethodGet && request.URL.Path == "/v1/workflows/workflow":
			json.NewEncoder(writer).Encode(apiWorkflowResponse{Found: true, State: state})
		case request.Method == http.MethodPost && request.URL.Path == "/v1/workflows/workflow/cancel":
			var body map[string]string
			json.NewDecoder(request.Body).Decode(&body)
			if body["request_id"] != "cancel" || body["reason"] != "operator" {
				t.Errorf("invalid cancellation: %v", body)
			}
			json.NewEncoder(writer).Encode(apiCommandResponse{Results: []core.Result{{WorkflowID: "workflow"}}})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	submitted, err := remoteSubmit([]string{"--coordinator", server.URL, "--request-id", "submit", path})
	if err != nil || len(submitted.Results) != 1 || submitted.Results[0].WorkflowID != "workflow" {
		t.Fatalf("submission failed: %+v %v", submitted, err)
	}
	inspected, err := remoteInspect([]string{"--coordinator", server.URL, "workflow"})
	if err != nil || !inspected.Found || inspected.State.ID != "workflow" {
		t.Fatalf("inspection failed: %+v %v", inspected, err)
	}
	cancelled, err := remoteCancel([]string{"--coordinator", server.URL, "--request-id", "cancel", "--reason", "operator", "workflow"})
	if err != nil || len(cancelled.Results) != 1 {
		t.Fatalf("cancellation failed: %+v %v", cancelled, err)
	}
}

func TestConsumeEventsResumes(t *testing.T) {
	events := []core.Event{
		{Kind: core.EventWorkflowSubmitted, WorkflowID: "workflow", Sequence: 1},
		{Kind: core.EventTaskReady, WorkflowID: "workflow", TaskID: "task", Sequence: 2},
		{Kind: core.EventWorkflowCompleted, WorkflowID: "workflow", Sequence: 3},
	}
	var stream strings.Builder
	for _, event := range events {
		data, _ := json.Marshal(event)
		fmt.Fprintf(&stream, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Kind, data)
	}
	cursor := uint64(1)
	var output bytes.Buffer
	terminal, err := consumeEvents(strings.NewReader(stream.String()), &cursor, &output)
	if err != nil || !terminal || cursor != 3 {
		t.Fatalf("stream failed: %d %v %v", cursor, terminal, err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 || strings.Contains(lines[0], "workflow_submitted") || !strings.Contains(lines[1], "workflow_completed") {
		t.Fatalf("unexpected output: %q", output.String())
	}
}

func TestAPIClientFollowsRedirect(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		json.NewEncoder(writer).Encode(apiWorkflowResponse{Found: true, State: &core.WorkflowState{ID: "workflow"}})
	}))
	defer leader.Close()
	follower := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Location", leader.URL+request.URL.RequestURI())
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer follower.Close()
	workflow, err := newAPIClient(follower.URL).workflow(context.Background(), "workflow")
	if err != nil || !workflow.Found {
		t.Fatalf("redirect failed: %+v %v", workflow, err)
	}
}
