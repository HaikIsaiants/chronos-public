package core_test

import (
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestCanonicalCommandHash(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	first := core.Command{
		Kind: core.CommandSubmit, RequestID: "request", At: 1,
		Definition: &core.WorkflowDefinition{Name: "hash", Namespace: "test", Tasks: []core.TaskDefinition{
			{ID: "b", Type: "test", Dependencies: []string{"a", "c"}, Retry: retry},
			{ID: "c", Type: "test", Retry: retry},
			{ID: "a", Type: "test", Retry: retry},
		}},
	}
	second := core.Command{
		Kind: core.CommandSubmit, RequestID: "request", At: 99,
		Definition: &core.WorkflowDefinition{Name: "hash", Namespace: "test", Tasks: []core.TaskDefinition{
			{ID: "a", Type: "test", Retry: retry},
			{ID: "b", Type: "test", Dependencies: []string{"c", "a"}, Retry: retry},
			{ID: "c", Type: "test", Retry: retry},
		}},
	}
	firstHash, err := core.HashCommand(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := core.HashCommand(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("canonical hashes differ: %s %s", firstHash, secondHash)
	}
	first.RequestID = "another-request"
	thirdHash, err := core.HashCommand(first)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash == thirdHash {
		t.Fatal("generated workflow target was omitted from the fingerprint")
	}
}
