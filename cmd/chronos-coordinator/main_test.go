package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParsePeers(t *testing.T) {
	peers, err := parsePeers("1=http://127.0.0.1:7101,2=http://127.0.0.1:7102,3=http://127.0.0.1:7103")
	if err != nil || peers[2] != "http://127.0.0.1:7102" {
		t.Fatalf("unexpected peers: %v %v", peers, err)
	}
	for _, invalid := range []string{"", "1=a,2=b", "1=a,2=b,4=d", "1=a,1=b,3=c"} {
		if _, err := parsePeers(invalid); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
}

func TestParseWeights(t *testing.T) {
	weights, err := parseWeights("beta=2,alpha=1")
	if err != nil || weights["alpha"] != 1 || weights["beta"] != 2 {
		t.Fatalf("unexpected weights: %v %v", weights, err)
	}
	for _, invalid := range []string{"alpha=0", "alpha=x", "alpha=1,alpha=2", "=1"} {
		if _, err := parseWeights(invalid); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
}

func TestCompletionAckFailpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ack")
	failpoint := completionAckFailpoint(path)
	if failpoint == nil {
		t.Fatal("fault file did not create a failpoint")
	}
	if err := failpoint("completion"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("armed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := failpoint("renew"); err != nil {
		t.Fatal(err)
	}
	if err := failpoint("completion"); err == nil {
		t.Fatal("armed completion acknowledgement was not lost")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "lost" {
		t.Fatalf("unexpected first fault state: %q %v", data, err)
	}
	if err := failpoint("completion"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(path)
	if err != nil || string(data) != "retried" {
		t.Fatalf("unexpected retry state: %q %v", data, err)
	}
}
