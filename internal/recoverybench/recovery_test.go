package recoverybench_test

import (
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/recoverybench"
)

func TestRecoveryBenchmarkReplaysCommittedTail(t *testing.T) {
	report, err := recoverybench.Run(recoverybench.Options{Directory: t.TempDir(), Workflows: 25, TailCommands: 30})
	if err != nil {
		t.Fatal(err)
	}
	// t.Logf("recovery report: %+v", report)
	if report.Workflows != 25 || report.StatesVerified != 25 || report.TailCommands != 30 || report.TailEntries == 0 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	if report.IndexMismatches != 0 || report.StateHashMismatches != 0 || report.VersionMismatches != 0 {
		t.Fatalf("recovery mismatches: %+v", report)
	}
	if report.FixtureBuildNanos <= 0 || report.PersistNanos <= 0 || report.RecoveryNanos <= 0 || report.VerificationNanos <= 0 {
		t.Fatalf("recovery timing was not recorded: %+v", report)
	}
}

func TestRecoveryBenchmarkDefaultsTailCommands(t *testing.T) {
	report, err := recoverybench.Run(recoverybench.Options{Directory: t.TempDir(), Workflows: 4})
	if err != nil {
		t.Fatal(err)
	}
	if report.TailCommands != 4 {
		t.Fatalf("unexpected tail command count: %d", report.TailCommands)
	}
}

func TestRecoveryBenchmarkRejectsInvalidOptions(t *testing.T) {
	if _, err := recoverybench.Run(recoverybench.Options{}); err == nil {
		t.Fatal("incomplete recovery options accepted")
	}
	if _, err := recoverybench.Run(recoverybench.Options{Directory: t.TempDir(), Workflows: 2, TailCommands: 5}); err == nil {
		t.Fatal("excessive tail command count accepted")
	}
}
