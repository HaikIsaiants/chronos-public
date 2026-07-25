package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/HaikIsaiants/chronos-public/internal/bench"
	"github.com/HaikIsaiants/chronos-public/internal/recoverybench"
)

func main() {
	workflows := flag.Int("workflows", 1000, "workflow count")
	recoveryDirectory := flag.String("recovery-directory", "", "empty directory for a recovery measurement")
	tailCommands := flag.Uint64("tail-commands", 0, "committed commands replayed after the snapshot")
	flag.Parse()
	var report any
	var err error
	if *workflows < 1 {
		err = fmt.Errorf("workflows must be positive")
	} else if *recoveryDirectory == "" {
		report, err = bench.Run(*workflows)
	} else {
		report, err = recoverybench.Run(recoverybench.Options{Directory: *recoveryDirectory, Workflows: uint64(*workflows), TailCommands: *tailCommands})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
