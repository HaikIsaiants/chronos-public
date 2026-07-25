package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	chronosv1 "github.com/HaikIsaiants/chronos-public/api/chronos/v1"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/execution"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var endpointList string
	var workerID string
	var capabilityList string
	var fanoutList string
	var failureList string
	var delayList string
	var delay time.Duration
	var credits uint64
	flag.StringVar(&endpointList, "coordinators", "", "comma-separated worker gRPC addresses")
	flag.StringVar(&workerID, "id", "", "worker id")
	flag.StringVar(&capabilityList, "capabilities", "", "comma-separated task types")
	flag.StringVar(&fanoutList, "fanout-types", "", "comma-separated task types that emit two items")
	flag.StringVar(&failureList, "fail-types", "", "comma-separated task types that fail")
	flag.StringVar(&delayList, "delay-types", "", "comma-separated task types to delay")
	flag.DurationVar(&delay, "delay", 0, "task delay")
	flag.Uint64Var(&credits, "credits", 1, "parallel task credits")
	flag.Parse()
	endpoints := splitValues(endpointList)
	capabilities := splitValues(capabilityList)
	fanoutTypes := valueSet(splitValues(fanoutList))
	failureTypes := valueSet(splitValues(failureList))
	delayTypes := valueSet(splitValues(delayList))
	if len(endpoints) == 0 || workerID == "" || len(capabilities) == 0 || !validCredits(credits) || delay < 0 {
		fmt.Fprintln(os.Stderr, "usage: chronos-worker -coordinators <host:port,...> -id <id> -capabilities <type,...> [-credits n]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	simulator := &execution.Simulator{
		ID: workerID, Capabilities: capabilities, Credits: uint32(credits),
		Handle: func(ctx context.Context, assignment *chronosv1.TaskAssignment) (execution.HandlerResult, error) {
			if _, delayed := delayTypes[assignment.GetTaskType()]; delayed {
				select {
				case <-ctx.Done():
					return execution.HandlerResult{}, ctx.Err()
				case <-time.After(delay):
				}
			}
			if _, fail := failureTypes[assignment.GetTaskType()]; fail && !assignment.GetCompensation() {
				return execution.HandlerResult{}, fmt.Errorf("task type %s failed", assignment.GetTaskType())
			}
			result := execution.HandlerResult{Output: map[string]string{"task": assignment.GetTaskId(), "worker": workerID}}
			if _, expand := fanoutTypes[assignment.GetTaskType()]; expand && !assignment.GetCompensation() {
				result.Fanout = []core.FanoutItem{
					{Key: "item-1", Payload: map[string]string{"value": "item-1"}},
					{Key: "item-2", Payload: map[string]string{"value": "item-2"}},
				}
			}
			return result, nil
		},
	}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		// endpoint rotation lets reconnects discover a new leader.
		connection, err := grpc.NewClient(endpoints[attempt%len(endpoints)], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			err = simulator.Run(ctx, chronosv1.NewWorkerServiceClient(connection))
			connection.Close()
		}
		if ctx.Err() != nil {
			break
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func valueSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func validCredits(credits uint64) bool {
	return credits > 0 && credits <= math.MaxUint32
}

func splitValues(value string) []string {
	set := make(map[string]struct{})
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			set[item] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for item := range set {
		result = append(result, item)
	}
	sort.Strings(result)
	return result
}
