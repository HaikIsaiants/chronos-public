package core_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/reference"
)

func TestRandomizedDAGsMatchReference(t *testing.T) {
	// for seed := int64(37); seed <= 37; seed++ {
	for seed := int64(0); seed < 100; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			definition := randomDefinition(seed, 2+int(seed%11))
			if err := core.ValidateDefinition(&definition); err != nil {
				t.Fatalf("generated invalid definition: %v", err)
			}
			outputs := make(map[string]map[string]string, len(definition.Tasks))
			for _, task := range definition.Tasks {
				outputs[task.ID] = map[string]string{"value": task.ID}
			}
			referenceResult, err := reference.Run(definition, outputs)
			if err != nil {
				t.Fatal(err)
			}
			engine := core.NewEngine()
			submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
			if err != nil {
				t.Fatal(err)
			}
			order := make([]string, 0, len(definition.Tasks))
			logicalTime := int64(0)
			for {
				state, _ := engine.State(submit.WorkflowID)
				if state.Status == core.WorkflowCompleted {
					break
				}
				projection, _ := engine.Projection(submit.WorkflowID)
				if len(projection.ReadyTasks) == 0 {
					t.Fatal("engine made no progress")
				}
				for _, taskID := range projection.ReadyTasks {
					logicalTime++
					start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-" + taskID, WorkflowID: submit.WorkflowID, TaskID: taskID, At: logicalTime}))
					if err != nil {
						t.Fatal(err)
					}
					logicalTime++
					_, err = engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-" + taskID, WorkflowID: submit.WorkflowID, TaskID: taskID, At: logicalTime, Output: outputs[taskID]}, start))
					if err != nil {
						t.Fatal(err)
					}
					order = append(order, taskID)
				}
			}
			state, _ := engine.State(submit.WorkflowID)
			if state.Status != referenceResult.Status || !reflect.DeepEqual(order, referenceResult.Order) {
				t.Fatalf("reference mismatch: %s %v %+v", state.Status, order, referenceResult)
			}
			for id, output := range referenceResult.Outputs {
				if !reflect.DeepEqual(state.Tasks[id].Output, output) {
					t.Fatalf("output mismatch for %s", id)
				}
			}
			replayed, err := core.Replay(engine.Journal())
			if err != nil {
				t.Fatal(err)
			}
			replayedState, _ := replayed.State(submit.WorkflowID)
			liveHash, _ := core.StateHash(state)
			replayedHash, _ := core.StateHash(replayedState)
			if liveHash != replayedHash {
				t.Fatalf("replay mismatch: %s %s", liveHash, replayedHash)
			}
		})
	}
}

func FuzzValidateDefinition(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4})
	f.Add([]byte{9, 0, 8, 0, 7})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		count := 1 + int(data[0]%16)
		definition := definitionFromBytes(data, count)
		first := core.ValidateDefinition(&definition)
		second := core.ValidateDefinition(&definition)
		if fmt.Sprint(first) != fmt.Sprint(second) {
			t.Fatalf("validation is nondeterministic: %v %v", first, second)
		}
	})
}

func FuzzReplayDeterminism(f *testing.F) {
	f.Add(uint64(0))
	f.Add(^uint64(0))
	f.Fuzz(func(t *testing.T, seed uint64) {
		definition := randomDefinition(int64(seed), 2+int(seed%10))
		engine := core.NewEngine()
		submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
		if err != nil {
			t.Fatal(err)
		}
		logicalTime := int64(0)
		for {
			state, _ := engine.State(submit.WorkflowID)
			if state.Status == core.WorkflowCompleted {
				break
			}
			projection, _ := engine.Projection(submit.WorkflowID)
			for _, taskID := range projection.ReadyTasks {
				logicalTime++
				start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-" + taskID, WorkflowID: submit.WorkflowID, TaskID: taskID, At: logicalTime}))
				if err != nil {
					t.Fatal(err)
				}
				logicalTime++
				_, err = engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-" + taskID, WorkflowID: submit.WorkflowID, TaskID: taskID, At: logicalTime, Output: map[string]string{"value": taskID}}, start))
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		first, err := core.Replay(engine.Journal())
		if err != nil {
			t.Fatal(err)
		}
		second, err := core.Replay(engine.Journal())
		if err != nil {
			t.Fatal(err)
		}
		firstState, _ := first.State(submit.WorkflowID)
		secondState, _ := second.State(submit.WorkflowID)
		liveState, _ := engine.State(submit.WorkflowID)
		liveHash, _ := core.StateHash(liveState)
		firstHash, _ := core.StateHash(firstState)
		secondHash, _ := core.StateHash(secondState)
		if liveHash != firstHash || firstHash != secondHash {
			t.Fatalf("live and replay hashes differ: %s %s %s", liveHash, firstHash, secondHash)
		}
		liveProjection, _ := engine.Projection(submit.WorkflowID)
		firstProjection, _ := first.Projection(submit.WorkflowID)
		if !reflect.DeepEqual(liveProjection, firstProjection) {
			t.Fatal("live and replay projections differ")
		}
	})
}

func randomDefinition(seed int64, count int) core.WorkflowDefinition {
	random := rand.New(rand.NewSource(seed))
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	tasks := make([]core.TaskDefinition, count)
	for index := 0; index < count; index++ {
		task := core.TaskDefinition{ID: fmt.Sprintf("t%02d", index), Type: "test", Retry: retry}
		for previous := 0; previous < index; previous++ {
			if random.Intn(4) == 0 {
				task.Dependencies = append(task.Dependencies, fmt.Sprintf("t%02d", previous))
			}
		}
		random.Shuffle(len(task.Dependencies), func(i, j int) {
			task.Dependencies[i], task.Dependencies[j] = task.Dependencies[j], task.Dependencies[i]
		})
		tasks[index] = task
	}
	random.Shuffle(len(tasks), func(i, j int) {
		tasks[i], tasks[j] = tasks[j], tasks[i]
	})
	return core.WorkflowDefinition{Name: fmt.Sprintf("random-%d", seed), Namespace: "random", Tasks: tasks}
}

func definitionFromBytes(data []byte, count int) core.WorkflowDefinition {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	tasks := make([]core.TaskDefinition, count)
	position := 1
	for index := 0; index < count; index++ {
		tasks[index] = core.TaskDefinition{ID: fmt.Sprintf("t%02d", index), Type: "test", Retry: retry}
		for previous := 0; previous < index; previous++ {
			value := data[position%len(data)]
			position++
			if value%3 == 0 {
				tasks[index].Dependencies = append(tasks[index].Dependencies, fmt.Sprintf("t%02d", previous))
			}
		}
		sort.Strings(tasks[index].Dependencies)
	}
	return core.WorkflowDefinition{Name: "fuzz", Namespace: "fuzz", Tasks: tasks}
}
