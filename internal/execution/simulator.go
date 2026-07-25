package execution

import (
	"context"
	"fmt"
	"sort"
	"sync"

	chronosv1 "github.com/HaikIsaiants/chronos/api/chronos/v1"
	"github.com/HaikIsaiants/chronos/internal/core"
)

type HandlerResult struct {
	Output map[string]string
	Fanout []core.FanoutItem
}

type Handler func(context.Context, *chronosv1.TaskAssignment) (HandlerResult, error)

type Simulator struct {
	ID           string
	Capabilities []string
	Credits      uint32
	Handle       Handler
	mu           sync.Mutex
	pending      map[string]*chronosv1.TaskCompletion
}

func (s *Simulator) Run(ctx context.Context, client chronosv1.WorkerServiceClient) error {
	if s.ID == "" || len(s.Capabilities) == 0 || s.Credits == 0 || s.Handle == nil {
		return fmt.Errorf("invalid simulator configuration")
	}
	stream, err := client.Work(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Hello{
		Hello: &chronosv1.WorkerHello{WorkerId: s.ID, Capabilities: append([]string(nil), s.Capabilities...), Credits: s.Credits},
	}}); err != nil {
		return err
	}
	// Replay pending completions after reconnect
	for _, completion := range s.pendingCompletions() {
		if err := stream.Send(completionMessage(completion)); err != nil {
			return err
		}
	}
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		if assignment := message.GetAssignment(); assignment != nil {
			if completion := s.pendingCompletion(assignment.GetAttemptId()); completion != nil {
				if err := stream.Send(completionMessage(completion)); err != nil {
					return err
				}
				continue
			}
			result, runErr := s.Handle(ctx, assignment)
			completion := &chronosv1.TaskCompletion{
				RequestId:  core.CompletionRequestID(assignment.GetAttemptId(), assignment.GetFencingToken()),
				WorkflowId: assignment.GetWorkflowId(), TaskId: assignment.GetTaskId(),
				AttemptId: assignment.GetAttemptId(), FencingToken: assignment.GetFencingToken(),
				Output: copyMap(result.Output), Fanout: protoFanout(result.Fanout),
			}
			if runErr != nil {
				completion.Output = nil
				completion.Fanout = nil
				completion.Error = runErr.Error()
			}
			s.storePending(completion)
			if err := stream.Send(completionMessage(completion)); err != nil {
				return err
			}
			continue
		}
		if ack := message.GetAck(); ack != nil {
			if ack.GetAccepted() || ack.GetCode() == "fenced" || ack.GetCode() == "expired" {
				s.clearPending(ack.GetRequestId())
			}
		}
	}
}

func completionMessage(completion *chronosv1.TaskCompletion) *chronosv1.WorkerMessage {
	return &chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Completion{Completion: completion}}
}

func (s *Simulator) pendingCompletions() []*chronosv1.TaskCompletion {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		s.pending = make(map[string]*chronosv1.TaskCompletion)
	}
	keys := make([]string, 0, len(s.pending))
	for key := range s.pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*chronosv1.TaskCompletion, 0, len(keys))
	for _, key := range keys {
		result = append(result, cloneCompletion(s.pending[key]))
	}
	return result
}

func (s *Simulator) pendingCompletion(attemptID string) *chronosv1.TaskCompletion {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneCompletion(s.pending[attemptID])
}

func (s *Simulator) storePending(completion *chronosv1.TaskCompletion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		s.pending = make(map[string]*chronosv1.TaskCompletion)
	}
	s.pending[completion.GetAttemptId()] = cloneCompletion(completion)
}

func (s *Simulator) clearPending(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for attemptID, completion := range s.pending {
		if completion.GetRequestId() == requestID {
			delete(s.pending, attemptID)
			return
		}
	}
}

func cloneCompletion(value *chronosv1.TaskCompletion) *chronosv1.TaskCompletion {
	if value == nil {
		return nil
	}
	return &chronosv1.TaskCompletion{
		RequestId: value.GetRequestId(), WorkflowId: value.GetWorkflowId(), TaskId: value.GetTaskId(),
		AttemptId: value.GetAttemptId(), FencingToken: value.GetFencingToken(),
		Output: copyMap(value.GetOutput()), Error: value.GetError(), Fanout: cloneProtoFanout(value.GetFanout()),
	}
}

func protoFanout(values []core.FanoutItem) []*chronosv1.FanoutItem {
	if len(values) == 0 {
		return nil
	}
	result := make([]*chronosv1.FanoutItem, len(values))
	for index, value := range values {
		result[index] = &chronosv1.FanoutItem{Key: value.Key, Payload: copyMap(value.Payload)}
	}
	return result
}

func cloneProtoFanout(values []*chronosv1.FanoutItem) []*chronosv1.FanoutItem {
	if len(values) == 0 {
		return nil
	}
	result := make([]*chronosv1.FanoutItem, len(values))
	for index, value := range values {
		result[index] = &chronosv1.FanoutItem{Key: value.GetKey(), Payload: copyMap(value.GetPayload())}
	}
	return result
}
