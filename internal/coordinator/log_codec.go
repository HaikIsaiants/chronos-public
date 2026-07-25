package coordinator

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/golang/snappy"
)

const maxLogBatchBytes = 64 << 20
const maxLogBatchCommands = 256
const maxLogBatchEvents = 1 << 20

var snappyStreamMagic = []byte{0xff, 0x06, 0, 0, 's', 'N', 'a', 'P', 'p', 'Y'}
var logBatchCodecPrefix = []byte{0x89, 'C', 'H', 'L', 'B'}

// The last byte is the codec version
var logBatchGobSnappyMagic = []byte{0x89, 'C', 'H', 'L', 'B', 1}

func encodeLogBatch(batch LogBatch) ([]byte, error) {
	if err := validateLogBatch(batch); err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(batch); err != nil {
		return nil, err
	}
	if buffer.Len() > maxLogBatchBytes {
		return nil, fmt.Errorf("raft command batch exceeds size limit")
	}
	encoded := make([]byte, len(logBatchGobSnappyMagic)+snappy.MaxEncodedLen(buffer.Len()))
	copy(encoded, logBatchGobSnappyMagic)
	compressed := snappy.Encode(encoded[len(logBatchGobSnappyMagic):], buffer.Bytes())
	encoded = encoded[:len(logBatchGobSnappyMagic)+len(compressed)]
	if len(encoded) > maxLogBatchBytes {
		return nil, fmt.Errorf("raft command batch exceeds size limit")
	}
	return encoded, nil
}

func decodeLogBatch(data []byte) (LogBatch, error) {
	if len(data) > maxLogBatchBytes {
		return LogBatch{}, fmt.Errorf("raft command batch exceeds size limit")
	}
	var batch LogBatch
	switch {
	case bytes.HasPrefix(data, logBatchCodecPrefix):
		if !bytes.HasPrefix(data, logBatchGobSnappyMagic) {
			return LogBatch{}, fmt.Errorf("unsupported raft command batch codec")
		}
		decodedLength, err := snappy.DecodedLen(data[len(logBatchGobSnappyMagic):])
		if err != nil {
			return LogBatch{}, err
		}
		if decodedLength > maxLogBatchBytes {
			return LogBatch{}, fmt.Errorf("raft command batch exceeds size limit")
		}
		decoded, err := snappy.Decode(nil, data[len(logBatchGobSnappyMagic):])
		if err != nil {
			return LogBatch{}, err
		}
		decoder := gob.NewDecoder(bytes.NewReader(decoded))
		if err := decoder.Decode(&batch); err != nil {
			return LogBatch{}, err
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return LogBatch{}, fmt.Errorf("raft command batch has trailing data")
		}
	case bytes.HasPrefix(data, snappyStreamMagic):
		decoded, err := io.ReadAll(io.LimitReader(snappy.NewReader(bytes.NewReader(data)), maxLogBatchBytes+1))
		if err != nil {
			return LogBatch{}, err
		}
		if len(decoded) > maxLogBatchBytes {
			return LogBatch{}, fmt.Errorf("raft command batch exceeds size limit")
		}
		if err := json.Unmarshal(decoded, &batch); err != nil {
			return LogBatch{}, err
		}
	default:
		if err := json.Unmarshal(data, &batch); err != nil {
			return LogBatch{}, err
		}
	}
	if err := validateLogBatch(batch); err != nil {
		return LogBatch{}, err
	}
	return batch, nil
}

func validateLogBatch(batch LogBatch) error {
	if batch.Version != LogBatchVersion || len(batch.Commands) == 0 || len(batch.Commands) > maxLogBatchCommands {
		return fmt.Errorf("invalid raft command batch")
	}
	events := 0
	for _, command := range batch.Commands {
		if command.Version != core.CommandBatchVersion || command.RequestID == "" || command.RequestHash == "" || command.WorkflowID == "" || len(command.Events) == 0 {
			return fmt.Errorf("invalid raft command batch")
		}
		events += len(command.Events)
		if events > maxLogBatchEvents {
			return fmt.Errorf("invalid raft command batch")
		}
		for _, event := range command.Events {
			if event.RequestID != command.RequestID || event.RequestHash != command.RequestHash || event.WorkflowID != command.WorkflowID || !canonicalEvent(event) {
				return fmt.Errorf("invalid raft command batch")
			}
		}
	}
	return nil
}

func canonicalizeLogBatch(batch *LogBatch) {
	// Nil and empty slices need the same wire form
	// so prepared entries can be matched byte for byte
	for commandIndex := range batch.Commands {
		for eventIndex := range batch.Commands[commandIndex].Events {
			event := &batch.Commands[commandIndex].Events[eventIndex]
			canonicalizeDefinition(event.Definition)
			if len(event.ExpandedTasks) == 0 {
				event.ExpandedTasks = nil
			}
			for expandedIndex := range event.ExpandedTasks {
				task := &event.ExpandedTasks[expandedIndex].Definition
				if len(task.Dependencies) == 0 {
					task.Dependencies = nil
				}
			}
		}
	}
}

func canonicalizeDefinition(definition *core.WorkflowDefinition) {
	if definition == nil {
		return
	}
	for index := range definition.Tasks {
		if len(definition.Tasks[index].Dependencies) == 0 {
			definition.Tasks[index].Dependencies = nil
		}
	}
}

func canonicalEvent(event core.Event) bool {
	if event.ExpandedTasks != nil && len(event.ExpandedTasks) == 0 || !canonicalWorkflowDefinition(event.Definition) {
		return false
	}
	for _, expanded := range event.ExpandedTasks {
		if expanded.Definition.Dependencies != nil && len(expanded.Definition.Dependencies) == 0 {
			return false
		}
	}
	return true
}

func canonicalWorkflowDefinition(definition *core.WorkflowDefinition) bool {
	if definition == nil {
		return true
	}
	if len(definition.Tasks) == 0 {
		return false
	}
	for _, task := range definition.Tasks {
		if task.Dependencies != nil && len(task.Dependencies) == 0 {
			return false
		}
	}
	return true
}
