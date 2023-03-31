package types

import (
	"context"
	"fmt"
	"log"
	"time"

	"cs.utexas.edu/zjia/faas/protocol"
)

type LogEntry struct {
	SeqNum  uint64
	Tags    []uint64
	Data    []byte
	AuxData []byte
}

type Future interface {
	GetLocalId() uint64
	GetResult() interface{}
	Verify() error
}

// TODO: move to an independent package
type futureImpl struct {
	LocalId    uint64
	SeqNum     uint64
	outputChan chan []byte
}

func NewFuture(localId uint64, outputChan chan []byte) Future {
	return &futureImpl{
		LocalId:    localId,
		outputChan: outputChan,
	}
}

func (f *futureImpl) GetLocalId() uint64 {
	return f.LocalId
}

func (f *futureImpl) GetResult() interface{} {
	return f.SeqNum
}

func (f *futureImpl) Verify() error {
	sleepDuration := 5 * time.Millisecond
	remainingRetries := 4
	for {
		response := <-f.outputChan
		result := protocol.GetSharedLogResultTypeFromMessage(response)
		if result == protocol.SharedLogResultType_APPEND_OK {
			f.SeqNum = protocol.GetLogSeqNumFromMessage(response)
			return nil
		} else if result == protocol.SharedLogResultType_DISCARDED {
			log.Printf("[ERROR] Append discarded, will retry")
			if remainingRetries > 0 {
				time.Sleep(sleepDuration)
				sleepDuration *= 2
				remainingRetries--
				continue
			} else {
				return fmt.Errorf("failed to append log, exceeds maximum number of retries")
			}
		} else {
			return fmt.Errorf("failed to append log, unacceptable result type: %d", result)
		}

	}
}

type Environment interface {
	InvokeFunc(ctx context.Context, funcName string, input []byte) ( /* output */ []byte, error)
	InvokeFuncAsync(ctx context.Context, funcName string, input []byte) error
	GrpcCall(ctx context.Context, service string, method string, request []byte) ( /* reply */ []byte, error)

	GenerateUniqueID() uint64

	// Shared log operations
	// Append a new log entry, tags must be non-zero
	SharedLogAppend(ctx context.Context, tags []uint64, data []byte) ( /* seqnum */ uint64, error)
	// Read the first log with `tag` whose seqnum >= given `seqNum`
	// `tag`==0 means considering log with any tag, including empty tag
	SharedLogReadNext(ctx context.Context, tag uint64, seqNum uint64) (*LogEntry, error)
	SharedLogReadNextBlock(ctx context.Context, tag uint64, seqNum uint64) (*LogEntry, error)
	// Read the last log with `tag` whose seqnum <= given `seqNum`
	// `tag`==0 means considering log with any tag, including empty tag
	SharedLogReadPrev(ctx context.Context, tag uint64, seqNum uint64) (*LogEntry, error)
	// Alias for ReadPrev(tag, MaxSeqNum)
	SharedLogCheckTail(ctx context.Context, tag uint64) (*LogEntry, error)
	// Set auxiliary data for log entry of given `seqNum`
	SharedLogSetAuxData(ctx context.Context, seqNum uint64, auxData []byte) error

	AsyncSharedLogAppend(ctx context.Context, tags []uint64, data []byte) (Future, error)
}

type FuncHandler interface {
	Call(ctx context.Context, input []byte) ( /* output */ []byte, error)
}

type GrpcFuncHandler interface {
	Call(ctx context.Context, method string, request []byte) ( /* reply */ []byte, error)
}

type FuncHandlerFactory interface {
	New(env Environment, funcName string) (FuncHandler, error)
	GrpcNew(env Environment, service string) (GrpcFuncHandler, error)
}
