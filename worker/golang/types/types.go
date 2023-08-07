package types

import (
	"context"
	"fmt"
	"time"
)

type LogEntry struct {
	LocalId uint64
	SeqNum  uint64
	Tags    []uint64
	Data    []byte
	AuxData []byte
}

type LogStreamEntry[T LogEntry | LogEntryWithMeta] struct {
	LogEntry *T
	Err      error
}

// Deps: User defined dependencies to check if a log is able to apply.
//  1. If no dependency, mark the log based on the condition.
//  2. If no condition, Fsm.Apply(log) should return true, then the log is
//     marked as APPLIED.
//
// Dependencies status:
//   - PENDING: Resolve dependencies recursively.
//   - APPLIED: Only if all dependencies are APPLIED can the current log be applied,
//     or to say, being passed to the Fsm.Apply(log)
//   - DISCARDED: Directly mark the current log as DISCARDED too, without passing
//     to Fsm.Apply(log)
type LogEntryWithMeta struct {
	LogEntry
	Deps        []uint64
	Identifiers []Tag
}

type LogEntryIndex struct {
	LocalId uint64
	SeqNum  uint64
}

// DEBUG
func (l LogEntryIndex) String() string {
	return fmt.Sprintf("LocalId=%016X SeqNum=%016X", l.LocalId, l.SeqNum)
}

type Future[T uint64 | *LogEntryWithMeta] interface {
	GetLocalId() uint64
	GetSeqNum() uint64
	GetResult(timeout time.Duration) (T, error)
	Await(timeout time.Duration) error
	IsResolved() bool
}

// When using []Tag, StreamId here forms a list of []StreamId, this is
// identical to the Tags in the LogEntry. They will be merged in the actual
// log data.
type Tag struct {
	// add stream type to support cross node replay which would not happen
	// in boki
	StreamType uint8 `json:"type"`
	// original tag
	StreamId uint64 `json:"tag"`
}
type DataWrapper struct {
	// deps and streamids for now
	Meta []byte `json:"meta"`
	// original log data
	Data []byte `json:"data"`
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
	SharedLogSetAuxDataWithShards(ctx context.Context, tags []uint64, seqNum uint64, auxData []byte) error
	// Batch read for range [seqNum, target)
	SharedLogReadNextUntil(ctx context.Context, tag uint64, seqNum uint64, target LogEntryIndex) *Queue[LogStreamEntry[LogEntry]]
	AsyncSharedLogReadNextUntil(ctx context.Context, tag uint64, seqNum uint64, target LogEntryIndex) *Queue[LogStreamEntry[LogEntryWithMeta]]

	AsyncSharedLogAppend(ctx context.Context, tags []Tag, data []byte) (Future[uint64], error)
	AsyncSharedLogAppendWithDeps(ctx context.Context, tags []Tag, data []byte, deps []uint64) (Future[uint64], error)
	AsyncSharedLogReadNext(ctx context.Context, tag uint64, seqNum uint64) (*LogEntryWithMeta, error)
	AsyncSharedLogReadNextBlock(ctx context.Context, tag uint64, seqNum uint64) (*LogEntryWithMeta, error)
	AsyncSharedLogReadPrev(ctx context.Context, tag uint64, seqNum uint64) (*LogEntryWithMeta, error)
	AsyncSharedLogCheckTail(ctx context.Context, tag uint64) (*LogEntryWithMeta, error)
	AsyncSharedLogReadPrevWithAux(ctx context.Context, tag uint64, seqNum uint64) (*LogEntryWithMeta, error)
	// async read API
	AsyncSharedLogRead(ctx context.Context, localId uint64) (*LogEntryWithMeta, error)
	AsyncSharedLogReadIndex(ctx context.Context, localId uint64) (uint64, error)
	// TODO: replace original blocking read
	AsyncSharedLogReadNext2(ctx context.Context, tag uint64, seqNum uint64) (Future[*LogEntryWithMeta], error)
	AsyncSharedLogReadNextBlock2(ctx context.Context, tag uint64, seqNum uint64) (Future[*LogEntryWithMeta], error)
	AsyncSharedLogReadPrev2(ctx context.Context, tag uint64, seqNum uint64) (Future[*LogEntryWithMeta], error)
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
