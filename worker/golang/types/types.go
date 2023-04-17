package types

import (
	"context"
	"time"
)

type LogEntry struct {
	SeqNum  uint64
	Tags    []uint64
	Data    []byte
	AuxData []byte
}

// Cond: User defined conditions to check if a log is taged as APPLIED or DISCARDED.
// A log is always passed to the Fsm.Apply(log) -> bool first, in which the
// cond is delegated to check by the user code.
// Note that the condition is totally coupling to bussiness logic, there's no
// gaurantee that a log not being applied if cond is not satisfied, it all
// depends on the user.
// Fsm.Apply(log) returns the decision from the user, then the lib sets the
// log's aux data as the condition result, then it can be propagated later.
//
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
type CondLogEntry struct {
	LogEntry
	Deps         []FutureMeta
	Cond         []CondMeta
	TagBuildMeta []TagMeta
}

type Future[T uint64 | *CondLogEntry] interface {
	GetMeta() FutureMeta
	GetResult() (T, error)
	Await(timeout time.Duration) error
}

type TagMeta struct {
	FsmType uint8    `json:"deps"`
	TagKeys []string `json:"tagKeys"`
}
type DataWrapper struct {
	Deps         []FutureMeta `json:"deps"`
	Conds        []CondMeta   `json:"cond"`
	TagBuildMeta []TagMeta    `json:"tagBuildMeta"`
	Data         []byte       `json:"data"`
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

	AsyncSharedLogAppend(ctx context.Context, tags []uint64, tagBuildMeta []TagMeta, data []byte) (Future[uint64], error)
	AsyncSharedLogCondAppend(ctx context.Context, tags []uint64, tagBuildMeta []TagMeta, data []byte, cond func(CondHandle)) (Future[uint64], error)
	// TODO: deprecated, use blocking read without binary search is enough
	AsyncSharedLogReadNext(ctx context.Context, tag uint64, seqNum uint64) (*CondLogEntry, error)
	// async read API
	AsyncSharedLogRead(ctx context.Context, futureMeta FutureMeta) (*CondLogEntry, error)
	AsyncSharedLogReadIndex(ctx context.Context, futureMeta FutureMeta) (Future[uint64], error)
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
