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
type CondLogEntry struct {
	LogEntry
	Deps []FutureMeta
	Cond []Op
}

type Future[T uint64 | *CondLogEntry] interface {
	GetMeta() FutureMeta
	GetLocalId() uint64 // TODO: merge with GetMeta()
	GetResult() (T, error)
	Await(timeout time.Duration) error
}

type AsyncLogContext interface {
	Chain(future FutureMeta) AsyncLogContext
	Sync(timeout time.Duration) error
	Serialize() ([]byte, error)

	// DEBUG
	String() string
}

type WrapperData struct {
	Deps []FutureMeta `json:"deps"`
	Cond []Op         `json:"cond"`
	Data []byte       `json:"data"`
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

	AsyncSharedLogAppend(ctx context.Context, tags []uint64, data []byte) (Future[uint64], error)
	AsyncSharedLogCondAppend(ctx context.Context, tags []uint64, data []byte, cond func(CondHandle)) (Future[uint64], error)
	// TODO: deprecated, use blocking read without binary search is enough
	AsyncSharedLogReadNext(ctx context.Context, tag uint64, future Future[uint64]) (Future[*CondLogEntry], error)
	// async read API
	AsyncSharedLogRead(ctx context.Context, tag uint64, future Future[uint64]) (*CondLogEntry, error)
	AsyncSharedLogReadIndex(ctx context.Context, futureMeta FutureMeta) (Future[uint64], error)
	// async log context
	AsyncLogCtx() AsyncLogContext
	NewAsyncLogCtx(data []byte) error
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
