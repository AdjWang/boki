package types

import (
	"math"
	"sync/atomic"
	"time"

	"cs.utexas.edu/zjia/faas/protocol"
)

var InvalidLocalId uint64 = math.MaxUint64

func IsLocalIdValid(localId uint64) bool {
	return localId != InvalidLocalId
}

// Implement types.Future
type futureImpl[T uint64 | *LogEntryWithMeta] struct {
	LocalId uint64 // always available
	SeqNum  uint64 // invalid when appending, always available when reading
	// result union
	result T
	err    error
	// sync
	fn_resolve func() (T, error)
	resolved   atomic.Bool
}

func NewFuture[T uint64 | *LogEntryWithMeta](localId uint64, seqNum uint64, resolve func() (T, error)) Future[T] {
	var emptyRes T
	future := &futureImpl[T]{
		LocalId: localId,
		SeqNum:  seqNum,

		result: emptyRes,
		err:    nil,

		fn_resolve: resolve,
		resolved:   atomic.Bool{},
	}
	future.resolved.Store(false)
	return future
}

// speed up cached read
// dummy future only wraps data, the resolve function must be finished
func NewDummyFuture[T uint64 | *LogEntryWithMeta](localId uint64, seqNum uint64, resolve func() (T, error)) Future[T] {
	var emptyRes T
	future := &futureImpl[T]{
		LocalId: localId,
		SeqNum:  seqNum,

		result: emptyRes,
		err:    nil,

		fn_resolve: resolve,
		resolved:   atomic.Bool{},
	}
	future.result, future.err = resolve()
	future.resolved.Store(true)
	return future
}

func (f *futureImpl[T]) GetResult(timeout time.Duration) (T, error) {
	if !f.resolved.Load() {
		f.result, f.err = f.fn_resolve()
		f.resolved.Store(true)
	}
	return f.result, f.err
}

func (f *futureImpl[T]) GetLocalId() uint64 {
	return f.LocalId
}

func (f *futureImpl[T]) GetSeqNum() uint64 {
	return f.SeqNum
}

func (f *futureImpl[T]) GetLogEntryIndex() LogEntryIndex {
	return LogEntryIndex{
		LocalId: f.LocalId,
		SeqNum:  f.SeqNum,
	}
}

func (f *futureImpl[T]) IsResolved() bool {
	return f.resolved.Load()
}

// specificialize future to read and write

type readFutureImpl struct {
	future futureImpl[*LogEntryWithMeta]
}

func NewReadFuture(localId uint64, seqNum uint64, resolve func() (*LogEntryWithMeta, error)) Future[*LogEntryWithMeta] {
	return NewFuture[*LogEntryWithMeta](localId, seqNum, resolve)
}

func (rFuture *readFutureImpl) GetLocalId() uint64 {
	panic("not implemented")
}

func (rFuture *readFutureImpl) GetSeqNum() uint64 {
	// TODO: handle timeout and error
	logEntry, _ := rFuture.future.GetResult(60 * time.Second)
	return logEntry.SeqNum
}

func (rFuture *readFutureImpl) GetLogEntry() *LogEntryWithMeta {
	// TODO: handle timeout and error
	logEntry, _ := rFuture.future.GetResult(60 * time.Second)
	return logEntry
}

func (rFuture *readFutureImpl) IsResolved() bool {
	return rFuture.future.IsResolved()
}

type writeFutureImpl struct {
	future futureImpl[uint64]
}

func NewWriteFuture(localId uint64, resolve func() (uint64, error)) Future[uint64] {
	// seqNum is invalid when appending
	return NewFuture[uint64](localId, protocol.MaxLogSeqnum, resolve)
}

func (wFuture *writeFutureImpl) GetLocalId() uint64 {
	return wFuture.future.GetLocalId()
}

func (wFuture *writeFutureImpl) GetSeqNum() uint64 {
	// TODO: handle timeout and error
	seqNum, _ := wFuture.future.GetResult(60 * time.Second)
	return seqNum
}

func (wFuture *writeFutureImpl) GetLogEntry() *LogEntryWithMeta {
	panic("not implemented")
}

func (wFuture *writeFutureImpl) IsResolved() bool {
	return wFuture.future.IsResolved()
}
