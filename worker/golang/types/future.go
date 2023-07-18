package types

import (
	"context"
	"math"
	"sync"
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
	wg       sync.WaitGroup
	resolved atomic.Bool
}

func NewFuture[T uint64 | *LogEntryWithMeta](localId uint64, seqNum uint64, resolve func() (T, error)) Future[T] {
	var emptyRes T
	future := &futureImpl[T]{
		LocalId: localId,
		SeqNum:  seqNum,

		result: emptyRes,
		err:    nil,

		wg:       sync.WaitGroup{},
		resolved: atomic.Bool{},
	}
	future.wg.Add(1)
	future.resolved.Store(false)
	go func(fu *futureImpl[T]) {
		fu.result, fu.err = resolve()
		fu.wg.Done()
		fu.resolved.Store(true)
	}(future)
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

		wg:       sync.WaitGroup{},
		resolved: atomic.Bool{},
	}
	future.result, future.err = resolve()
	future.resolved.Store(true)
	return future
}

func (f *futureImpl[T]) GetResult(timeout time.Duration) (T, error) {
	if !f.resolved.Load() {
		if err := f.Await(timeout); err != nil {
			return f.result, err
		}
	}
	return f.result, f.err
}

func (f *futureImpl[T]) Await(timeout time.Duration) error {
	if f.resolved.Load() {
		return nil
	}

	// log.Printf("wait future=%+v with timeout=%v", f, timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	awaitDone := make(chan struct{})
	go func() {
		f.wg.Wait()
		awaitDone <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		// log.Printf("wait future=%+v timeout", f)
		return ctx.Err()
	case <-awaitDone:
		// log.Printf("wait future=%+v without error", f)
		return nil
	}
}

func (f *futureImpl[T]) GetLocalId() uint64 {
	return f.LocalId
}

func (f *futureImpl[T]) GetSeqNum() uint64 {
	return f.SeqNum
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
