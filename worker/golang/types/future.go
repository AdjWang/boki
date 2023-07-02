package types

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var InvalidLocalId uint64 = math.MaxUint64

func IsLocalIdValid(localId uint64) bool {
	return localId != InvalidLocalId
}

// Implement types.Future
type futureImpl[T uint64 | *LogEntryWithMeta] struct {
	LocalId uint64
	// result union
	result T
	err    error

	// sync
	wg       sync.WaitGroup
	resolved atomic.Bool
}

func NewFuture[T uint64 | *LogEntryWithMeta](localId uint64, resolve func() (T, error)) Future[T] {
	var emptyRes T
	future := &futureImpl[T]{
		LocalId: localId,
		result:  emptyRes,
		err:     nil,

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
func NewDummyFuture[T uint64 | *LogEntryWithMeta](localId uint64, resolve func() (T, error)) Future[T] {
	var emptyRes T
	future := &futureImpl[T]{
		LocalId: localId,
		result:  emptyRes,
		err:     nil,

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

func (f *futureImpl[T]) IsResolved() bool {
	return f.resolved.Load()
}
