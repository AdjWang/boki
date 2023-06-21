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
type futureImpl[T uint64 | *CondLogEntry] struct {
	LocalId uint64
	// result union
	result T
	err    error

	// sync
	wg       sync.WaitGroup
	resolved atomic.Bool
}

func NewFuture[T uint64 | *CondLogEntry](localId uint64, resolve func() (T, error)) Future[T] {
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
		res, err := resolve()
		if err == nil {
			fu.result = res
			fu.err = nil
		} else {
			fu.err = err
		}
		fu.wg.Done()
		future.resolved.Store(true)
	}(future)
	return future
}

func (f *futureImpl[T]) GetResult() (T, error) {
	f.wg.Wait()
	return f.result, f.err
}

func (f *futureImpl[T]) Await(timeout time.Duration) error {
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

func (f *futureImpl[T]) Resolved() bool {
	return f.resolved.Load()
}
