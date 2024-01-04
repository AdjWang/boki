package types

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
)

func GroupLocalIdByEngine(localIds []uint64) map[uint16][]uint64 {
	grouped := make(map[uint16][]uint64)
	for _, id := range localIds {
		engineId := uint16((id & 0x0000FFFF00000000) >> 32)
		if group_ids, ok := grouped[engineId]; ok {
			group_ids = append(group_ids, id)
			grouped[engineId] = group_ids
		} else {
			grouped[engineId] = []uint64{id}
		}
	}
	return grouped
}

var InvalidFutureMeta = FutureMeta{LocalId: math.MaxUint64}

// Serializable
type FutureMeta struct {
	// for append, it's the localid, also book id, generated by log.engine.LogProducer
	// for read, it's the local op id generated by worker
	LocalId uint64 `json:"localid"`
}

func (fm FutureMeta) Serialize() ([]byte, error) {
	return json.Marshal(fm)
}
func DeserializeFutureMeta(data []byte) (FutureMeta, error) {
	var fm FutureMeta
	err := json.Unmarshal(data, &fm)
	return fm, errors.Wrapf(err, "invalid data: %v", data)
}

func (fm FutureMeta) IsValid() bool {
	return fm != InvalidFutureMeta
}

// Implement types.Future
type futureImpl[T uint64 | *CondLogEntry] struct {
	FutureMeta
	// result union
	result T
	err    error

	// sync
	wg sync.WaitGroup
}

func NewFuture[T uint64 | *CondLogEntry](localId uint64, resolve func() (T, error)) Future[T] {
	var emptyRes T
	future := &futureImpl[T]{
		FutureMeta: FutureMeta{
			LocalId: localId,
		},
		result: emptyRes,
		err:    nil,
		wg:     sync.WaitGroup{},
	}
	future.wg.Add(1)
	go func(fu *futureImpl[T]) {
		res, err := resolve()
		if err == nil {
			fu.result = res
			fu.err = nil
		} else {
			fu.err = err
		}
		fu.wg.Done()
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

func (f *futureImpl[T]) GetMeta() FutureMeta {
	return FutureMeta{
		LocalId: f.LocalId,
	}
}
