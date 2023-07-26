package types

import "sync"

// Not reclaim values, since we don't expect too many values would enqueue
type Queue[T any] struct {
	nextPos int
	buffer  []T
	sigMu   *sync.Mutex
	signal  *sync.Cond
}

func NewQueue[T any](reservedCapacity int) *Queue[T] {
	q := &Queue[T]{
		nextPos: 0,
		buffer:  make([]T, 0, reservedCapacity),
		sigMu:   &sync.Mutex{},
	}
	q.signal = sync.NewCond(q.sigMu)
	return q
}

func (q *Queue[T]) Enqueue(value T) {
	q.signal.L.Lock()
	q.buffer = append(q.buffer, value)
	q.signal.Signal()
	q.signal.L.Unlock()
}

func (q *Queue[T]) BlockingDequeue() T {
	q.signal.L.Lock()
	for q.isEmpty() {
		q.signal.Wait()
	}
	value := q.buffer[q.nextPos]
	q.nextPos++
	q.signal.L.Unlock()
	return value
}

func (q *Queue[T]) isEmpty() bool {
	return q.nextPos >= len(q.buffer)
}
