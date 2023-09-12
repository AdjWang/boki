package worker

import (
	"fmt"
	"log"
	"time"

	"cs.utexas.edu/zjia/faas/protocol"
	types "cs.utexas.edu/zjia/faas/types"
	"github.com/montanaflynn/stats"
)

// Reorder response by response count
type ResponseBuffer interface {
	Enqueue(message []byte)
	Dequeue() []byte
	AwaitResolved()

	// DEBUG
	Inspect()
}

type DummyResponseBuffer struct {
	buffer         chan []byte
	signalResolved chan struct{}
}

func NewDummyResponseBuffer(reservedCapacity int) ResponseBuffer {
	return &DummyResponseBuffer{
		buffer:         make(chan []byte, reservedCapacity),
		signalResolved: make(chan struct{}, 1),
	}
}

func (drb *DummyResponseBuffer) checkResolved(message []byte) {
	flags := protocol.GetSharedLogOpFlagsFromMessage(message)
	if (flags & protocol.FLAG_kLogResponseContinueFlag) == 0 {
		drb.signalResolved <- struct{}{}
		// ensure only do once
		close(drb.signalResolved)
		close(drb.buffer)
	}
}
func (drb *DummyResponseBuffer) Enqueue(message []byte) {
	drb.buffer <- message
}

func (drb *DummyResponseBuffer) Dequeue() []byte {
	message := <-drb.buffer
	defer drb.checkResolved(message)
	return message
}

func (drb *DummyResponseBuffer) AwaitResolved() {
	<-drb.signalResolved
}

func (drb *DummyResponseBuffer) Inspect() {

}

type ResponseBufferImpl struct {
	ingress  chan []byte
	outgress *types.Queue[[]byte]

	responseId     uint64
	responseBuffer map[uint64][]byte

	resolved       bool
	signalResolved chan struct{}

	// DEBUG
	deliverTs        map[uint64]time.Time
	deliverDurations []float64
	bufferedCount    int
	totalCount       int
}

func NewResponseBuffer(reservedCapacity int) ResponseBuffer {
	rb := ResponseBufferImpl{
		ingress:  make(chan []byte),
		outgress: types.NewQueue[[]byte](reservedCapacity),

		responseId:     0,
		responseBuffer: make(map[uint64][]byte),

		resolved:       false,
		signalResolved: make(chan struct{}, 1),

		// DEBUG
		deliverTs:        make(map[uint64]time.Time),
		deliverDurations: make([]float64, 0, 100),
		bufferedCount:    0,
		totalCount:       0,
	}
	go rb.worker()
	return &rb
}
func (rb *ResponseBufferImpl) AwaitResolved() {
	<-rb.signalResolved
}

// DEBUG
func (rb *ResponseBufferImpl) Inspect() {
	p30, _ := stats.Percentile(rb.deliverDurations, 30.0)
	p50, _ := stats.Percentile(rb.deliverDurations, 50.0)
	p70, _ := stats.Percentile(rb.deliverDurations, 70.0)
	p90, _ := stats.Percentile(rb.deliverDurations, 90.0)
	p99, _ := stats.Percentile(rb.deliverDurations, 99.0)
	p100, _ := stats.Percentile(rb.deliverDurations, 100.0)
	log.Printf("[DEBUG] ResponseBuffer buffered=%v total=%v deliver stats=(30:%v 50:%v 70:%v 90:%v 99:%v max:%v)",
		rb.bufferedCount, rb.totalCount, p30, p50, p70, p90, p99, p100)
}

func (rb *ResponseBufferImpl) worker() {
	for {
		message, ok := <-rb.ingress
		if !ok {
			break
		}
		responseId := protocol.GetResponseIdFromMessage(message)
		// log.Printf("[DEBUG] ResponseBuffer received %v", protocol.InspectMessage(message))
		// DEBUG
		rb.deliverTs[responseId] = time.Now()
		rb.totalCount++

		if responseId == rb.responseId {
			rb.outputMessage(message)
			rb.responseId++
			for {
				if bufferedMessage, found := rb.responseBuffer[rb.responseId]; found {
					rb.outputMessage(bufferedMessage)
					delete(rb.responseBuffer, rb.responseId)
					rb.responseId++
				} else {
					break
				}
			}
		} else {
			rb.responseBuffer[responseId] = message
			rb.bufferedCount++
		}
	}
}

func (rb *ResponseBufferImpl) checkResolved(message []byte) {
	flags := protocol.GetSharedLogOpFlagsFromMessage(message)
	if (flags & protocol.FLAG_kLogResponseContinueFlag) == 0 {
		// DEBUG
		// log.Printf("[DEBUG] ResponseBuffer resolved id=%v %v",
		// 	rb.responseId, protocol.InspectMessage(message))
		rb.resolved = true
		rb.signalResolved <- struct{}{}
		// ensure only do once
		close(rb.signalResolved)
		close(rb.ingress)

		// DEBUG: print debug info
		// rb.Inspect()
	}
}

func (rb *ResponseBufferImpl) outputMessage(message []byte) {
	if rb.resolved {
		panic("output message after resolved")
	}
	ts, found := rb.deliverTs[rb.responseId]
	if !found {
		panic(fmt.Errorf("respid=%v not found", rb.responseId))
	}
	// log.Printf("[DEBUG] ResponseBuffer output id=%v deliverTime=%vus msg=%v",
	// 	rb.responseId, time.Since(ts).Microseconds(), protocol.InspectMessage(message))
	rb.deliverDurations = append(rb.deliverDurations, float64(time.Since(ts).Microseconds()))

	rb.outgress.Enqueue(message)
	rb.checkResolved(message)
}

func (rb *ResponseBufferImpl) Enqueue(message []byte) {
	rb.ingress <- message
}

func (rb *ResponseBufferImpl) Dequeue() []byte {
	message := rb.outgress.BlockingDequeue()
	// log.Printf("[DEBUG] SharedLogOp output cid=%v %v", rb.cid, protocol.InspectMessage(message))
	return message
}
