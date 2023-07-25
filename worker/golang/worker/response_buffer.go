package worker

import (
	"fmt"
	"log"

	"cs.utexas.edu/zjia/faas/protocol"
	"github.com/enriquebris/goconcurrentqueue"
)

// Reorder response by response count
type ResponseBuffer struct {
	ingress  chan []byte
	outgress goconcurrentqueue.Queue

	responseId     uint64
	responseBuffer map[uint64][]byte

	resolved       bool
	SignalResolved chan struct{}

	// DEBUG
	Debug           bool
	debugMessageOps [][]byte
	cid             uint64
}

func NewResponseBuffer(capacity int, cid uint64) *ResponseBuffer {
	rb := ResponseBuffer{
		ingress:  make(chan []byte, capacity),
		outgress: goconcurrentqueue.NewFIFO(),

		responseId:     0,
		responseBuffer: make(map[uint64][]byte),

		resolved:       false,
		SignalResolved: make(chan struct{}, 1),

		// DEBUG
		Debug:           true,
		debugMessageOps: make([][]byte, 0, 20),
		cid:             cid,
	}
	go rb.worker()
	return &rb
}

func (rb *ResponseBuffer) worker() {
	for {
		message, ok := <-rb.ingress
		if !ok {
			break
		}
		// DEBUG
		rb.debugMessageOps = append(rb.debugMessageOps, message)

		responseId := protocol.GetResponseIdFromMessage(message)
		// DEBUG
		responseId &= 0x0000FFFFFFFFFFFF

		// log.Printf("[DEBUG] ResponseBuffer received %v", protocol.InspectMessage(message))
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
			// log.Printf("[DEBUG] ResponseBuffer buffered %v", protocol.InspectMessage(message))
			rb.responseBuffer[responseId] = message
		}
	}
}

func (rb *ResponseBuffer) checkResolved(message []byte) {
	flags := protocol.GetSharedLogOpFlagsFromMessage(message)
	if (flags & protocol.FLAG_kLogResponseContinueFlag) == 0 {
		// DEBUG
		// log.Printf("[DEBUG] ResponseBuffer resolved id=%v %v",
		// 	rb.responseId, protocol.InspectMessage(message))
		rb.resolved = true
		rb.SignalResolved <- struct{}{}
		// ensure only do once
		close(rb.SignalResolved)
		close(rb.ingress)
	}
}

func (rb *ResponseBuffer) outputMessage(message []byte) {
	if rb.resolved {
		if rb.Debug {
			log.Println(rb.debugMessageOps)
			log.Printf("buffer cid=%v", rb.cid)
			for _, msg := range rb.debugMessageOps {
				log.Println(protocol.InspectMessage(msg))
			}
		}
		panic("output message after resolved")
	}
	// log.Printf("[DEBUG] ResponseBuffer output %v", protocol.InspectMessage(message))
	if err := rb.outgress.Enqueue(message); err != nil {
		panic(err)
	}
	rb.checkResolved(message)
}

func (rb *ResponseBuffer) Enqueue(message []byte, cid uint64) {
	if rb.cid != cid {
		panic(fmt.Sprintf("inconsistent cid. initial=%v, current=%v", rb.cid, cid))
	}
	rb.ingress <- message
}

func (rb *ResponseBuffer) Dequeue() []byte {
	data, err := rb.outgress.DequeueOrWaitForNextElement()
	if err != nil {
		panic(err)
	}
	message := data.([]byte)
	// log.Printf("[DEBUG] SharedLogOp output cid=%v %v", rb.cid, protocol.InspectMessage(message))
	return message
}
