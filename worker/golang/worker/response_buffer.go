package worker

import (
	"log"

	"cs.utexas.edu/zjia/faas/protocol"
)

// Reorder response by response count
type ResponseBuffer struct {
	ingress  chan []byte
	outgress chan []byte

	responseId     uint64
	responseBuffer map[uint64][]byte

	SignalResolved chan struct{}
}

func NewResponseBuffer(capacity int) *ResponseBuffer {
	rb := ResponseBuffer{
		ingress:  make(chan []byte, capacity),
		outgress: make(chan []byte, capacity),

		responseId:     0,
		responseBuffer: make(map[uint64][]byte),

		SignalResolved: make(chan struct{}, 1),
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
		responseId := protocol.GetResponseIdFromMessage(message)
		log.Printf("[DEBUG] ResponseBuffer received %v", protocol.InspectMessage(message))
		if responseId == rb.responseId {
			log.Printf("[DEBUG] ResponseBuffer given %v", protocol.InspectMessage(message))
			rb.checkResolved(message)
			rb.outgress <- message
			rb.responseId++
			for {
				if bufferedMessage, found := rb.responseBuffer[rb.responseId]; found {
					log.Printf("[DEBUG] ResponseBuffer given %v", protocol.InspectMessage(bufferedMessage))
					rb.checkResolved(message)
					rb.outgress <- bufferedMessage
					delete(rb.responseBuffer, rb.responseId)
					rb.responseId++
				} else {
					break
				}
			}
		} else {
			log.Printf("[DEBUG] ResponseBuffer buffer %v", protocol.InspectMessage(message))
			rb.responseBuffer[responseId] = message
		}
	}
}

func (rb *ResponseBuffer) checkResolved(message []byte) {
	flags := protocol.GetSharedLogOpFlagsFromMessage(message)
	if (flags & protocol.FLAG_kLogResponseContinueFlag) == 0 {
		log.Printf("[DEBUG] ResponseBuffer resolved id=%v", rb.responseId)
		rb.SignalResolved <- struct{}{}
		close(rb.SignalResolved) // ensure only do once
	}
}

func (rb *ResponseBuffer) Enqueue(message []byte) {
	rb.ingress <- message
}

func (rb *ResponseBuffer) Dequeue() []byte {
	return <-rb.outgress
}
