package worker

import "cs.utexas.edu/zjia/faas/protocol"

// Reorder response by response count
type ResponseBuffer struct {
	ingress  chan []byte
	outgress chan []byte

	responseCount  uint16
	responseBuffer map[uint16][]byte
}

func NewResponseBuffer(capacity int) *ResponseBuffer {
	rb := ResponseBuffer{
		ingress:  make(chan []byte, capacity),
		outgress: make(chan []byte, capacity),

		responseCount:  0,
		responseBuffer: make(map[uint16][]byte),
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
		responseCount := protocol.GetResponseCountFromMessage(message)
		if responseCount == rb.responseCount {
			rb.outgress <- message
			rb.responseCount++
			for {
				if bufferedMessage, found := rb.responseBuffer[rb.responseCount]; found {
					rb.outgress <- bufferedMessage
					delete(rb.responseBuffer, rb.responseCount)
					rb.responseCount++
				} else {
					break
				}
			}
		} else {
			rb.responseBuffer[responseCount] = message
		}
	}
}

func (rb *ResponseBuffer) Enqueue(message []byte) {
	rb.ingress <- message
}

func (rb *ResponseBuffer) Dequeue() []byte {
	return <-rb.outgress
}
