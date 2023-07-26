package worker

import (
	"log"

	"cs.utexas.edu/zjia/faas/protocol"
	types "cs.utexas.edu/zjia/faas/types"
)

// Reorder response by response count
type ResponseBuffer struct {
	ingress  chan []byte
	outgress *types.Queue[[]byte]

	responseId     uint64
	responseBuffer map[uint64][]byte

	resolved       bool
	SignalResolved chan struct{}

	// DEBUG
	debugIngressRecord      [][]byte
	debugIntermediateRecord [][]byte
	debugOutgressRecord     [][]byte
}

func NewResponseBuffer(reservedCapacity int) *ResponseBuffer {
	rb := ResponseBuffer{
		ingress:  make(chan []byte),
		outgress: types.NewQueue[[]byte](reservedCapacity),

		responseId:     0,
		responseBuffer: make(map[uint64][]byte),

		resolved:       false,
		SignalResolved: make(chan struct{}, 1),

		// DEBUG
		debugIngressRecord:      make([][]byte, 0, 20),
		debugIntermediateRecord: make([][]byte, 0, 20),
		debugOutgressRecord:     make([][]byte, 0, 20),
	}
	go rb.worker()
	return &rb
}

// DEBUG
func (rb *ResponseBuffer) Inspect() {
	log.Println("Ingress")
	log.Println(rb.debugIngressRecord)
	for _, message := range rb.debugIngressRecord {
		log.Println(protocol.InspectMessage(message))
	}
	log.Println("Intermediate")
	for _, message := range rb.debugIntermediateRecord {
		log.Println(protocol.InspectMessage(message))
	}
	log.Println("Outgress")
	for _, message := range rb.debugOutgressRecord {
		log.Println(protocol.InspectMessage(message))
	}
}

func (rb *ResponseBuffer) worker() {
	for {
		message, ok := <-rb.ingress
		if !ok {
			break
		}
		// DEBUG
		rb.debugIngressRecord = append(rb.debugIngressRecord, message)

		responseId := protocol.GetResponseIdFromMessage(message)
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
		panic("output message after resolved")
	}
	// log.Printf("[DEBUG] ResponseBuffer output %v", protocol.InspectMessage(message))
	// DEBUG
	rb.debugIntermediateRecord = append(rb.debugIntermediateRecord, message)

	rb.outgress.Enqueue(message)
	rb.checkResolved(message)
}

func (rb *ResponseBuffer) Enqueue(message []byte) {
	rb.ingress <- message
}

func (rb *ResponseBuffer) Dequeue() []byte {
	message := rb.outgress.BlockingDequeue()
	// DEBUG
	rb.debugOutgressRecord = append(rb.debugOutgressRecord, message)

	// log.Printf("[DEBUG] SharedLogOp output cid=%v %v", rb.cid, protocol.InspectMessage(message))
	return message
}
