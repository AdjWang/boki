package worker

import (
	"encoding/binary"
	"testing"

	"cs.utexas.edu/zjia/faas/protocol"
)

func TestResponseBuffer(t *testing.T) {
	dummyMessage0 := make([]byte, 64)
	binary.LittleEndian.PutUint16(dummyMessage0[28:30], 0)
	dummyMessage1 := make([]byte, 64)
	binary.LittleEndian.PutUint16(dummyMessage1[28:30], 1)
	dummyMessage2 := make([]byte, 64)
	binary.LittleEndian.PutUint16(dummyMessage2[28:30], 2)

	{
		rb := NewResponseBuffer(3)
		rb.Enqueue(dummyMessage0)
		rb.Enqueue(dummyMessage1)
		rb.Enqueue(dummyMessage2)
		msg := rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 0 {
			t.Fatalf("response count=%v, need=%v", rc, 0)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 1 {
			t.Fatalf("response count=%v, need=%v", rc, 1)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 2 {
			t.Fatalf("response count=%v, need=%v", rc, 2)
		}
	}
	{
		rb := NewResponseBuffer(3)
		rb.Enqueue(dummyMessage1)
		rb.Enqueue(dummyMessage0)
		rb.Enqueue(dummyMessage2)
		msg := rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 0 {
			t.Fatalf("response count=%v, need=%v", rc, 0)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 1 {
			t.Fatalf("response count=%v, need=%v", rc, 1)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 2 {
			t.Fatalf("response count=%v, need=%v", rc, 2)
		}
	}
	{
		rb := NewResponseBuffer(3)
		rb.Enqueue(dummyMessage2)
		rb.Enqueue(dummyMessage1)
		rb.Enqueue(dummyMessage0)
		msg := rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 0 {
			t.Fatalf("response count=%v, need=%v", rc, 0)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 1 {
			t.Fatalf("response count=%v, need=%v", rc, 1)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseCountFromMessage(msg); rc != 2 {
			t.Fatalf("response count=%v, need=%v", rc, 2)
		}
	}

}
