package worker

import (
	"encoding/binary"
	"testing"

	"cs.utexas.edu/zjia/faas/protocol"
)

func TestResponseBuffer(t *testing.T) {
	dummyMessage0 := make([]byte, 64)
	dummyMessage1 := make([]byte, 64)
	dummyMessage2 := make([]byte, 64)
	// set reqId
	binary.LittleEndian.PutUint64(dummyMessage0[40:48], 0)
	binary.LittleEndian.PutUint64(dummyMessage1[40:48], 1)
	binary.LittleEndian.PutUint64(dummyMessage2[40:48], 2)
	// set flags
	binary.LittleEndian.PutUint32(dummyMessage0[28:32], protocol.FLAG_kLogResponseContinueFlag)
	binary.LittleEndian.PutUint32(dummyMessage1[28:32], protocol.FLAG_kLogResponseContinueFlag)
	binary.LittleEndian.PutUint32(dummyMessage2[28:32], protocol.FLAG_kLogResponseEOFFlag)
	{
		rb := NewResponseBuffer(3)
		rb.Enqueue(dummyMessage0)
		rb.Enqueue(dummyMessage1)
		rb.Enqueue(dummyMessage2)
		msg := rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 0 {
			t.Fatalf("response count=%v, need=%v", rc, 0)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 1 {
			t.Fatalf("response count=%v, need=%v", rc, 1)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 2 {
			t.Fatalf("response count=%v, need=%v", rc, 2)
		}
	}
	{
		rb := NewResponseBuffer(3)
		rb.Enqueue(dummyMessage1)
		rb.Enqueue(dummyMessage0)
		rb.Enqueue(dummyMessage2)
		msg := rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 0 {
			t.Fatalf("response count=%v, need=%v", rc, 0)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 1 {
			t.Fatalf("response count=%v, need=%v", rc, 1)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 2 {
			t.Fatalf("response count=%v, need=%v", rc, 2)
		}
	}
	{
		rb := NewResponseBuffer(3)
		rb.Enqueue(dummyMessage2)
		rb.Enqueue(dummyMessage1)
		rb.Enqueue(dummyMessage0)
		msg := rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 0 {
			t.Fatalf("response count=%v, need=%v", rc, 0)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 1 {
			t.Fatalf("response count=%v, need=%v", rc, 1)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 2 {
			t.Fatalf("response count=%v, need=%v", rc, 2)
		}
	}
	{
		rb := NewResponseBuffer(3)
		rb.Enqueue(dummyMessage2)
		rb.Enqueue(dummyMessage0)
		rb.Enqueue(dummyMessage1)
		msg := rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 0 {
			t.Fatalf("response count=%v, need=%v", rc, 0)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 1 {
			t.Fatalf("response count=%v, need=%v", rc, 1)
		}
		msg = rb.Dequeue()
		if rc := protocol.GetResponseIdFromMessage(msg); rc != 2 {
			t.Fatalf("response count=%v, need=%v", rc, 2)
		}
	}
}
