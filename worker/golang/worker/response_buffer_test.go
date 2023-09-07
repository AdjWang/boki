package worker

import (
	"encoding/binary"
	"math/rand"
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

func generateMessageGroup(n int, shuffle bool) [][]byte {
	if n <= 0 {
		return nil
	}
	msgSize := 2816
	messages := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		dummyMessage := make([]byte, msgSize)
		// set reqId
		binary.LittleEndian.PutUint64(dummyMessage[40:48], uint64(i))
		// set flags
		if i < n-1 {
			binary.LittleEndian.PutUint32(dummyMessage[28:32], protocol.FLAG_kLogResponseContinueFlag)
		} else {
			binary.LittleEndian.PutUint32(dummyMessage[28:32], protocol.FLAG_kLogResponseEOFFlag)
		}
		messages = append(messages, dummyMessage)
	}
	if shuffle {
		// shuffle
		rand.Shuffle(n, func(i, j int) {
			messages[i], messages[j] = messages[j], messages[i]
		})
	}
	return messages
}

func BenchmarkResponseBuffer(b *testing.B) {
	reservedCap := 100
	nMsg := 1

	messages := generateMessageGroup(nMsg /*n msg*/, false /*shuffle*/)
	b.Run("raw", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			channel := make(chan []byte)
			go func() {
				for i := 0; i < nMsg; i++ {
					channel <- messages[i]
				}
			}()
			count := 0
			for msg := range channel {
				count++
				flag := protocol.GetFlagsFromMessage(msg)
				if (flag & protocol.FLAG_kLogResponseContinueFlag) == 0 {
					break
				}
			}
			if count != nMsg {
				b.Fatalf("inconsistent n in=%v out=%v", nMsg, count)
			}
		}
	})

	messages = generateMessageGroup(nMsg /*n msg*/, false /*shuffle*/)
	b.Run("ordered", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rb := NewResponseBuffer(reservedCap)
			outputCh := rb.DequeueCh()
			go func() {
				for i := 0; i < nMsg; i++ {
					rb.Enqueue(messages[i])
				}
			}()
			count := 0
			for msg := range outputCh {
				count++
				flag := protocol.GetFlagsFromMessage(msg)
				if (flag & protocol.FLAG_kLogResponseContinueFlag) == 0 {
					break
				}
			}
			if count != nMsg {
				b.Fatalf("inconsistent n in=%v out=%v", nMsg, count)
			}
		}
	})

	messages = generateMessageGroup(nMsg /*n msg*/, true /*shuffle*/)
	b.Run("disordered", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rb := NewResponseBuffer(reservedCap)
			outputCh := rb.DequeueCh()
			go func() {
				for i := 0; i < nMsg; i++ {
					rb.Enqueue(messages[i])
				}
			}()
			count := 0
			for msg := range outputCh {
				count++
				flag := protocol.GetFlagsFromMessage(msg)
				if (flag & protocol.FLAG_kLogResponseContinueFlag) == 0 {
					break
				}
			}
			if count != nMsg {
				b.Fatalf("inconsistent n in=%v out=%v", nMsg, count)
			}
		}
	})
}
