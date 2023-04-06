package types

import (
	"fmt"
	"testing"
)

func TestOpPropagate(t *testing.T) {
	var rawData []byte
	{
		client := condImpl{}
		future := NewFuture(1 /*localid*/, func() (uint64, error) {
			return 2 /*seqnum*/, nil
		})
		client.Read(3 /*tag*/, future)
		data, err := client.Ops[0].Serialize()
		if err != nil {
			t.Fatalf("serialize op error: %v", err)
		}
		rawData = data
	}
	// dummy propagate...
	// restore
	{
		op, err := DeserializeOp(rawData)
		if err != nil {
			t.Fatalf("deserialize op error: %v", err)
		}
		res := CheckOp(*op)
		fmt.Printf("check op: %v\n", res)
	}
}
