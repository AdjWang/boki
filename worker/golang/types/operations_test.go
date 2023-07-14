package types

import (
	"reflect"
	"testing"

	"cs.utexas.edu/zjia/faas/protocol"
)

func TestCondPropagate(t *testing.T) {
	var rawData []byte
	future := NewFuture(1 /*localid*/, protocol.MaxLogSeqnum, func() (uint64, error) {
		return 2 /*seqnum*/, nil
	})
	{
		client := logDataWrapperImpl{}
		client.WithDeps([]uint64{future.GetLocalId()})
		client.WithStreamTypes([]uint8{1})
		rawData = client.Build([]byte{})
	}
	// dummy propagate...
	// restore
	{
		client, restData, err := UnwrapData(rawData)
		if err != nil {
			t.Fatalf("deserialize op error: %v", err)
		}
		if !reflect.DeepEqual(restData, []byte{}) {
			t.Fatalf("expected empty data, got %v", restData)
		}
		if !reflect.DeepEqual(client.Deps, []uint64{future.GetLocalId()}) {
			t.Fatalf("unexpected deps: %+v, expected %+v", client.Deps, []uint64{future.GetLocalId()})
		}
		if !reflect.DeepEqual(client.StreamTypes, []uint8{1}) {
			t.Fatalf("unexpected stream types: %+v, expected %+v", client.StreamTypes, []uint8{1})
		}
	}
}
