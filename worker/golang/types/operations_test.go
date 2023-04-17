package types

import (
	"reflect"
	"testing"
)

func TestCondPropagate(t *testing.T) {
	var rawData []byte
	future := NewFuture(1 /*localid*/, func() (uint64, error) {
		return 2 /*seqnum*/, nil
	})
	{
		client := condImpl{}
		client.AddDep(future.GetMeta())
		client.AddCond(1)
		rawData = client.WrapData([]TagMeta{}, []byte{})
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
		if !reflect.DeepEqual(client.Deps, []FutureMeta{future.GetMeta()}) {
			t.Fatalf("unexpected deps: %+v, expected %+v", client.Deps, []FutureMeta{future.GetMeta()})
		}
		if !reflect.DeepEqual(client.CondMetas, []CondMeta{{1}}) {
			t.Fatalf("unexpected deps: %+v, expected %+v", client.CondMetas, []CondMeta{{1}})
		}
	}
}
