package common

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type APITracer struct {
	mu      sync.Mutex
	records map[string][]int64
}

func NewAPITracer() *APITracer {
	return &APITracer{
		mu:      sync.Mutex{},
		records: make(map[string][]int64),
	}
}

func (t *APITracer) AppendTrace(name string, latency int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.records[name] = append(t.records[name], latency)
}

func (t *APITracer) PrintTrace(tag string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	traceSummary := make(map[string]string)
	for name, records := range t.records {
		n, sum, max, min := summary(records)
		traceSummary[name] = fmt.Sprintf("%v(n=%v max=%v min=%v)", sum, n, max, min)
	}
	log.Printf("[%v] %+v", tag, traceSummary)
}

// https://pkg.go.dev/context#WithValue
type CtxTrace struct{}

func ContextWithTracer(ctx context.Context) context.Context {
	return context.WithValue(ctx, CtxTrace{}, NewAPITracer())
}

func summary(datas []int64) (n int, sum int64, max int64, min int64) {
	min = (1 << 32)
	for _, i := range datas {
		sum += i
		if max < i {
			max = i
		}
		if min > i {
			min = i
		}
	}
	n = len(datas)
	return
}

func PrintTrace(ctx context.Context, tag string) {
	rawTracer := ctx.Value(CtxTrace{})
	if rawTracer == nil {
		return
	}
	tracer := rawTracer.(*APITracer)
	tracer.PrintTrace(tag)
}

func AppendTrace(ctx context.Context, fnName string, latency int64) {
	rawTracer := ctx.Value(CtxTrace{})
	if rawTracer == nil {
		return
	}
	tracer := rawTracer.(*APITracer)
	tracer.AppendTrace(fnName, latency)
}
