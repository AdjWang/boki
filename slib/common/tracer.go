package common

import (
	"context"
	"fmt"
	"log"
)

type TracerType map[string][]int64

func ContextWithTracer(ctx context.Context) context.Context {
	return context.WithValue(ctx, "CTX_TRACE", make(TracerType))
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
	rawTracer := ctx.Value("CTX_TRACE")
	if rawTracer == nil {
		return
	}
	tracer := rawTracer.(TracerType)

	traceSummary := make(map[string]string)
	for fnName, records := range tracer {
		n, sum, max, min := summary(records)
		traceSummary[fnName] = fmt.Sprintf("%v(n=%v max=%v min=%v)", sum, n, max, min)
	}
	log.Printf("[%v] %+v", tag, traceSummary)
}

func AppendTrace(ctx context.Context, fnName string, latency int64) {
	rawTracer := ctx.Value("CTX_TRACE")
	if rawTracer == nil {
		return
	}
	tracer := rawTracer.(TracerType)
	tracer[fnName] = append(tracer[fnName], latency)
}
