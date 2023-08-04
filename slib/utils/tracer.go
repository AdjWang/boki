package utils

import (
	"fmt"
	"time"
)

type TracePoint struct {
	timestamp  time.Time
	timestamp2 time.Time
	tip        string
}

type Tracer struct {
	tracePoints []*TracePoint
}

func NewTracer() *Tracer {
	t := &Tracer{
		tracePoints: make([]*TracePoint, 0, 10),
	}
	t.Trace().Tip("initial point")
	return t
}

func (t *Tracer) Trace() *Tracer {
	tracePoint := &TracePoint{
		timestamp: time.Now(),
		tip:       "",
	}
	t.tracePoints = append(t.tracePoints, tracePoint)
	return t
}

// not let tip evaluation time to be in the measurement
func (t *Tracer) Tip(tip string) {
	t.tracePoints[len(t.tracePoints)-1].tip = "[TRACE] " + tip
	t.tracePoints[len(t.tracePoints)-1].timestamp2 = time.Now()
}

func (t *Tracer) String() string {
	n := len(t.tracePoints)
	if n <= 1 {
		return ""
	}
	totalTime := int64(0)
	for i := 0; i < n-1; i++ {
		pointA, pointB := t.tracePoints[i], t.tracePoints[i+1]
		duration := pointB.timestamp.Sub(pointA.timestamp2).Microseconds()
		totalTime += duration
	}
	output := ""
	for i := 0; i < n-1; i++ {
		pointA, pointB := t.tracePoints[i], t.tracePoints[i+1]
		duration := pointB.timestamp.Sub(pointA.timestamp2).Microseconds()
		ratio := float64(duration) / float64(totalTime) * 100.0
		output += fmt.Sprintf("%s: %d us, %.1f%%\n", pointB.tip, duration, ratio)
	}
	return output
}
