package utils

import (
	"sync"
	"time"
)

type TracerGroup struct {
	traceMu  sync.Mutex
	counters map[string]*CounterCollector
	stats    map[string]*StatisticsCollector
}

func NewTracer() *TracerGroup {
	client := &TracerGroup{
		traceMu:  sync.Mutex{},
		counters: make(map[string]*CounterCollector),
		stats:    make(map[string]*StatisticsCollector),
	}
	return client
}
func (tg *TracerGroup) AddTrace(tag string, sampleUs int64) {
	tg.traceMu.Lock()
	defer tg.traceMu.Unlock()

	if _, found := tg.counters[tag]; !found {
		tg.counters[tag] = NewCounterCollector(tag, 10*time.Second)
	}
	if _, found := tg.stats[tag]; !found {
		tg.stats[tag] = NewStatisticsCollector(tag, 1000 /*reportSamples*/, 10*time.Second)
	}
	tg.counters[tag].Tick(1)
	tg.stats[tag].AddSample(float64(sampleUs))
}
