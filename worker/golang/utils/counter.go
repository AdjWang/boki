package utils

import (
	"log"
	"sync"
	"time"
)

type CounterCollector struct {
	// configs
	reportInterval time.Duration

	title             string
	value             uint64
	last_report_value uint64
	lastReportTime    time.Time

	mu sync.Mutex
}

func NewCounterCollector(title string, interval time.Duration) *CounterCollector {
	cc := &CounterCollector{
		reportInterval: interval,

		title:             title,
		value:             uint64(0),
		last_report_value: uint64(0),
		lastReportTime:    time.Now(),

		mu: sync.Mutex{},
	}
	// force reporting to see throughput over time
	// go func() {
	// 	for {
	// 		time.Sleep(10 * time.Second)
	// 		cc.mu.Lock()
	// 		elapsed := time.Since(cc.lastReportTime)
	// 		log.Printf("[STAT] %v counter value=%d rate=%.1f per second",
	// 			cc.title, cc.value, float64(cc.value-cc.last_report_value)/elapsed.Seconds())
	// 		cc.mu.Unlock()
	// 	}
	// }()
	return cc
}
func (cc *CounterCollector) Tick(n uint64) {
	// add sample
	cc.value += n
	// check report
	elapsed := time.Since(cc.lastReportTime)
	if cc.value > cc.last_report_value && elapsed > cc.reportInterval {
		cc.lastReportTime = time.Now()
		log.Printf("[STAT] %v counter value=%d rate=%.1f per second",
			cc.title, cc.value, float64(cc.value-cc.last_report_value)/elapsed.Seconds())
		cc.last_report_value = cc.value
	}
}
