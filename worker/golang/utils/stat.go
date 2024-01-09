package utils

import (
	"log"
	"time"

	"github.com/montanaflynn/stats"
)

type StatisticsCollector struct {
	// configs
	reportSamples  int
	reportInterval time.Duration

	title          string
	samples        []float64
	lastReportTime time.Time
}

func NewStatisticsCollector(title string, reportSamples int, interval time.Duration) *StatisticsCollector {
	sc := &StatisticsCollector{
		reportSamples:  reportSamples,
		reportInterval: interval,

		title:          title,
		samples:        make([]float64, 0, 1000),
		lastReportTime: time.Now(),
	}
	return sc
}

func (sc *StatisticsCollector) AddSample(item float64) {
	// add sample
	sc.samples = append(sc.samples, item)
	// check report
	elapsed := time.Since(sc.lastReportTime)
	if len(sc.samples) >= sc.reportSamples && elapsed > sc.reportInterval {
		stat_res := getStatistics(sc.samples)
		sc.samples = sc.samples[:0] // clear all
		sc.lastReportTime = time.Now()
		log.Printf("[STAT] %v statistics (%v samples %.1f ops) p30=%.2f p50=%.2f p70=%.2f p90=%.2f p99=%.2f p99.9=%.2f",
			sc.title, stat_res.num, float64(stat_res.num)/elapsed.Seconds(),
			stat_res.p30, stat_res.p50, stat_res.p70, stat_res.p90, stat_res.p99, stat_res.p99_9)
	}
}

type statistics struct {
	num   int
	p30   float64
	p50   float64
	p70   float64
	p90   float64
	p99   float64
	p99_9 float64
}

func getStatistics(datas []float64) statistics {
	p30, _ := stats.Percentile(datas, 30.0)
	p50, _ := stats.Percentile(datas, 50.0)
	p70, _ := stats.Percentile(datas, 70.0)
	p90, _ := stats.Percentile(datas, 90.0)
	p99, _ := stats.Percentile(datas, 99.0)
	p99_9, _ := stats.Percentile(datas, 99.9)
	return statistics{
		num:   len(datas),
		p30:   p30,
		p50:   p50,
		p70:   p70,
		p90:   p90,
		p99:   p99,
		p99_9: p99_9,
	}
}
