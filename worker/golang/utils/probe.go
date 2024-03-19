package utils

import (
	"encoding/json"
	"log"
	"sync"
)

type ProbeContext struct {
	mu     sync.Mutex
	values map[string]interface{}
}

func NewProbeCtx() *ProbeContext {
	return &ProbeContext{
		mu:     sync.Mutex{},
		values: make(map[string]interface{}),
	}
}

func NewProbeCtxFromString(rawData string) *ProbeContext {
	if rawData == "" || rawData == "{}" {
		return NewProbeCtx()
	}
	var probeValues map[string]interface{}
	err := json.Unmarshal([]byte(rawData), &probeValues)
	if err != nil {
		log.Panicln(err)
	}
	probeCtx := NewProbeCtx()
	probeCtx.values = probeValues
	return probeCtx
}

func (p *ProbeContext) ToString() string {
	p.mu.Lock()
	rawData, err := json.Marshal(p.values)
	p.mu.Unlock()
	if err != nil {
		panic(err)
	}
	return string(rawData)
}

func (p *ProbeContext) Value(k string) interface{} {
	p.mu.Lock()
	v, ok := p.values[k]
	p.mu.Unlock()
	if ok {
		return v
	} else {
		return nil
	}
}

func (p *ProbeContext) WithValue(k string, v interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.values[k] = v
}

// Last write win
func (p *ProbeContext) Merge(other *ProbeContext) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, v := range other.values {
		p.values[k] = v
	}
}
