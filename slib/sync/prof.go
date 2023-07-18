package sync

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	ProfType_Append = iota
	ProfType_SyncTo
	ProfType_AAR
	ProfType_FindNext
)

func ProfTypeToString(profType uint8) string {
	switch profType {
	case ProfType_Append:
		return "Append"
	case ProfType_SyncTo:
		return "SyncTo"
	case ProfType_AAR:
		return "AAR"
	case ProfType_FindNext:
		return "FindNext"
	default:
		panic("unreachable")
	}
}

type profInfo struct {
	profType uint8
	duration time.Duration
	message  string

	// temp vars
	startTime time.Time
}

func ProfStart(profType uint8) *profInfo {
	return &profInfo{
		profType:  profType,
		startTime: time.Now(),
	}
}

func (p *profInfo) ProfStop(message string) {
	p.duration = time.Since(p.startTime)
	p.message = message
}

type profInfoJson struct {
	ProfType string `json:"type"`
	Duration int64  `json:"duration"`
	Message  string `json:"message"`
}

func (p *profInfo) String() string {
	pInfoJson := profInfoJson{
		ProfType: ProfTypeToString(p.profType),
		Duration: p.duration.Microseconds(),
		Message:  p.message,
	}
	result, err := json.Marshal(pInfoJson)
	if err != nil {
		return fmt.Sprintf("json marshal %+v error %v", pInfoJson, err)
	} else {
		return string(result)
	}
}
