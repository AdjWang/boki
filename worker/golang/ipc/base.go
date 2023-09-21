package ipc

import (
	"fmt"
)

var rootPathForIpc string

const fileCreatMode = 0664

func SetRootPathForIpc(path string) {
	rootPathForIpc = path
}

func GetEngineUnixSocketPath() string {
	return fmt.Sprintf("%s/engine.sock", rootPathForIpc)
}

func GetFuncWorkerInputFifoName(clientId uint16) string {
	return fmt.Sprintf("worker_%d_input", clientId)
}

func GetFuncWorkerOutputFifoName(clientId uint16) string {
	return fmt.Sprintf("worker_%d_output", clientId)
}

func GetFuncCallInputShmName(fullCallId uint64) string {
	return fmt.Sprintf("%d.i", fullCallId)
}

func GetFuncCallOutputShmName(fullCallId uint64) string {
	return fmt.Sprintf("%d.o", fullCallId)
}

func GetFuncCallOutputFifoName(fullCallId uint64) string {
	return fmt.Sprintf("%d.o", fullCallId)
}

func GetSharedLogRespShmName(fullCallId uint64, messageType uint16, opId uint64) string {
	// full_call:message_type:client_op_id
	return fmt.Sprintf("%d_%d_%d.o", fullCallId, messageType, opId)
}
