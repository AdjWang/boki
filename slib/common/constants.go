package common

import "time"

const TxnIdLogTag = 1
const ObjectIdLogTag = 2

const LogTagReserveBits = 3
const ObjectLogTagLowBits = 2
const TxnHistoryLogTagLowBits = 3
const QueueLogTagLowBits = 4
const QueuePushLogTagLowBits = 5

const (
	FsmType_TxnIdLog = iota
	FsmType_ObjectIdLog
	FsmType_ObjectLog
	FsmType_TxnHistoryLog

	FsmType_QueueLog
	FsmType_QueuePushLog
)

const AsyncWaitTimeout = 60 * time.Second
const TagKeyBase = 36

// set at linking by 'go build -ldflags="-X cs.utexas.edu/zjia/faas/slib/common.CONSISTENCY"'
var CONSISTENCY = "SEQUENTIAL" // or STRONG
