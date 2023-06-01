package common

import "time"

const LogTagReserveBits = 3

const TxnMetaLogTag = 1
const ObjectLogTagLowBits = 2
const TxnHistoryLogTagLowBits = 3
const QueueLogTagLowBits = 4
const QueuePushLogTagLowBits = 5

const (
	FsmType_TxnMetaLog = iota
	FsmType_ObjectLog
	FsmType_TxnHistoryLog

	FsmType_QueueLog
	FsmType_QueuePushLog
)

const AsyncWaitTimeout = 60 * time.Second
const TagKeyBase = 36
