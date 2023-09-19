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

const (
	// for single object and readonly txn
	// r/w txn is always strict serializable
	SEQUENTIAL_CONSISTENCY = "SEQUENTIAL"
	// for single object -> linearizable
	// for txn -> strict serializable
	STRONG_CONSISTENCY = "STRONG"

	TXN_CHECK_SEQUENCER = "CHECKSEQ"
	TXN_CHECK_APPEND    = "APPEND"

	SWITCH_ON  = "ON"
	SWITCH_OFF = "OFF"
)

// set at linking by 'go build -ldflags="-X cs.utexas.edu/zjia/faas/slib/common.CONSISTENCY"'
var CONSISTENCY = "SEQUENTIAL"    // or STRONG
var TXN_CHECK_METHOD = "CHECKSEQ" // or APPEND
var SW_STAT = "ON"                // or OFF
