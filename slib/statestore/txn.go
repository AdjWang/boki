package statestore

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/types"
)

type txnContext struct {
	active   bool
	readonly bool
	id       uint64 // TxnId is the seqnum of TxnBegin log
	ops      []*WriteOp
}

func CreateTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	var seqNum uint64
	var err error
	var ts_start time.Time
	if common.SW_STAT == common.SWITCH_ON {
		ts_start = time.Now()
	}
	if env.txnCheckMethos == common.TXN_CHECK_SEQUENCER {
		seqNum, err = env.faasEnv.SharedLogLinearizableCheckTail(env.faasCtx, 0 /*tag*/)
	} else if env.txnCheckMethos == common.TXN_CHECK_APPEND {
		seqNum, err = env.appendTxnBeginLog()
	} else {
		panic("unreachable")
	}
	if common.SW_STAT == common.SWITCH_ON {
		defer log.Printf("[STAT] strong rw-txn update view delay=%dus",
			time.Since(ts_start).Microseconds())
	}
	if err != nil {
		return nil, err
	}
	env.txnCtx = &txnContext{
		active:   true,
		readonly: false,
		id:       seqNum,
		ops:      make([]*WriteOp, 0, 4),
	}
	return env, nil
}

func CreateReadOnlyTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	if env.consistency == common.SEQUENTIAL_CONSISTENCY {
		var ts_start time.Time
		if common.SW_STAT == common.SWITCH_ON {
			ts_start = time.Now()
		}
		if tail, err := faasEnv.SharedLogCheckTail(ctx, 0 /* tag */); err == nil {
			if common.SW_STAT == common.SWITCH_ON {
				defer log.Printf("[STAT] sequential ro-txn update view delay=%dus",
					time.Since(ts_start).Microseconds())
			}
			seqNum := uint64(0)
			if tail != nil {
				seqNum = tail.SeqNum + 1
			}
			env.txnCtx = &txnContext{
				active:   true,
				readonly: true,
				id:       seqNum,
				ops:      nil,
			}
			return env, nil
		} else {
			return nil, err
		}
	} else if env.consistency == common.STRONG_CONSISTENCY {
		var seqNum uint64
		var err error
		var ts_start time.Time
		if common.SW_STAT == common.SWITCH_ON {
			ts_start = time.Now()
		}
		if env.txnCheckMethos == common.TXN_CHECK_SEQUENCER {
			seqNum, err = env.faasEnv.SharedLogLinearizableCheckTail(env.faasCtx, 0 /*tag*/)
		} else if env.txnCheckMethos == common.TXN_CHECK_APPEND {
			seqNum, err = env.appendTxnBeginLog()
		} else {
			panic("unreachable")
		}
		if common.SW_STAT == common.SWITCH_ON {
			defer log.Printf("[STAT] strong ro-txn update view delay=%dus",
				time.Since(ts_start).Microseconds())
		}
		if err == nil {
			env.txnCtx = &txnContext{
				active:   true,
				readonly: true,
				id:       seqNum,
				ops:      nil,
			}
			return env, nil
		} else {
			return nil, err
		}
	} else {
		panic("unreachable")
	}
}

func (env *envImpl) TxnAbort() error {
	if env.txnCtx == nil {
		panic("Not in a transaction env")
	}
	if env.txnCtx.readonly {
		panic("Read-only transaction")
	}
	ctx := env.txnCtx
	env.txnCtx = nil
	ctx.active = false
	logEntry := ObjectLogEntry{
		LogType: LOG_TxnAbort,
		TxnId:   ctx.id,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []uint64{common.TxnIdLogTag, txnHistoryLogTag(ctx.id)}
	if _, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, common.CompressData(encoded)); err == nil {
		return nil
	} else {
		return newRuntimeError(err.Error())
	}
}

func (env *envImpl) TxnCommit() (bool /* committed */, error) {
	if env.txnCtx == nil {
		panic("Not in a transaction env")
	}
	if env.txnCtx.readonly {
		panic("Read-only transaction")
	}
	ctx := env.txnCtx
	env.txnCtx = nil
	ctx.active = false
	// Append commit log
	objectLog := &ObjectLogEntry{
		LogType: LOG_TxnCommit,
		Ops:     ctx.ops,
		TxnId:   ctx.id,
	}
	encoded, err := json.Marshal(objectLog)
	if err != nil {
		panic(err)
	}
	tags := []uint64{common.TxnIdLogTag, txnHistoryLogTag(ctx.id)}
	for _, op := range ctx.ops {
		tags = append(tags, objectLogTag(common.NameHash(op.ObjName)))
	}
	seqNum, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, common.CompressData(encoded))
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	// log.Printf("[DEBUG] Append TxnCommit log: seqNum=%#016x, op_size=%d", seqNum, len(ctx.ops))
	objectLog.fillWriteSet()
	objectLog.seqNum = seqNum
	// Check for status
	if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
		return false, err
	} else {
		return committed, nil
	}
}

func (ctx *txnContext) appendOp(op *WriteOp) {
	ctx.ops = append(ctx.ops, op)
}
