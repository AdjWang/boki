package asyncstatestore

import (
	"context"
	"encoding/json"
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
	if seqNum, err := env.appendTxnBeginLog(); err == nil {
		env.txnCtx = &txnContext{
			active:   true,
			readonly: false,
			id:       seqNum,
			ops:      make([]*WriteOp, 0, 4),
		}
		return env, nil
	} else {
		return nil, err
	}
}

func CreateReadOnlyTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	if tail, err := faasEnv.AsyncSharedLogCheckTail(ctx, 0 /* tag */); err == nil {
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
	tags := []types.Tag{
		{StreamType: common.FsmType_TxnMetaLog, StreamId: common.TxnMetaLogTag},
		{StreamType: common.FsmType_TxnHistoryLog, StreamId: txnHistoryLogTag(ctx.id)},
	}
	if future, err := env.faasEnv.AsyncSharedLogAppendWithDeps(env.faasCtx, tags, common.CompressData(encoded), []uint64{ctx.id}); err == nil {
		// ensure durability
		return future.Await(common.AsyncWaitTimeout)
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
	tags := []types.Tag{
		{StreamType: common.FsmType_TxnMetaLog, StreamId: common.TxnMetaLogTag},
		{StreamType: common.FsmType_TxnHistoryLog, StreamId: txnHistoryLogTag(ctx.id)},
	}
	for _, op := range ctx.ops {
		tags = append(tags, types.Tag{
			StreamType: common.FsmType_ObjectLog,
			StreamId:   objectLogTag(common.NameHash(op.ObjName)),
		})
	}
	future, err := env.faasEnv.AsyncSharedLogAppendWithDeps(env.faasCtx, tags, common.CompressData(encoded), []uint64{ctx.id})
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	// TODO: optimize
	seqNum, err := future.GetResult(60 * time.Second)
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	// log.Printf("[DEBUG] Append TxnCommit log: seqNum=%#016x, op_size=%d", seqNum, len(ctx.ops))
	objectLog.fillWriteSet()
	objectLog.seqNum = seqNum
	// Check for status
	inspector := syncToInspector{
		readCount:  0,
		applyCount: 0,

		txnReadCount:  0,
		txnApplyCount: 0,
		ops:           make([]opsEntry, 0, 20),
	}
	if committed, err := objectLog.checkTxnCommitResult(env, &inspector, 0 /*depth*/); err != nil {
		return false, err
	} else {
		return committed, nil
	}
}

func (ctx *txnContext) appendOp(op *WriteOp) {
	ctx.ops = append(ctx.ops, op)
}
