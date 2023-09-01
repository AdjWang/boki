package asyncstatestore

import (
	"context"
	"encoding/json"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/types"
)

type txnContext struct {
	active   bool
	readonly bool
	idFuture types.Future[uint64]
	ops      []*WriteOp
}

func CreateTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	if future, err := env.appendTxnBeginLog(); err == nil {
		env.txnCtx = &txnContext{
			active:   true,
			readonly: false,
			idFuture: future,
			ops:      make([]*WriteOp, 0, 4),
		}
		return env, nil
	} else {
		return nil, err
	}
}

func CreateReadOnlyTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	if env.consistency == SEQUENTIAL_CONSISTENCY {
		if tail, err := faasEnv.AsyncSharedLogCheckTail(ctx, 0 /* tag */); err == nil {
			seqNum := uint64(0)
			if tail != nil {
				seqNum = tail.SeqNum + 1
			}
			env.txnCtx = &txnContext{
				active:   true,
				readonly: true,
				idFuture: types.NewDummyFuture(tail.LocalId, tail.SeqNum,
					func() (uint64, error) { return seqNum, nil }),
				ops: nil,
			}
			return env, nil
		} else {
			return nil, err
		}
	} else if env.consistency == STRONG_CONSISTENCY {
		if future, err := env.appendTxnBeginLog(); err == nil {
			env.txnCtx = &txnContext{
				active:   true,
				readonly: true,
				idFuture: future,
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
	txnId, err := ctx.idFuture.GetResult(common.AsyncWaitTimeout)
	if err != nil {
		return newRuntimeError(err.Error())
	}
	logEntry := ObjectLogEntry{
		LogType: LOG_TxnAbort,
		TxnId:   txnId,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []types.Tag{
		{StreamType: common.FsmType_TxnIdLog, StreamId: common.TxnIdLogTag},
		{StreamType: common.FsmType_TxnHistoryLog, StreamId: txnHistoryLogTag(txnId)},
	}
	if future, err := env.faasEnv.AsyncSharedLogAppendWithDeps(env.faasCtx, tags, common.CompressData2(encoded), []uint64{ctx.idFuture.GetLocalId()}); err == nil {
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
	txnId, err := ctx.idFuture.GetResult(common.AsyncWaitTimeout)
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	objectLog := &ObjectLogEntry{
		LogType: LOG_TxnCommit,
		Ops:     ctx.ops,
		TxnId:   txnId,
	}
	encoded, err := json.Marshal(objectLog)
	if err != nil {
		panic(err)
	}
	tags := []types.Tag{
		{StreamType: common.FsmType_TxnIdLog, StreamId: common.TxnIdLogTag},
		{StreamType: common.FsmType_TxnHistoryLog, StreamId: txnHistoryLogTag(txnId)},
	}
	for _, op := range ctx.ops {
		tags = append(tags, types.Tag{
			StreamType: common.FsmType_ObjectLog,
			StreamId:   objectLogTag(common.NameHash(op.ObjName)),
		})
	}
	future, err := env.faasEnv.AsyncSharedLogAppendWithDeps(env.faasCtx, tags, common.CompressData2(encoded), []uint64{ctx.idFuture.GetLocalId()})
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	// log.Printf("[DEBUG] Append TxnCommit log: seqNum=%#016x, op_size=%d", seqNum, len(ctx.ops))
	objectLog.fillWriteSet()
	objectLog.localId = future.GetLocalId()
	objectLog.seqNum = protocol.InvalidLogSeqNum
	// Check for status
	if committed, err := objectLog.checkTxnCommitResult(env, func() (uint64, error) {
		return future.GetResult(common.AsyncWaitTimeout)
	}); err != nil {
		return false, err
	} else {
		return committed, nil
	}
}

func (ctx *txnContext) appendOp(op *WriteOp) {
	ctx.ops = append(ctx.ops, op)
}
