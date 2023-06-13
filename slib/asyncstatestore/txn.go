package asyncstatestore

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/types"
)

type txnContext struct {
	active      bool
	readonly    bool
	id          uint64
	txnIdFuture types.Future[uint64] // TxnId is the seqnum of TxnBegin log
	ops         []*WriteOp
}

func CreateTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	trySeqNum := uint64(0)
	if tail, err := faasEnv.AsyncSharedLogCheckTail(ctx, 0 /* tag */); err == nil {
		if tail != nil {
			trySeqNum = tail.SeqNum + 1
		}
	}
	if future, err := env.appendTxnBeginLog(); err == nil {
		env.txnCtx = &txnContext{
			active:      true,
			readonly:    false,
			id:          trySeqNum,
			txnIdFuture: future,
			ops:         make([]*WriteOp, 0, 4),
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
	tags := []uint64{common.TxnMetaLogTag, txnHistoryLogTag(ctx.id)}
	tagMetas := []types.TagMeta{
		{FsmType: common.FsmType_TxnMetaLog, TagKeys: []string{strconv.FormatUint(common.TxnMetaLogTag, common.TagKeyBase)}},
		{FsmType: common.FsmType_TxnHistoryLog, TagKeys: []string{strconv.FormatUint(ctx.id, common.TagKeyBase)}},
	}
	if future, err := env.faasEnv.AsyncSharedLogCondAppend(env.faasCtx, tags, tagMetas, common.CompressData(encoded), []uint64{ctx.id}); err == nil {
		// ensure durability
		return future.Await(common.AsyncWaitTimeout)
	} else {
		return newRuntimeError(err.Error())
	}
}

func (env *envImpl) TxnCommit() (bool /* committed */, string /*msg*/, error) {
	if env.txnCtx == nil {
		panic("Not in a transaction env")
	}
	if env.txnCtx.readonly {
		panic("Read-only transaction")
	}
	if err := env.txnLogic(env); err != nil {
		return false, err.Error(), env.TxnAbort()
	}

	ctx := env.txnCtx
	// env.txnCtx = nil
	// ctx.active = false
	// Append commit log
	objectLog := &ObjectLogEntry{
		LogType:         LOG_TxnCommit,
		Ops:             ctx.ops,
		AggressiveTxnId: ctx.id,
		TxnId:           ctx.txnIdFuture.GetLocalId(),
	}
	encoded, err := json.Marshal(objectLog)
	if err != nil {
		panic(err)
	}
	txnId := objectLog.TxnId
	tags := []uint64{common.TxnMetaLogTag, txnHistoryLogTag(txnId)}
	tagMetas := []types.TagMeta{
		{FsmType: common.FsmType_TxnMetaLog, TagKeys: []string{strconv.FormatUint(common.TxnMetaLogTag, common.TagKeyBase)}},
		{FsmType: common.FsmType_TxnHistoryLog, TagKeys: []string{strconv.FormatUint(txnId, common.TagKeyBase)}},
	}
	for _, op := range ctx.ops {
		tags = append(tags, objectLogTag(common.NameHash(op.ObjName)))
		tagMetas = append(tagMetas, types.TagMeta{FsmType: common.FsmType_ObjectLog, TagKeys: []string{op.ObjName}})
	}
	future, err := env.faasEnv.AsyncSharedLogCondAppend(env.faasCtx, tags, tagMetas, common.CompressData(encoded), []uint64{txnId})
	if err != nil {
		return false, "", newRuntimeError(err.Error())
	}

	txnId, err = ctx.txnIdFuture.GetResult()
	if err != nil {
		return false, "", err
	}
	if conflict, err := objectLog.checkTxnViewConfliction(env, txnId); err != nil {
		return false, "", err
	} else if conflict {
		log.Println("conflict")
		env.txnCtx.id = txnId
		if err := env.txnLogic(env); err != nil {
			return false, err.Error(), env.TxnAbort()
		}
	}
	log.Println("conf-handled")
	ctx.active = false
	seqNum, err := future.GetResult()
	if err != nil {
		return false, "", newRuntimeError(err.Error())
	}
	// log.Printf("[DEBUG] Append TxnCommit log: seqNum=%#016x, op_size=%d", seqNum, len(ctx.ops))
	objectLog.fillWriteSet()
	objectLog.seqNum = seqNum
	objectLog.TxnId = txnId
	// Check for status
	if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
		return false, "", err
	} else {
		return committed, "", nil
	}
}

func (ctx *txnContext) appendOp(op *WriteOp) {
	ctx.ops = append(ctx.ops, op)
}
