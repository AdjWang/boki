package asyncstatestore

import (
	"context"

	"cs.utexas.edu/zjia/faas/slib/common"
	"cs.utexas.edu/zjia/faas/types"
)

const (
	// for single object and readonly txn
	// r/w txn is always strict serializable
	SEQUENTIAL_CONSISTENCY = "SEQUENTIAL"
	// for single object -> linearizable
	// for txn -> strict serializable
	STRONG_CONSISTENCY = "STRONG"
)

type Env interface {
	Object(name string) *ObjectRef
	TxnCommit() (bool /* committed */, error)
	TxnAbort() error
}

type envImpl struct {
	faasCtx     context.Context
	faasEnv     types.Environment
	objs        map[string]*ObjectRef
	txnCtx      *txnContext
	consistency string
}

func CreateEnv(ctx context.Context, faasEnv types.Environment) Env {
	return &envImpl{
		faasCtx:     ctx,
		faasEnv:     faasEnv,
		objs:        make(map[string]*ObjectRef),
		txnCtx:      nil,
		consistency: common.CONSISTENCY,
	}
}
