package asyncstatestore

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type Env interface {
	Object(name string) *ObjectRef
	WithTxn(txnLogic func(txnEnv Env) error) Env
	TxnCommit() (bool /* committed */, string /*msg*/, error)
	TxnAbort() error
}

type envImpl struct {
	faasCtx  context.Context
	faasEnv  types.Environment
	objs     map[string]*ObjectRef
	txnCtx   *txnContext
	txnLogic func(txnEnv Env) error
}

func CreateEnv(ctx context.Context, faasEnv types.Environment) Env {
	return &envImpl{
		faasCtx: ctx,
		faasEnv: faasEnv,
		objs:    make(map[string]*ObjectRef),
		txnCtx:  nil,
	}
}

func (e *envImpl) WithTxn(txnLogic func(txnEnv Env) error) Env {
	e.txnLogic = txnLogic
	return e
}
