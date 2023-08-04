package statestore

import (
	"context"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
)

type Env interface {
	Object(name string) *ObjectRef
	TxnCommit() (bool /* committed */, error)
	TxnAbort() error
}

type envImpl struct {
	faasCtx context.Context
	faasEnv types.Environment
	objsMu  sync.Mutex
	objs    map[string]*ObjectRef
	txnCtx  *txnContext
}

func CreateEnv(ctx context.Context, faasEnv types.Environment) Env {
	return &envImpl{
		faasCtx: ctx,
		faasEnv: faasEnv,
		objs:    make(map[string]*ObjectRef),
		txnCtx:  nil,
	}
}
