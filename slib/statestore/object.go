package statestore

import (
	"fmt"
	"log"
	"os"
	"time"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/protocol"

	gabs "github.com/Jeffail/gabs/v2"
)

type ObjectView struct {
	name       string
	nextSeqNum uint64
	contents   *gabs.Container
}

type ObjectRef struct {
	env      *envImpl
	name     string
	nameHash uint64
	view     *ObjectView
	multiCtx *multiContext
	txnCtx   *txnContext
}

func (env *envImpl) Object(name string) *ObjectRef {
	if env.txnCtx != nil && !env.txnCtx.active {
		panic("Cannot create object within inactive transaction!")
	}
	env.objsMu.Lock()
	defer env.objsMu.Unlock()
	if obj, exists := env.objs[name]; exists {
		return obj
	} else {
		obj := &ObjectRef{
			env:      env,
			name:     name,
			nameHash: common.NameHash(name),
			view:     nil,
			multiCtx: nil,
			txnCtx:   env.txnCtx,
		}
		env.objs[name] = obj
		return obj
	}
}

func (objView *ObjectView) Clone() *ObjectView {
	return &ObjectView{
		name:       objView.name,
		nextSeqNum: objView.nextSeqNum,
		contents:   gabs.Wrap(common.DeepCopy(objView.contents.Data())),
	}
}

func (obj *ObjectRef) ensureView() error {
	if obj.view == nil {
		tailSeqNum := protocol.MaxLogSeqnum
		txnStr := "Normal"
		if obj.txnCtx != nil {
			tailSeqNum = obj.txnCtx.id
			txnStr = "Txn"
		}
		syncToStart := time.Now()
		inspector, err := obj.syncTo(tailSeqNum)
		syncToElapsed := time.Since(syncToStart).Microseconds()
		if flagProf := obj.env.faasCtx.Value("PROF"); flagProf != nil {
			defer log.Printf("[PROF] Get%v=nil read=%v count r/a=(%v %v) txn_r/a=(%v %v)",
				txnStr, syncToElapsed, inspector.readCount, inspector.applyCount,
				inspector.txnReadCount, inspector.txnApplyCount)
			if len(inspector.ops) > 0 {
				defer fmt.Fprintf(os.Stdout, "[PROF] Get%v %+v\n", txnStr, inspector.ops)
			}
		}
		return err
	} else {
		return nil
	}
}

// DEBUG: not using
// func (obj *ObjectRef) Sync() error {
// 	if obj.txnCtx != nil {
// 		panic("Cannot Sync() objects within a transaction context")
// 	}
// 	return obj.syncTo(protocol.MaxLogSeqnum)
// }

func (obj *ObjectRef) Get(path string) (Value, error) {
	if obj.multiCtx != nil {
		panic("Cannot call Get within multi context")
	}
	if obj.txnCtx != nil && !obj.txnCtx.active {
		panic("Cannot call Get within inactive transaction!")
	}
	if err := obj.ensureView(); err != nil {
		return NullValue(), err
	}
	resolved := obj.view.contents.Path(path)
	if resolved == nil {
		return NullValue(), newPathNotExistError(path)
	}
	return valueFromInterface(resolved.Data()), nil
}
