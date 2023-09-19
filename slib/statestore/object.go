package statestore

import (
	"log"
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
	if obj.env.consistency == common.SEQUENTIAL_CONSISTENCY {
		if obj.view == nil {
			tailSeqNum := protocol.MaxLogSeqnum
			if obj.txnCtx != nil {
				tailSeqNum = obj.txnCtx.id
			}
			return obj.syncTo(tailSeqNum)
		} else {
			return nil
		}
	} else if obj.env.consistency == common.STRONG_CONSISTENCY {
		if obj.txnCtx != nil {
			if obj.view == nil {
				tailSeqNum := obj.txnCtx.id
				return obj.syncTo(tailSeqNum)
			} else {
				return nil
			}
		} else {
			var seqNum uint64
			var err error
			var ts_start time.Time
			if common.SW_STAT == common.SWITCH_ON {
				ts_start = time.Now()
			}
			if obj.env.txnCheckMethos == common.TXN_CHECK_SEQUENCER {
				seqNum, err = obj.env.faasEnv.SharedLogLinearizableCheckTail(obj.env.faasCtx, 0 /*tag*/)
			} else if obj.env.txnCheckMethos == common.TXN_CHECK_APPEND {
				seqNum, err = obj.appendNormalOpSyncLog()
			} else {
				panic("unreachable")
			}
			if common.SW_STAT == common.SWITCH_ON {
				defer log.Printf("[STAT] strong non-txn ensureView delay=%dus",
					time.Since(ts_start).Microseconds())
			}
			if err != nil {
				return err
			}
			return obj.syncTo(seqNum)
		}
	} else {
		panic("unreachable")
	}
}

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
