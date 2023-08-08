package asyncstatestore

import (
	"fmt"
	"reflect"

	"cs.utexas.edu/zjia/faas/slib/common"
	"cs.utexas.edu/zjia/faas/types"

	"cs.utexas.edu/zjia/faas/protocol"

	gabs "github.com/Jeffail/gabs/v2"
)

type ObjectView struct {
	name       string
	nextSeqNum uint64
	contents   *gabs.Container
}

func (view ObjectView) Equal(other ObjectView) bool {
	if view.name != other.name {
		return false
	}
	if !reflect.DeepEqual(view.contents.Data(), other.contents.Data()) {
		return false
	}
	return true
}

func (view *ObjectView) String() string {
	if view == nil {
		return "nil"
	}
	return fmt.Sprintf("name=%v data=%+v", view.name, view.contents.Data())
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
	if obj.view == nil {
		tailLogIndex := types.LogEntryIndex{
			LocalId: protocol.InvalidLogLocalId,
			SeqNum:  protocol.MaxLogSeqnum,
		}
		if obj.txnCtx != nil {
			tailLogIndex = obj.txnCtx.idFuture.GetLogEntryIndex()
		}
		return obj.syncTo(tailLogIndex)
	} else {
		return nil
	}
}

func (obj *ObjectRef) Sync() error {
	if obj.txnCtx != nil {
		panic("Cannot Sync() objects within a transaction context")
	}
	return obj.syncTo(types.LogEntryIndex{
		LocalId: protocol.InvalidLogLocalId,
		SeqNum:  protocol.MaxLogSeqnum,
	})
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
