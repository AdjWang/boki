package asyncstatestore

import (
	"encoding/json"
	"fmt"
	"log"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/protocol"

	gabs "github.com/Jeffail/gabs/v2"
)

type ObjectView struct {
	name       string
	nextSeqNum uint64
	contents   *gabs.Container
}

func NewEmptyObjectView(name string) *ObjectView {
	return &ObjectView{
		name:       name,
		nextSeqNum: 0,
		contents:   gabs.New(),
	}
}

func (view *ObjectView) String() string {
	return fmt.Sprintf("%+v", view.contents.Data())
}

func (view *ObjectView) ApplyLogEntry(objectLog *ObjectLogEntry) error {
	if objectLog.seqNum < view.nextSeqNum {
		log.Fatalf("[FATAL] LogSeqNum=%#016x, ViewNextSeqNum=%#016x", objectLog.seqNum, view.nextSeqNum)
	}
	view.nextSeqNum = objectLog.seqNum + 1
	for _, op := range objectLog.Ops {
		if op.ObjName == view.name {
			if _, err := view.applyWriteOp(op); err != nil {
				return err
			}
		}
	}
	return nil
}

func SerializeViewsCache(obj interface{}) ([]byte, error) {
	encoded, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	compressed := common.CompressData(encoded)
	if obj == nil {
		panic("unreachable")
	}
	return compressed, nil
}

func DeserializeViewsCache(data []byte) (map[string]interface{}, error) {
	reader, err := common.DecompressReader(data)
	if err != nil {
		return nil, err
	}
	var views map[string]interface{}
	err = json.NewDecoder(reader).Decode(&views)
	if err != nil {
		return nil, err
	}
	return views, nil
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
		tailSeqNum := protocol.MaxLogSeqnum
		if obj.txnCtx != nil {
			tailSeqNum = obj.txnCtx.id
		}
		return obj.syncTo(tailSeqNum)
	} else {
		return nil
	}
}

func (obj *ObjectRef) Sync() error {
	if obj.txnCtx != nil {
		panic("Cannot Sync() objects within a transaction context")
	}
	return obj.syncTo(protocol.MaxLogSeqnum)
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
