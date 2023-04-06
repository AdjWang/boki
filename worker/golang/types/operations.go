package types

import (
	"encoding/json"
	"fmt"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type readArg struct {
	Tag    uint64     `json:"tag"`
	FuMeta FutureMeta `json:"fuMeta"`
}

// used by CondAppend in user code
type CondHandle interface {
	AddDep(future Future[uint64])
	Read(tag uint64, future Future[uint64])
}

// used inside CondAppend to wrap to original data
type CondDataWrapper interface {
	WrapData(originalData []byte) []byte
}

type condImpl struct {
	Deps []FutureMeta
	Ops  []Op
}

func NewCond() (CondHandle, CondDataWrapper) {
	cond := &condImpl{
		Deps: make([]FutureMeta, 0, 10),
		Ops:  make([]Op, 0, 10),
	}
	return cond, cond
}

func (c *condImpl) WrapData(originalData []byte) []byte {
	newDataStruct := WrapperData{
		Deps: c.Deps,
		Cond: c.Ops,
		Data: originalData,
	}
	rawData, err := json.Marshal(newDataStruct)
	check(err)
	return rawData
}

func UnwrapData(rawData []byte) (*condImpl, []byte, error) {
	var wrapperData WrapperData
	err := json.Unmarshal(rawData, &wrapperData)
	if err != nil {
		return nil, nil, err
	} else {
		return &condImpl{
				Deps: wrapperData.Deps,
				Ops:  wrapperData.Cond,
			},
			wrapperData.Data,
			nil
	}
}

func (c *condImpl) AddDep(future Future[uint64]) {
	c.Deps = append(c.Deps, future.GetMeta())
}

func (c *condImpl) Read(tag uint64, future Future[uint64]) {
	arg := readArg{
		Tag:    tag,
		FuMeta: future.GetMeta(),
	}
	rawArg, err := json.Marshal(arg)
	check(err)
	c.Ops = append(c.Ops,
		Op{
			Method: "Read",
			Args:   rawArg,
		})
}

// used by the state machine of a stream
type CondExecutor struct{}

func (c *CondExecutor) Read(tag uint64, futureMeta FutureMeta) (bool, error) {
	// TODO
	fmt.Printf("tag=%v, futureMeta=%v\n", tag, futureMeta)
	return true, nil
}

type Op struct {
	Method string
	Args   []byte
}

func (op *Op) Serialize() ([]byte, error) {
	return json.Marshal(op)
}
func DeserializeOp(data []byte) (*Op, error) {
	var op Op
	err := json.Unmarshal(data, &op)
	return &op, err
}

func CheckOp(op Op) bool {
	// fmt.Printf("%+v\n", op)
	executor := &CondExecutor{}
	switch op.Method {
	case "Read":
		var arg readArg
		check(json.Unmarshal([]byte(op.Args), &arg))
		ok, err := executor.Read(arg.Tag, arg.FuMeta)
		check(err)
		return ok
	default:
		panic("unreachable")
	}
}
