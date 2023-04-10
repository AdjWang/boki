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
	FuMeta FutureMeta `json:"fuMeta"`
}

// used by CondAppend in user code
type CondHandle interface {
	AddDep(futureMeta FutureMeta)
	Read(futureMeta FutureMeta)
}

// used inside CondAppend to wrap to original data
// TODO: restruct with builder pattern
type CondDataWrapper interface {
	WrapData(tagBuildMeta []TagMeta, originalData []byte) []byte
}

type condImpl struct {
	Deps         []FutureMeta
	Ops          []Op
	TagBuildMeta []TagMeta
}

func NewCond() (CondHandle, CondDataWrapper) {
	cond := &condImpl{
		Deps: make([]FutureMeta, 0, 10),
		Ops:  make([]Op, 0, 10),
	}
	return cond, cond
}

func (c *condImpl) WrapData(tagBuildMeta []TagMeta, originalData []byte) []byte {
	newDataStruct := DataWrapper{
		Deps:         c.Deps,
		Cond:         c.Ops,
		TagBuildMeta: tagBuildMeta,
		Data:         originalData,
	}
	rawData, err := json.Marshal(newDataStruct)
	check(err)
	return rawData
}

func UnwrapData(rawData []byte) (*condImpl, []byte, error) {
	var wrapperData DataWrapper
	err := json.Unmarshal(rawData, &wrapperData)
	if err != nil {
		return nil, nil, err
	} else {
		return &condImpl{
				Deps:         wrapperData.Deps,
				Ops:          wrapperData.Cond,
				TagBuildMeta: wrapperData.TagBuildMeta,
			},
			wrapperData.Data,
			nil
	}
}

func (c *condImpl) AddDep(futureMeta FutureMeta) {
	c.Deps = append(c.Deps, futureMeta)
}

func (c *condImpl) Read(futureMeta FutureMeta) {
	arg := readArg{
		FuMeta: futureMeta,
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
// Verify touches bussiness logic, so delegate each operation to the
// user code, pass the *operand* to the user and get the result
// Code here only implements the *operators*.
type CondExecutor struct{}

func (c *CondExecutor) Read(futureMeta FutureMeta) (bool, error) {
	// TODO
	fmt.Printf("futureMeta=%v\n", futureMeta)
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
		ok, err := executor.Read(arg.FuMeta)
		check(err)
		return ok
	default:
		panic("unreachable")
	}
}

func CheckOps(ops []Op) bool {
	for _, op := range ops {
		if !CheckOp(op) {
			return false
		}
	}
	return true
}
