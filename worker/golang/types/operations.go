package types

import (
	"encoding/json"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// used by CondAppend in user code
type CondHandle interface {
	AddDep(futureMeta FutureMeta)
	AddResolver(resolver uint8)
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

func (c *condImpl) AddResolver(resolver uint8) {
	c.Ops = append(c.Ops,
		Op{
			Resolver: resolver,
		})
}

type Op struct {
	Resolver uint8 `json:"resolver"`
}

func (op *Op) Serialize() ([]byte, error) {
	return json.Marshal(op)
}
func DeserializeOp(data []byte) (*Op, error) {
	var op Op
	err := json.Unmarshal(data, &op)
	return &op, err
}
