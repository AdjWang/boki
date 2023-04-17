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
	AddCond(resolver uint8)
}

// used inside CondAppend to wrap to original data
// TODO: restruct with builder pattern
type CondDataWrapper interface {
	WrapData(tagBuildMeta []TagMeta, originalData []byte) []byte
}

type condImpl struct {
	Deps         []FutureMeta
	CondMetas    []CondMeta
	TagBuildMeta []TagMeta
}

func NewCond() (CondHandle, CondDataWrapper) {
	cond := &condImpl{
		Deps:      make([]FutureMeta, 0, 10),
		CondMetas: make([]CondMeta, 0, 10),
	}
	return cond, cond
}

func (c *condImpl) WrapData(tagBuildMeta []TagMeta, originalData []byte) []byte {
	newDataStruct := DataWrapper{
		Deps:         c.Deps,
		Conds:        c.CondMetas,
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
				CondMetas:    wrapperData.Conds,
				TagBuildMeta: wrapperData.TagBuildMeta,
			},
			wrapperData.Data,
			nil
	}
}

func (c *condImpl) AddDep(futureMeta FutureMeta) {
	if futureMeta != InvalidFutureMeta {
		c.Deps = append(c.Deps, futureMeta)
	}
}

func (c *condImpl) AddCond(resolver uint8) {
	c.CondMetas = append(c.CondMetas, CondMeta{Resolver: resolver})
}

type CondMeta struct {
	Resolver uint8 `json:"resolver"`
}

// func (condMeta *CondMeta) Serialize() ([]byte, error) {
// 	return json.Marshal(condMeta)
// }
// func DeserializeCondMeta(data []byte) (*CondMeta, error) {
// 	var condMeta CondMeta
// 	err := json.Unmarshal(data, &condMeta)
// 	return &condMeta, err
// }
