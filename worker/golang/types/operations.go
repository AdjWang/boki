package types

import (
	"encoding/json"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// used inside CondAppend to wrap to original data
type CondDataWrapper interface {
	WithDeps(deps []uint64) CondDataWrapper
	WithTagMetas(tagBuildMetas []TagMeta) CondDataWrapper
	Build(data []byte) []byte
}

type condImpl struct {
	Deps          []uint64  `json:"dep"`
	TagBuildMetas []TagMeta `json:"tagMetas"`
}

func NewCond() CondDataWrapper {
	cond := &condImpl{
		Deps:          make([]uint64, 0, 10),
		TagBuildMetas: make([]TagMeta, 0, 10),
	}
	return cond
}

func (c *condImpl) WithDeps(deps []uint64) CondDataWrapper {
	c.Deps = append(c.Deps, deps...)
	return c
}

func (c *condImpl) WithTagMetas(tagBuildMetas []TagMeta) CondDataWrapper {
	c.TagBuildMetas = append(c.TagBuildMetas, tagBuildMetas...)
	return c
}

func (c *condImpl) Build(data []byte) []byte {
	metaJson, err := json.Marshal(c)
	check(err)
	newDataStruct := DataWrapper{
		Meta: metaJson,
		Data: data,
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
		var meta condImpl
		err = json.Unmarshal(wrapperData.Meta, &meta)
		if err != nil {
			return nil, nil, err
		}
		return &meta, wrapperData.Data, nil
	}
}
