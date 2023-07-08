package types

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// utils
func SeparateTags(tags []Tag) ([]uint8, []uint64) {
	streamTypes := make([]uint8, len(tags))
	streamIds := make([]uint64, len(tags))
	for i, tag := range tags {
		streamTypes[i] = tag.StreamType
		streamIds[i] = tag.StreamId
	}
	return streamTypes, streamIds
}
func CombineTags(streamTypes []uint8, streamIds []uint64) []Tag {
	if len(streamTypes) != len(streamIds) {
		panic(fmt.Sprintf("inconsistent types and streamids: %v, %v", len(streamTypes), len(streamIds)))
	}
	tags := make([]Tag, len(streamTypes))
	for i := 0; i < len(streamTypes); i++ {
		tags[i].StreamType = streamTypes[i]
		tags[i].StreamId = streamIds[i]
	}
	return tags
}

// wrap metadatas to original data
// we rely on this to be compatible with boki API
type LogDataWrapper interface {
	WithDeps(deps []uint64) LogDataWrapper
	WithStreamTypes(StreamTypes []uint8) LogDataWrapper
	Build(data []byte) []byte
}

type logDataWrapperImpl struct {
	Deps        []uint64 `json:"dep"`
	StreamTypes []uint8  `json:"sids"`
}

func NewLogDataWrapper() LogDataWrapper {
	wrapper := &logDataWrapperImpl{
		Deps:        make([]uint64, 0, 10),
		StreamTypes: make([]uint8, 0, 10),
	}
	return wrapper
}

func (c *logDataWrapperImpl) WithDeps(deps []uint64) LogDataWrapper {
	c.Deps = append(c.Deps, deps...)
	return c
}

func (c *logDataWrapperImpl) WithStreamTypes(streamTypes []uint8) LogDataWrapper {
	c.StreamTypes = append(c.StreamTypes, streamTypes...)
	return c
}

// wrap original data with metas
func (c *logDataWrapperImpl) Build(data []byte) []byte {
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

func UnwrapData(rawData []byte) (*logDataWrapperImpl, []byte, error) {
	var wrapperData DataWrapper
	err := json.Unmarshal(rawData, &wrapperData)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Failed to unmarshal json to DataWrapper: %v", rawData)
	} else {
		var meta logDataWrapperImpl
		err = json.Unmarshal(wrapperData.Meta, &meta)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "Failed to unmarshal json to logDataWrapperImpl: %v", wrapperData.Meta)
		}
		return &meta, wrapperData.Data, nil
	}
}
