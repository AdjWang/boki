package objectview

import (
	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

type ViewVersion struct {
	LocalId uint64
	SeqNum  uint64
}

func IsViewVersionInvalid(version ViewVersion) bool {
	return version.LocalId == protocol.InvalidLogLocalId &&
		version.SeqNum == protocol.InvalidLogSeqNum
}

// Implemented by user to materialize a log stream to object view.
// The materizalized object view is stored in the engine node as auxdata to
// serve fast random read.
type ObjectView interface {
	// stream tag indicates which object to build
	GetTag() uint64
	// convert log operation data to auxiliary view data
	UpdateView(view interface{}, logEntry *types.LogEntryWithMeta) ([]uint64, interface{})
	EncodeView(view interface{}) ([]byte, error)
	DecodeView(rawViewData []byte) (interface{}, error)
}
