package ipc

// #include <stdlib.h>
// #include <index_data_c.h>
import "C"

import (
	"fmt"
	"log"
	"sync"
	"unsafe"

	"cs.utexas.edu/zjia/faas/protocol"
	"github.com/pkg/errors"
)

// DEBUG
func TestBinding() {
	varIn := 3
	varInOut := 2
	varOut := 0
	ret := int(C.int(C.test_func(C.uint(varIn), (*C.ulong)(unsafe.Pointer(&varInOut)), (*C.ulong)(unsafe.Pointer(&varOut)))))
	log.Printf("go binding test_func In=%d InOut=%d Out=%d ret=%d\n", varIn, varInOut, varOut, ret)
}

// Workflow
//
//  1. Function worker initialized
//     -> create empty new global singleton ViewManager
//
//  2. Function worker read unknown view_id
//     -> funcworker call SharedLogSetupView()
//     -> engine setup path: view_id/user_logspace/logspace_id[]
//     -> funcworker load path and SetupIndexData(s)
//     -> funcworker serve reads on view_id
//
//  3. Function later reads
//     -> known view_id: funcworker serve reads on view_id
//     -> unknown view_id: turn to 2
type IndexPool map[ /*logspaceId*/ uint32]*IndexData
type ViewManager struct {
	mu           sync.Mutex
	viewPool     map[ /*viewId*/ uint16]IndexPool
	userLogSpace uint32
}

var (
	ViewManagerErr_InvalidUser          = errors.New("Invalid User")
	ViewManagerErr_ConstructIndexFailed = errors.New("Construct IndexData failed")
	ViewManagerErr_IndexNotExist        = errors.New("Required index not exists")
)

func NewViewManager() *ViewManager {
	return &ViewManager{
		mu:           sync.Mutex{},
		userLogSpace: protocol.InvalidUserLogspace,
		viewPool:     make(map[uint16]IndexPool),
	}
}

func (view *ViewManager) SetUserLogSpace(userLogSpace uint32) {
	view.mu.Lock()
	defer view.mu.Unlock()
	view.userLogSpace = userLogSpace
}

func (view *ViewManager) LoadIndexData(logSpaceId uint32) (*IndexData, error) {
	view.mu.Lock()
	defer view.mu.Unlock()

	if view.userLogSpace == protocol.InvalidUserLogspace {
		return nil, ViewManagerErr_InvalidUser
	}

	if logSpaceId == 0 || logSpaceId == protocol.InvalidLogSpaceId {
		logSpaceId = uint32(C.GetLogSpaceIdentifier(C.uint(view.userLogSpace)))
	}
	ret := int(C.CheckIndexData(C.uint(logSpaceId), C.uint(view.userLogSpace)))
	if ret == -1 {
		return nil, errors.Wrapf(ViewManagerErr_IndexNotExist, "logSpaceId=%08X, userLogSpace=%08X", logSpaceId, view.userLogSpace)
	} else if ret != 0 {
		panic("unreachable")
	}

	viewId := protocol.GetViewId(logSpaceId)
	if _, ok := view.viewPool[viewId]; !ok {
		view.viewPool[viewId] = make(IndexPool)
	}
	if indexData, ok := view.viewPool[viewId][logSpaceId]; ok {
		return indexData, nil
	}
	indexData, err := constructIndexData(0 /*metalogProgress(DEPRECATED)*/, logSpaceId, view.userLogSpace)
	if err != nil {
		return nil, errors.Wrap(ViewManagerErr_ConstructIndexFailed, err.Error())
	}
	view.viewPool[viewId][logSpaceId] = indexData
	log.Printf("[DEBUG] LoadIndexData install viewId=%04X logSpaceId=%08X", viewId, logSpaceId)
	return indexData, nil
}

func (view *ViewManager) ProcessPendingQuery(indexedMetalogProgress uint64) {
	view.mu.Lock()
	defer view.mu.Unlock()

	logSpaceId := protocol.GetLogSpaceId(indexedMetalogProgress)
	viewId := protocol.GetViewId(logSpaceId)
	if _, ok := view.viewPool[viewId]; !ok {
		return
	}
	indexPool := view.viewPool[viewId]
	if indexData, ok := indexPool[logSpaceId]; !ok {
		return
	} else {
		indexData.ProcessPendingQuery(indexedMetalogProgress)
	}
}

var (
	IndexSetupErr_Null = errors.New("Failed with null instance")

	IndexQueryErr_CacheMiss          = errors.New("CacheMiss")
	IndexQueryErr_Continue           = errors.New("EAGAIN")
	IndexQueryErr_Pending            = errors.New("EAGAIN")
	IndexQueryErr_AuxDataInvalidUser = errors.New("Invalid User")
)

const (
	QueryType_LocalId  uint8 = 0
	QueryType_ReadPrev uint8 = 1
	QueryType_ReadNext uint8 = 2
)

type queryResponse struct {
	Response []byte
	Err      error
}
type pendingQuery struct {
	queryType uint8
	// args
	metalogProgress uint64
	queryLocalId    uint64
	querySeqNum     uint64
	queryTag        uint64
	// return value
	responseReceiver chan queryResponse
}

type IndexData struct {
	instance     unsafe.Pointer
	userLogSpace uint32

	pendingQueryMu sync.Mutex
	pendingQueries map[uint64][]pendingQuery
}

func InitLibrary(ipcRootPath string, vlogLevel int) {
	cIpcRootPath := C.CString(ipcRootPath)
	defer C.free(unsafe.Pointer(cIpcRootPath))
	C.Init(cIpcRootPath, C.int(vlogLevel))
}

func constructIndexData(metalogProgress uint64 /*DEPRECATED*/, logSpaceId uint32, userLogSpace uint32) (*IndexData, error) {
	viewInst := unsafe.Pointer(C.ConstructIndexData(C.ulong(metalogProgress), C.uint(logSpaceId), C.uint(userLogSpace)))
	if viewInst != nil {
		return &IndexData{
			instance:     viewInst,
			userLogSpace: userLogSpace,

			pendingQueryMu: sync.Mutex{},
			pendingQueries: make(map[uint64][]pendingQuery),
		}, nil
	} else {
		return nil, errors.Wrapf(IndexSetupErr_Null,
			"InstallIndexData failed: metalogProgress=%016X logSpaceId=%08X userLogSpace=%d",
			metalogProgress, logSpaceId, userLogSpace)
	}
}

func (idx *IndexData) Uninstall() {
	C.DestructIndexData(idx.instance)
}

func (idx *IndexData) Inspect() {
	C.Inspect(idx.instance)
}

func (idx *IndexData) AddPendingQuery(queryType uint8, metalogProgress uint64, queryTarget uint64, queryTag uint64,
	responseReceiver chan queryResponse) {

	idx.pendingQueryMu.Lock()
	defer idx.pendingQueryMu.Unlock()
	if _, ok := idx.pendingQueries[metalogProgress]; !ok {
		idx.pendingQueries[metalogProgress] = make([]pendingQuery, 0, 100)
	}
	if queryType == QueryType_LocalId {
		idx.pendingQueries[metalogProgress] = append(idx.pendingQueries[metalogProgress], pendingQuery{
			queryType:        queryType,
			metalogProgress:  metalogProgress,
			queryLocalId:     queryTarget,
			responseReceiver: responseReceiver,
		})
	} else {
		idx.pendingQueries[metalogProgress] = append(idx.pendingQueries[metalogProgress], pendingQuery{
			queryType:        queryType,
			metalogProgress:  metalogProgress,
			querySeqNum:      queryTarget,
			queryTag:         queryTag,
			responseReceiver: responseReceiver,
		})
	}
}

func (idx *IndexData) ProcessPendingQuery(indexedMetalogProgress uint64) {
	idx.pendingQueryMu.Lock()
	defer idx.pendingQueryMu.Unlock()

	for pendingMetalogProgress, pendingQueries := range idx.pendingQueries {
		if indexedMetalogProgress >= pendingMetalogProgress {
			for _, query := range pendingQueries {
				if query.queryType == QueryType_LocalId {
					response, err := idx.doLogReadLocalId(query.metalogProgress, query.queryLocalId)
					if errors.Is(err, IndexQueryErr_Pending) {
						panic("unreachable")
					}
					query.responseReceiver <- queryResponse{Response: response, Err: err}
				} else if query.queryType == QueryType_ReadNext {
					response, err := idx.doLogReadNext(query.metalogProgress, query.querySeqNum, query.queryTag)
					if errors.Is(err, IndexQueryErr_Pending) {
						panic("unreachable")
					}
					query.responseReceiver <- queryResponse{Response: response, Err: err}
				} else if query.queryType == QueryType_ReadPrev {
					response, err := idx.doLogReadPrev(query.metalogProgress, query.querySeqNum, query.queryTag)
					if errors.Is(err, IndexQueryErr_Pending) {
						panic("unreachable")
					}
					query.responseReceiver <- queryResponse{Response: response, Err: err}
				} else {
					panic("unreachable")
				}
			}
			delete(idx.pendingQueries, pendingMetalogProgress)
		}
	}
}

// enum APIReturnValue {
//     ReadOK = faas::log::IndexQueryResult::kFound,
//
//     // enum State { kFound, kEmpty, kContinue, kPending };
//     IndexReadEmpty = faas::log::IndexQueryResult::kEmpty,
//     IndexReadContinue = faas::log::IndexQueryResult::kContinue,
//     IndexReadPending = faas::log::IndexQueryResult::kPending,

//     IndexReadInitFutureViewBail = -1,
//     IndexReadInitCurrentViewPending = -2,
//     IndexReadContinueOK = -3,

//	    LogReadCacheMiss = -4,
//	};
func strerr(resultState int) string {
	switch resultState {
	case 0:
		panic("unreachable")
	case 1:
		return "kEmpty"
	case 2:
		return "kContinue"
	case 3:
		return "kPending"
	case -1:
		return "kInitFutureViewBail"
	case -2:
		return "kInitCurrentViewPending"
	case -3:
		return "kContOK"
	case -4:
		return "LogReadCacheMiss"
	default:
		return "unknown"
	}
}

// func (idx *IndexData) IndexReadLocalId(metalogProgress uint64, localId uint64) (uint64, uint64, uint16, error) {
// 	_InOut_MetalogProgress := metalogProgress
// 	_Out_SeqNum := uint64(0)
// 	_Out_EngineId := uint16(0)
// 	resultState := int(C.IndexReadLocalId(idx.instance,
// 		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
// 		C.uint(idx.userLogSpace),
// 		C.ulong(localId),
// 		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)),
// 		(*C.ushort)(unsafe.Pointer(&_Out_EngineId))))
// 	if resultState < 0 {
// 		return 0, 0, 0, fmt.Errorf("IndexReadLocalId error=%s", strerr(resultState))
// 	} else if resultState == 0 {
// 		return _InOut_MetalogProgress, _Out_SeqNum, _Out_EngineId, nil
// 	} else if resultState == 1 {
// 		return 0, 0, 0, IndexQueryErr_Empty
// 	} else if resultState == 2 {
// 		return 0, 0, 0, IndexQueryErr_Continue
// 	} else if resultState == 3 {
// 		return 0, 0, 0, IndexQueryErr_Pending
// 	} else {
// 		panic("unreachable")
// 	}
// }

// func (idx *IndexData) IndexReadNext(metalogProgress uint64, querySeqNum uint64, queryTag uint64) (uint64, uint64, uint16, error) {
// 	_InOut_MetalogProgress := metalogProgress
// 	_Out_SeqNum := uint64(0)
// 	_Out_EngineId := uint16(0)
// 	resultState := int(C.IndexReadNext(idx.instance,
// 		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
// 		C.uint(idx.userLogSpace),
// 		C.ulong(querySeqNum),
// 		C.ulong(queryTag),
// 		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)),
// 		(*C.ushort)(unsafe.Pointer(&_Out_EngineId))))
// 	if resultState < 0 {
// 		return 0, 0, 0, fmt.Errorf("IndexReadNext error=%s", strerr(resultState))
// 	} else if resultState == 0 {
// 		return _InOut_MetalogProgress, _Out_SeqNum, _Out_EngineId, nil
// 	} else if resultState == 1 {
// 		return 0, 0, 0, IndexQueryErr_Empty
// 	} else if resultState == 2 {
// 		return 0, 0, 0, IndexQueryErr_Continue
// 	} else if resultState == 3 {
// 		return 0, 0, 0, IndexQueryErr_Pending
// 	} else {
// 		panic("unreachable")
// 	}
// }

// func (idx *IndexData) IndexReadPrev(metalogProgress uint64, querySeqNum uint64, queryTag uint64) (uint64, uint64, uint16, error) {
// 	_InOut_MetalogProgress := metalogProgress
// 	_Out_SeqNum := uint64(0)
// 	_Out_EngineId := uint16(0)
// 	resultState := int(C.IndexReadPrev(idx.instance,
// 		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
// 		C.uint(idx.userLogSpace),
// 		C.ulong(querySeqNum),
// 		C.ulong(queryTag),
// 		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)),
// 		(*C.ushort)(unsafe.Pointer(&_Out_EngineId))))
// 	if resultState < 0 {
// 		return 0, 0, 0, fmt.Errorf("IndexReadPrev error=%s", strerr(resultState))
// 	} else if resultState == 0 {
// 		return _InOut_MetalogProgress, _Out_SeqNum, _Out_EngineId, nil
// 	} else if resultState == 1 {
// 		return 0, 0, 0, IndexQueryErr_Empty
// 	} else if resultState == 2 {
// 		return 0, 0, 0, IndexQueryErr_Continue
// 	} else if resultState == 3 {
// 		return 0, 0, 0, IndexQueryErr_Pending
// 	} else {
// 		panic("unreachable")
// 	}
// }

const MessageFullByteSize = 2816

func handleErr(message []byte, resultState int, tip string) ([]byte, error) {
	if resultState == 0 {
		return message, nil
	} else if resultState == 1 {
		return nil, nil
	} else if resultState == 2 {
		return nil, IndexQueryErr_Continue
	} else if resultState == 3 {
		return nil, IndexQueryErr_Pending
	} else if resultState == -4 {
		return message, IndexQueryErr_CacheMiss
	} else {
		return nil, fmt.Errorf("%v error=%s", tip, strerr(resultState))
	}
}

func (idx *IndexData) doLogReadLocalId(metalogProgress uint64, localId uint64) ([]byte, error) {
	message := make([]byte, MessageFullByteSize)
	resultState := int(C.LogReadLocalId(idx.instance,
		C.ulong(metalogProgress),
		C.uint(idx.userLogSpace),
		C.ulong(localId),
		unsafe.Pointer(&message[0])))
	return handleErr(message, resultState, "LogReadLocalId")
}

func (idx *IndexData) doLogReadNext(metalogProgress uint64, querySeqNum uint64, queryTag uint64) ([]byte, error) {
	message := make([]byte, MessageFullByteSize)
	resultState := int(C.LogReadNext(idx.instance,
		C.ulong(metalogProgress),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		unsafe.Pointer(&message[0])))
	return handleErr(message, resultState, "LogReadNext")
}

func (idx *IndexData) doLogReadPrev(metalogProgress uint64, querySeqNum uint64, queryTag uint64) ([]byte, error) {
	message := make([]byte, MessageFullByteSize)
	resultState := int(C.LogReadPrev(idx.instance,
		C.ulong(metalogProgress),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		unsafe.Pointer(&message[0])))
	return handleErr(message, resultState, "LogReadPrev")
}

func (idx *IndexData) LogReadLocalId(metalogProgress uint64, localId uint64) chan queryResponse {
	response, err := idx.doLogReadLocalId(metalogProgress, localId)
	responseCh := make(chan queryResponse, 1)
	if errors.Is(err, IndexQueryErr_Pending) {
		idx.AddPendingQuery(QueryType_LocalId, metalogProgress, localId, 0 /*not used here*/, responseCh)
	} else {
		responseCh <- queryResponse{Response: response, Err: err}
	}
	return responseCh
}

func (idx *IndexData) LogReadNext(metalogProgress uint64, querySeqNum uint64, queryTag uint64) chan queryResponse {
	response, err := idx.doLogReadNext(metalogProgress, querySeqNum, queryTag)
	responseCh := make(chan queryResponse, 1)
	if errors.Is(err, IndexQueryErr_Pending) {
		idx.AddPendingQuery(QueryType_ReadNext, metalogProgress, querySeqNum, queryTag, responseCh)
	} else {
		responseCh <- queryResponse{Response: response, Err: err}
	}
	return responseCh
}

func (idx *IndexData) LogReadPrev(metalogProgress uint64, querySeqNum uint64, queryTag uint64) chan queryResponse {
	response, err := idx.doLogReadPrev(metalogProgress, querySeqNum, queryTag)
	responseCh := make(chan queryResponse, 1)
	if errors.Is(err, IndexQueryErr_Pending) {
		idx.AddPendingQuery(QueryType_ReadPrev, metalogProgress, querySeqNum, queryTag, responseCh)
	} else {
		responseCh <- queryResponse{Response: response, Err: err}
	}
	return responseCh
}

func LogSetAuxData(seqNum uint64, data []byte) error {
	// TODO: how to get user_logspace?
	resultState := int(C.SetAuxData(C.uint(0) /*userLogSpace*/, C.ulong(seqNum), unsafe.Pointer(&data[0]), C.ulong(len(data))))
	if resultState == 0 {
		return nil
	} else if resultState == -5 {
		return IndexQueryErr_AuxDataInvalidUser
	} else {
		panic("unreachable")
	}
}
