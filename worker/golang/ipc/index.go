package ipc

// #include <stdlib.h>
// #include <index_data_c.h>
import "C"

import (
	"fmt"
	"log"
	"sync"
	"unsafe"

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

type IndexDataManager struct {
	mu        sync.Mutex
	indexPool map[uint32]*IndexData
}

func NewIndexDataManager() *IndexDataManager {
	return &IndexDataManager{
		mu:        sync.Mutex{},
		indexPool: map[uint32]*IndexData{},
	}
}

func (im *IndexDataManager) LoadIndexData(metalogProgress uint64, seqNum uint64) (*IndexData, error) {
	// TODO: how to get user_logspace?
	logSpaceId := uint32(C.GetLogSpaceIdentifier(C.uint(0)))

	im.mu.Lock()
	defer im.mu.Unlock()
	if indexData, ok := im.indexPool[logSpaceId]; ok {
		return indexData, nil
	}
	indexData, err := ConstructIndexData(metalogProgress, logSpaceId)
	if err != nil {
		return nil, err
	}
	im.indexPool[logSpaceId] = indexData
	return indexData, nil
}

var (
	Err_Empty              = errors.New("EMPTY")
	Err_CacheMiss          = errors.New("CacheMiss")
	Err_Continue           = errors.New("EAGAIN")
	Err_Pending            = errors.New("EAGAIN")
	Err_AuxDataInvalidUser = errors.New("Invalid User")
)

type IndexData struct {
	instance     unsafe.Pointer
	userLogSpace uint32
}

func InitLibrary(ipcRootPath string, vlogLevel int) {
	cIpcRootPath := C.CString(ipcRootPath)
	defer C.free(unsafe.Pointer(cIpcRootPath))
	C.Init(cIpcRootPath, C.int(vlogLevel))
}

func ConstructIndexData(metalogProgress uint64, logSpaceId uint32) (*IndexData, error) {
	// TODO: call SharedLogInstallView(viewId) here to install view shm data
	viewInst := unsafe.Pointer(C.ConstructIndexData(C.ulong(metalogProgress), C.uint(logSpaceId), C.uint(0)))
	if viewInst != nil {
		return &IndexData{
			instance:     viewInst,
			userLogSpace: uint32(0), // TODO: return by SharedLogInstallView(viewId)
		}, nil
	} else {
		return nil, fmt.Errorf("InstallIndexData failed: metalogProgress=%016X logSpaceId=%08X userLogSpace=%d",
			metalogProgress, logSpaceId, 0)
	}
}

func (idx *IndexData) Uninstall() {
	C.DestructIndexData(idx.instance)
}

func (idx *IndexData) Inspect() {
	C.Inspect(idx.instance)
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

func (idx *IndexData) IndexReadLocalId(metalogProgress uint64, localId uint64) (uint64, uint64, uint16, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	_Out_EngineId := uint16(0)
	resultState := int(C.IndexReadLocalId(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(localId),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)),
		(*C.ushort)(unsafe.Pointer(&_Out_EngineId))))
	if resultState < 0 {
		return 0, 0, 0, fmt.Errorf("IndexReadLocalId error=%s", strerr(resultState))
	} else if resultState == 0 {
		return _InOut_MetalogProgress, _Out_SeqNum, _Out_EngineId, nil
	} else if resultState == 1 {
		return 0, 0, 0, Err_Empty
	} else if resultState == 2 {
		return 0, 0, 0, Err_Continue
	} else if resultState == 3 {
		return 0, 0, 0, Err_Pending
	} else {
		panic("unreachable")
	}
}

func (idx *IndexData) IndexReadNext(metalogProgress uint64, querySeqNum uint64, queryTag uint64) (uint64, uint64, uint16, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	_Out_EngineId := uint16(0)
	resultState := int(C.IndexReadNext(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)),
		(*C.ushort)(unsafe.Pointer(&_Out_EngineId))))
	if resultState < 0 {
		return 0, 0, 0, fmt.Errorf("IndexReadNext error=%s", strerr(resultState))
	} else if resultState == 0 {
		return _InOut_MetalogProgress, _Out_SeqNum, _Out_EngineId, nil
	} else if resultState == 1 {
		return 0, 0, 0, Err_Empty
	} else if resultState == 2 {
		return 0, 0, 0, Err_Continue
	} else if resultState == 3 {
		return 0, 0, 0, Err_Pending
	} else {
		panic("unreachable")
	}
}

func (idx *IndexData) IndexReadPrev(metalogProgress uint64, querySeqNum uint64, queryTag uint64) (uint64, uint64, uint16, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	_Out_EngineId := uint16(0)
	resultState := int(C.IndexReadPrev(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)),
		(*C.ushort)(unsafe.Pointer(&_Out_EngineId))))
	if resultState < 0 {
		return 0, 0, 0, fmt.Errorf("IndexReadPrev error=%s", strerr(resultState))
	} else if resultState == 0 {
		return _InOut_MetalogProgress, _Out_SeqNum, _Out_EngineId, nil
	} else if resultState == 1 {
		return 0, 0, 0, Err_Empty
	} else if resultState == 2 {
		return 0, 0, 0, Err_Continue
	} else if resultState == 3 {
		return 0, 0, 0, Err_Pending
	} else {
		panic("unreachable")
	}
}

const MessageFullByteSize = 2816

func (idx *IndexData) LogReadLocalId(metalogProgress uint64, localId uint64) ([]byte, error) {
	message := make([]byte, MessageFullByteSize)
	resultState := int(C.LogReadLocalId(idx.instance,
		C.ulong(metalogProgress),
		C.uint(idx.userLogSpace),
		C.ulong(localId),
		unsafe.Pointer(&message[0])))
	if resultState == 0 {
		return message, nil
	} else if resultState == 1 {
		return nil, nil
	} else if resultState == -4 {
		return message, Err_CacheMiss
	} else {
		return nil, fmt.Errorf("LogReadLocalId error=%s", strerr(resultState))
	}
}

func (idx *IndexData) LogReadNext(metalogProgress uint64, querySeqNum uint64, queryTag uint64) ([]byte, error) {
	message := make([]byte, MessageFullByteSize)
	resultState := int(C.LogReadNext(idx.instance,
		C.ulong(metalogProgress),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		unsafe.Pointer(&message[0])))
	if resultState == 0 {
		return message, nil
	} else if resultState == 1 {
		return nil, nil
	} else if resultState == -4 {
		return message, Err_CacheMiss
	} else {
		return nil, fmt.Errorf("LogReadNext error=%s", strerr(resultState))
	}
}

func (idx *IndexData) LogReadPrev(metalogProgress uint64, querySeqNum uint64, queryTag uint64) ([]byte, error) {
	message := make([]byte, MessageFullByteSize)
	resultState := int(C.LogReadPrev(idx.instance,
		C.ulong(metalogProgress),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		unsafe.Pointer(&message[0])))
	if resultState == 0 {
		return message, nil
	} else if resultState == 1 {
		return nil, nil
	} else if resultState == -4 {
		return message, Err_CacheMiss
	} else {
		return nil, fmt.Errorf("LogReadPrev error=%s", strerr(resultState))
	}
}

func LogSetAuxData(seqNum uint64, data []byte) error {
	// TODO: how to get user_logspace?
	resultState := int(C.SetAuxData(C.uint(0) /*userLogSpace*/, C.ulong(seqNum), unsafe.Pointer(&data[0]), C.ulong(len(data))))
	if resultState == 0 {
		return nil
	} else if resultState == -5 {
		return Err_AuxDataInvalidUser
	} else {
		panic("unreachable")
	}
}
