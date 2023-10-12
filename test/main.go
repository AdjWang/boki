package main

// #cgo CFLAGS: -I../lib/shared_index/include
// #cgo LDFLAGS: -L../lib/shared_index/bin/debug -lrt -ldl -lindex
// #include <stdlib.h>
// #include <index_data_c.h>
import "C"

import (
	"errors"
	"fmt"
	"log"
	"unsafe"
)

func main() {
	log.Println("main enter")
	vlogLevel := 1
	InitLibrary("/tmp/boki/ipc", vlogLevel)
	log.Println("test binding")
	TestBinding()

	logSpaceId := uint32(C.GetLogSpaceIdentifier(C.uint(0)))
	log.Printf("GetLogSpaceIdentifier of userLogSpace 0, got=%08X", logSpaceId)

	log.Println("test allocation")
	indexData, err := ConstructIndexData(0, 1)
	if err != nil {
		panic(err)
	}
	defer func() {
		log.Println("test deallocation")
		indexData.Uninstall()
	}()

	log.Println("index inspect")
	indexData.Inspect()

	log.Println("test query")
	seqNum := uint64(0x000000010000000F)
	metaLogProgress, resultSeqNum, err := indexData.IndexReadPrev(0 /*metaLogProgress*/, seqNum, uint64(2) /*tag*/)
	if err != nil {
		log.Printf("IndexReadPrev error=%v", err)
	} else {
		log.Printf("IndexReadPrev metaLogProgress=%016X, resultSeqNum=%016X", metaLogProgress, resultSeqNum)
	}
}

// DEBUG
func TestBinding() {
	varIn := 3
	varInOut := 2
	varOut := 0
	ret := int(C.int(C.test_func(C.uint(varIn), (*C.ulong)(unsafe.Pointer(&varInOut)), (*C.ulong)(unsafe.Pointer(&varOut)))))
	log.Printf("go binding test_func In=%d InOut=%d Out=%d ret=%d\n", varIn, varInOut, varOut, ret)
}

var (
	Err_Empty    = errors.New("EMPTY")
	Err_Continue = errors.New("EAGAIN")
	Err_Pending  = errors.New("EAGAIN")
)

type IndexData struct {
	instance     unsafe.Pointer
	userLogSpace uint32
}

// DEBUG
func (idx *IndexData) Inspect() {
	C.Inspect(idx.instance)
}

func InitLibrary(ipcRootPath string, vlogLevel int) {
	cIpcRootPath := C.CString(ipcRootPath)
	defer C.free(unsafe.Pointer(cIpcRootPath))
	C.Init(cIpcRootPath, C.int(vlogLevel))
}

func ConstructIndexData(metalogProgress uint64, logSpaceId uint32) (*IndexData, error) {
	// TODO: this function should do only once
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

func (idx *IndexData) IndexReadLocalId(metalogProgress uint64, localId uint64) (uint64, uint64, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	resultState := int(C.int(C.IndexReadLocalId(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(localId),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)))))
	if resultState < 0 {
		return 0, 0, fmt.Errorf("IndexReadLocalId error=%s", strerr(resultState))
	} else if resultState == 0 {
		return _InOut_MetalogProgress, _Out_SeqNum, nil
	} else if resultState == 1 {
		return 0, 0, Err_Empty
	} else if resultState == 2 {
		return 0, 0, Err_Continue
	} else if resultState == 3 {
		return 0, 0, Err_Pending
	} else {
		panic("unreachable")
	}
}

func (idx *IndexData) IndexReadNext(metalogProgress uint64, querySeqNum uint64, queryTag uint64) (uint64, uint64, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	resultState := int(C.int(C.IndexReadNext(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)))))
	if resultState < 0 {
		return 0, 0, fmt.Errorf("IndexReadNext error=%s", strerr(resultState))
	} else if resultState == 0 {
		return _InOut_MetalogProgress, _Out_SeqNum, nil
	} else if resultState == 1 {
		return 0, 0, Err_Empty
	} else if resultState == 2 {
		return 0, 0, Err_Continue
	} else if resultState == 3 {
		return 0, 0, Err_Pending
	} else {
		panic("unreachable")
	}
}

func (idx *IndexData) IndexReadPrev(metalogProgress uint64, querySeqNum uint64, queryTag uint64) (uint64, uint64, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	resultState := int(C.int(C.IndexReadPrev(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)))))
	if resultState < 0 {
		return 0, 0, fmt.Errorf("IndexReadPrev error=%s", strerr(resultState))
	} else if resultState == 0 {
		return _InOut_MetalogProgress, _Out_SeqNum, nil
	} else if resultState == 1 {
		return 0, 0, Err_Empty
	} else if resultState == 2 {
		return 0, 0, Err_Continue
	} else if resultState == 3 {
		return 0, 0, Err_Pending
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
	} else {
		return nil, fmt.Errorf("LogReadPrev error=%s", strerr(resultState))
	}
}
