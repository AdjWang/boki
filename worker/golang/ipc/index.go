package ipc

// #include <stdlib.h>
// #include <index_data_c.h>
import "C"

import (
	"fmt"
	"log"
	"unsafe"

	"github.com/pkg/errors"
)

// DEBUG
// func GetViewShmPath(viewId uint16) string {
// 	return fmt.Sprintf("%s/shm/view_%d", rootPathForIpc, viewId)
// }

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

func InitLibrary(ipcRootPath string) {
	cIpcRootPath := C.CString(ipcRootPath)
	defer C.free(unsafe.Pointer(cIpcRootPath))
	C.Init(cIpcRootPath)
}

func InstallIndexData(logSpaceId uint32) (*IndexData, error) {
	// TODO: this function should do only once
	// TODO: call SharedLogInstallView(viewId) here to install view shm data
	viewInst := unsafe.Pointer(C.ConstructIndexData(C.uint(logSpaceId), C.uint(0)))
	return &IndexData{
		instance:     viewInst,
		userLogSpace: uint32(0), // TODO: return by SharedLogInstallView(viewId)
	}, nil
}

func (idx *IndexData) Uninstall() {
	C.DestructIndexData(idx.instance)
}

func strerr(resultState int) string {
	switch resultState {
	case -1:
		return "kInitFutureViewBail"
	case -2:
		return "kInitCurrentViewPending"
	case -3:
		return "kContOK"
	default:
		return "unknown"
	}
}

func (idx *IndexData) ProcessLocalIdQuery(metalogProgress uint64, localId uint64) (uint64, uint64, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	resultState := int(C.int(C.IndexReadLocalId(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(localId),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)))))
	if resultState < 0 {
		return 0, 0, fmt.Errorf("ProcessLocalIdQuery error=%s", strerr(resultState))
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

func (idx *IndexData) ProcessReadNext(metalogProgress uint64, querySeqNum uint64, queryTag uint64) (uint64, uint64, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	resultState := int(C.int(C.IndexReadNext(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)))))
	if resultState < 0 {
		return 0, 0, fmt.Errorf("ProcessReadNext error=%s", strerr(resultState))
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

func (idx *IndexData) ProcessReadPrev(metalogProgress uint64, querySeqNum uint64, queryTag uint64) (uint64, uint64, error) {
	_InOut_MetalogProgress := metalogProgress
	_Out_SeqNum := uint64(0)
	resultState := int(C.int(C.IndexReadPrev(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
		C.uint(idx.userLogSpace),
		C.ulong(querySeqNum),
		C.ulong(queryTag),
		(*C.ulong)(unsafe.Pointer(&_Out_SeqNum)))))
	if resultState < 0 {
		return 0, 0, fmt.Errorf("ProcessReadPrev error=%s", strerr(resultState))
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
