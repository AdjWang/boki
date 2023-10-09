package main

// #cgo CFLAGS: -I../src/base -I../src/log
// #cgo LDFLAGS: -L../bin/debug -lrt -ldl -lindex
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
	log.Println("test binding")
	TestBinding()
	log.Println("test allocation")
	indexData, err := InstallIndexData(1)
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
	metaLogProgress, resultSeqNum, err := indexData.ProcessReadPrev(0 /*metaLogProgress*/, seqNum, uint64(2) /*tag*/)
	if err != nil {
		panic(err)
	}
	log.Printf("ProcessReadPrev metaLogProgress=%016X, resultSeqNum=%016X", metaLogProgress, resultSeqNum)
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

// TODO: add index lock

// DEBUG
func (idx *IndexData) Inspect() {
	C.Inspect(idx.instance)
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
	resultState := int(C.int(C.ProcessLocalIdQuery(idx.instance,
		(*C.ulong)(unsafe.Pointer(&_InOut_MetalogProgress)),
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
	resultState := int(C.int(C.ProcessReadNext(idx.instance,
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
	resultState := int(C.int(C.ProcessReadPrev(idx.instance,
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
