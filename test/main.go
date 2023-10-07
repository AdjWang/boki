package main

// #cgo CFLAGS: -I../src/base -I../src/log
// #cgo LDFLAGS: -L../bin/debug -lrt -ldl -lindex
// #include <index_data_c.h>
import "C"

import (
	"fmt"
	"unsafe"
)

func main() {
	fmt.Println("main enter")
	varIn := 3
	varInOut := 2
	varOut := 0
	ret := C.int(C.test_func(C.uint(varIn), (*C.ulong)(unsafe.Pointer(&varInOut)), (*C.ulong)(unsafe.Pointer(&varOut))))
	fmt.Printf("go binding test_func In=%d InOut=%d Out=%d ret=%d\n", varIn, varInOut, varOut, ret)
}
