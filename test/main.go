package main

// #cgo CFLAGS: -I../src/base -I../src/log
// #cgo LDFLAGS: -L../bin/debug -lrt -ldl -lindex
// #include <index_data_c.h>
import "C"

import "fmt"

func main() {
	fmt.Println("main enter")
	C.test_func()
}
