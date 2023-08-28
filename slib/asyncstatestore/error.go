package asyncstatestore

import (
	"fmt"
	"log"
	"runtime/debug"
)

const (
	ERROR_Runtime = iota
	ERROR_BadArguments
	ERROR_PathNotExist
	ERROR_PathConflict
	ERROR_WrongDataType
	ERROR_GabsError
)

type Error struct {
	code      int
	message   string
	callstack string
}

func (e *Error) GetCode() int {
	return e.code
}

func (e *Error) Error() string {
	switch e.code {
	case ERROR_Runtime:
		return fmt.Sprintf("RuntimeError: %s %s", e.message, e.callstack)
	case ERROR_BadArguments:
		return fmt.Sprintf("BadArguments: %s %s", e.message, e.callstack)
	case ERROR_PathNotExist:
		return fmt.Sprintf("Path %s not exists %s", e.message, e.callstack)
	case ERROR_PathConflict:
		return fmt.Sprintf("Path %s conflicts %s", e.message, e.callstack)
	case ERROR_GabsError:
		return fmt.Sprintf("GabsError: %s %s", e.message, e.callstack)
	default:
		panic("Unknown error type")
	}
}

func newRuntimeError(message string) error {
	log.Fatalf("[FATAL] RuntimeError: %s\n%s", message, string(debug.Stack()))
	return &Error{code: ERROR_Runtime, message: message, callstack: string(debug.Stack())}
}

func newBadArgumentsError(message string) error {
	return &Error{code: ERROR_BadArguments, message: message, callstack: string(debug.Stack())}
}

func newPathNotExistError(path string) error {
	return &Error{code: ERROR_PathNotExist, message: path, callstack: string(debug.Stack())}
}

func newPathConflictError(path string) error {
	return &Error{code: ERROR_PathConflict, message: path, callstack: string(debug.Stack())}
}

func newGabsError(gabsError error) error {
	return &Error{code: ERROR_GabsError, message: gabsError.Error(), callstack: string(debug.Stack())}
}
