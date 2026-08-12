package seekdb

import "fmt"

// Code is a libseekdb return code.
type Code int

const (
	// Success indicates a successful libseekdb operation.
	Success Code = 0
	// InternalError indicates a native runtime or server error.
	InternalError Code = -1
	// InvalidArgument indicates an invalid C API argument.
	InvalidArgument Code = -2
	// NoMoreRows indicates normal result-set exhaustion.
	NoMoreRows Code = -3
)

// Error reports an error returned by libseekdb or the native SQL connection.
type Error struct {
	Operation  string
	Code       Code
	ServerCode int
	Message    string
}

func (err *Error) Error() string {
	if err.Message != "" && err.ServerCode != 0 {
		return fmt.Sprintf("seekdb: %s: server error %d: %s", err.Operation, err.ServerCode, err.Message)
	}
	if err.Message != "" {
		return fmt.Sprintf("seekdb: %s: %s", err.Operation, err.Message)
	}
	return fmt.Sprintf("seekdb: %s failed with code %d", err.Operation, err.Code)
}

func operationError(operation string, code Code) error {
	if code == Success {
		return nil
	}
	return &Error{Operation: operation, Code: code}
}
