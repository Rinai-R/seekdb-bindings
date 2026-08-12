// Package seekdb manages an embedded seekdb instance through libseekdb.
//
// An Instance owns the lifecycle handle returned by libseekdb. Callers must
// close every external database connection before closing the Instance.
package seekdb

/*
#cgo CFLAGS: -I${SRCDIR}/../../lib/include
#cgo LDFLAGS: -lseekdb

#include <stdlib.h>
#include "seekdb.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

// ErrClosed is returned when an operation uses a closed Instance.
var ErrClosed = errors.New("seekdb: instance is closed")

// Error reports an error returned by libseekdb.
type Error struct {
	Operation string
	Code      int
}

func (e *Error) Error() string {
	return fmt.Sprintf("seekdb: %s failed with code %d", e.Operation, e.Code)
}

// Transport identifies how a MySQL-protocol client connects to an Instance.
type Transport string

const (
	TransportTCP        Transport = "tcp"
	TransportUnixSocket Transport = "unix_socket"
	TransportNamedPipe  Transport = "named_pipe"
)

// ConnectionOptions contains the borrowed server connection information copied
// from libseekdb. It remains safe to use after Instance.Close.
type ConnectionOptions struct {
	Transport Transport
	Port      uint16
	Endpoint  string
	User      string
}

type openConfig struct {
	parameters []parameter
}

type parameter struct {
	key   string
	value string
}

// Option configures a newly initialized seekdb data directory.
//
// Seekdb persists server parameters. Parameters passed while reopening an
// existing data directory do not replace persisted values.
type Option interface {
	apply(*openConfig) error
}

type optionFunc func(*openConfig) error

func (option optionFunc) apply(config *openConfig) error {
	return option(config)
}

// WithParameter passes a seekdb server parameter during first initialization.
// Later options with the same key replace earlier ones.
func WithParameter(key, value string) Option {
	return optionFunc(func(config *openConfig) error {
		if key == "" {
			return errors.New("seekdb: parameter key is empty")
		}
		if strings.IndexByte(key, 0) >= 0 || strings.IndexByte(value, 0) >= 0 {
			return errors.New("seekdb: parameter contains a NUL byte")
		}

		for index := range config.parameters {
			if config.parameters[index].key == key {
				config.parameters[index].value = value
				return nil
			}
		}
		config.parameters = append(config.parameters, parameter{key: key, value: value})
		return nil
	})
}

// WithTCPPort makes the embedded instance listen on the loopback TCP port.
// Without this option, libseekdb uses a Unix socket on Unix and a named pipe on
// Windows.
func WithTCPPort(port uint16) Option {
	return optionFunc(func(config *openConfig) error {
		if port == 0 {
			return errors.New("seekdb: TCP port must be greater than zero")
		}
		return WithParameter("port", strconv.FormatUint(uint64(port), 10)).apply(config)
	})
}

// Instance owns a libseekdb lifecycle handle. Its methods are safe for
// concurrent use.
type Instance struct {
	mu     sync.RWMutex
	handle C.SeekdbHandle
}

// Open starts or attaches to the seekdb instance rooted at dataDirectory.
// Open may block while a new local server finishes initialization.
func Open(dataDirectory string, options ...Option) (*Instance, error) {
	if dataDirectory == "" {
		return nil, errors.New("seekdb: data directory is empty")
	}
	if strings.IndexByte(dataDirectory, 0) >= 0 {
		return nil, errors.New("seekdb: data directory contains a NUL byte")
	}

	config := openConfig{}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option.apply(&config); err != nil {
			return nil, err
		}
	}

	directory := C.CString(dataDirectory)
	defer C.free(unsafe.Pointer(directory))

	parameters, release := cParameters(config.parameters)
	defer release()

	var handle C.SeekdbHandle
	code := C.seekdb_open(directory, parameters, &handle)
	if code != C.SEEKDB_SUCCESS {
		if handle != nil {
			C.seekdb_close(handle)
		}
		return nil, &Error{Operation: "open", Code: int(code)}
	}

	return &Instance{handle: handle}, nil
}

// ConnectionOptions returns connection information for an external
// MySQL-protocol driver.
func (instance *Instance) ConnectionOptions() (ConnectionOptions, error) {
	if instance == nil {
		return ConnectionOptions{}, ErrClosed
	}

	instance.mu.RLock()
	defer instance.mu.RUnlock()
	if instance.handle == nil {
		return ConnectionOptions{}, ErrClosed
	}

	var native C.SeekdbConnectionOptions
	code := C.seekdb_connection_options(instance.handle, &native)
	if code != C.SEEKDB_SUCCESS {
		return ConnectionOptions{}, &Error{Operation: "connection options", Code: int(code)}
	}

	options := ConnectionOptions{Port: uint16(native.port)}
	if native.transport != nil {
		options.Transport = Transport(C.GoString(native.transport))
	}
	if native.endpoint != nil {
		options.Endpoint = C.GoString(native.endpoint)
	}
	if native.user != nil {
		options.User = C.GoString(native.user)
	}
	return options, nil
}

// Close releases the Instance lifecycle handle. Close is idempotent. External
// database connections must be closed before this method is called.
func (instance *Instance) Close() error {
	if instance == nil {
		return nil
	}

	instance.mu.Lock()
	defer instance.mu.Unlock()
	if instance.handle == nil {
		return nil
	}

	code := C.seekdb_close(instance.handle)
	if code != C.SEEKDB_SUCCESS {
		return &Error{Operation: "close", Code: int(code)}
	}
	instance.handle = nil
	return nil
}

func cParameters(parameters []parameter) (**C.char, func()) {
	if len(parameters) == 0 {
		return nil, func() {}
	}

	values := make([]*C.char, len(parameters)*2+1)
	for index, parameter := range parameters {
		values[index*2] = C.CString(parameter.key)
		values[index*2+1] = C.CString(parameter.value)
	}

	release := func() {
		for _, value := range values[:len(values)-1] {
			C.free(unsafe.Pointer(value))
		}
	}
	return (**C.char)(unsafe.Pointer(&values[0])), release
}
