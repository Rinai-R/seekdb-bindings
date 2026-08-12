// Package seekdb provides Go bindings for libseekdb.
package seekdb

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

// ErrClosed reports an operation on a closed instance, connection, or result.
var ErrClosed = errors.New("seekdb: handle is closed")

// Transport identifies the local MySQL-protocol transport exposed by libseekdb.
type Transport string

const (
	// TransportTCP connects through a loopback TCP port.
	TransportTCP Transport = "tcp"
	// TransportUnixSocket connects through a Unix-domain socket.
	TransportUnixSocket Transport = "unix_socket"
	// TransportNamedPipe connects through a Windows named pipe.
	TransportNamedPipe Transport = "named_pipe"
)

// ConnectionOptions contains a copied snapshot of libseekdb connection data.
type ConnectionOptions struct {
	Transport Transport
	Port      uint16
	Endpoint  string
	User      string
}

type parameter struct {
	key   string
	value string
}

type openConfig struct {
	parameters []parameter
}

// Option configures an instance on first initialization.
type Option func(*openConfig) error

// WithParameter passes a server parameter to libseekdb. A later option with
// the same key replaces the earlier value.
func WithParameter(key, value string) Option {
	return func(config *openConfig) error {
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
	}
}

var instances = struct {
	sync.Mutex
	byDirectory map[string]*instanceState
}{byDirectory: make(map[string]*instanceState)}

type instanceState struct {
	handle nativeInstance
	refs   int
}

func retainInstanceState(state *instanceState) {
	instances.Lock()
	state.refs++
	instances.Unlock()
}

func releaseInstanceState(directory string, state *instanceState) error {
	instances.Lock()
	defer instances.Unlock()

	state.refs--
	if state.refs > 0 {
		return nil
	}
	if code := nativeClose(state.handle); code != Success {
		state.refs++
		return operationError("close instance", code)
	}
	state.handle = nativeInstance{}
	if instances.byDirectory[directory] == state {
		delete(instances.byDirectory, directory)
	}
	return nil
}

// Instance owns a reference to a SeekDB database directory.
type Instance struct {
	mu        sync.Mutex
	directory string
	state     *instanceState
}

// Open starts or attaches to the SeekDB instance rooted at directory.
func Open(directory string, options ...Option) (*Instance, error) {
	if directory == "" {
		return nil, errors.New("seekdb: data directory is empty")
	}
	if strings.IndexByte(directory, 0) >= 0 {
		return nil, errors.New("seekdb: data directory contains a NUL byte")
	}
	normalized, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("seekdb: normalize data directory: %w", err)
	}
	normalized = filepath.Clean(normalized)

	config := openConfig{}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}

	instances.Lock()
	defer instances.Unlock()
	if state := instances.byDirectory[normalized]; state != nil {
		state.refs++
		return &Instance{directory: normalized, state: state}, nil
	}

	handle, code := nativeOpen(normalized, config.parameters)
	if code != Success {
		if handle.pointer != nil {
			nativeClose(handle)
		}
		return nil, operationError("open instance", code)
	}
	state := &instanceState{handle: handle, refs: 1}
	instances.byDirectory[normalized] = state
	return &Instance{directory: normalized, state: state}, nil
}

// DataDirectory returns the normalized absolute database directory.
func (instance *Instance) DataDirectory() string {
	if instance == nil {
		return ""
	}
	return instance.directory
}

// Closed reports whether this Instance has released its reference.
func (instance *Instance) Closed() bool {
	if instance == nil {
		return true
	}
	instance.mu.Lock()
	defer instance.mu.Unlock()
	return instance.state == nil
}

// ConnectionOptions returns a copied snapshot suitable for external clients.
func (instance *Instance) ConnectionOptions() (ConnectionOptions, error) {
	state, err := instance.retainState()
	if err != nil {
		return ConnectionOptions{}, err
	}
	defer releaseInstanceState(instance.directory, state)

	options, code := nativeConnectionOptions(state.handle)
	if code != Success {
		return ConnectionOptions{}, operationError("get connection options", code)
	}
	return options, nil
}

// Connect opens a native libseekdb connection. The connection retains the
// underlying instance until it is closed.
func (instance *Instance) Connect(database string, autocommit bool) (*Connection, error) {
	if strings.IndexByte(database, 0) >= 0 {
		return nil, errors.New("seekdb: database name contains a NUL byte")
	}
	state, err := instance.retainState()
	if err != nil {
		return nil, err
	}

	handle, nativeErr := nativeConnect(state.handle, database, autocommit)
	if nativeErr != nil {
		releaseInstanceState(instance.directory, state)
		return nil, nativeErr
	}
	connectionState := &connectionState{
		handle:            handle,
		refs:              1,
		instanceDirectory: instance.directory,
		instance:          state,
	}
	return &Connection{state: connectionState}, nil
}

// Close releases this Instance reference. It is safe to call repeatedly.
func (instance *Instance) Close() error {
	if instance == nil {
		return nil
	}
	instance.mu.Lock()
	defer instance.mu.Unlock()
	if instance.state == nil {
		return nil
	}
	if err := releaseInstanceState(instance.directory, instance.state); err != nil {
		return err
	}
	instance.state = nil
	return nil
}

func (instance *Instance) retainState() (*instanceState, error) {
	if instance == nil {
		return nil, ErrClosed
	}
	instance.mu.Lock()
	defer instance.mu.Unlock()
	if instance.state == nil {
		return nil, ErrClosed
	}
	retainInstanceState(instance.state)
	return instance.state, nil
}
