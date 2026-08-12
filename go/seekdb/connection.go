package seekdb

import (
	"errors"
	"strings"
	"sync"
)

type connectionState struct {
	mu                sync.Mutex
	handle            nativeConnection
	refs              int
	instanceDirectory string
	instance          *instanceState
}

func (state *connectionState) retain() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.handle.pointer == nil {
		return ErrClosed
	}
	state.refs++
	return nil
}

func (state *connectionState) release() error {
	state.mu.Lock()
	state.refs--
	if state.refs > 0 {
		state.mu.Unlock()
		return nil
	}
	code := nativeDisconnect(state.handle)
	if code != Success {
		state.refs++
		state.mu.Unlock()
		return operationError("close connection", code)
	}
	state.handle = nativeConnection{}
	instanceDirectory := state.instanceDirectory
	instance := state.instance
	state.instance = nil
	state.mu.Unlock()
	return releaseInstanceState(instanceDirectory, instance)
}

// Connection is a native libseekdb SQL connection.
type Connection struct {
	mu    sync.Mutex
	state *connectionState
}

// Exec executes a statement and discards its result.
func (connection *Connection) Exec(query string) error {
	result, err := connection.Query(query)
	if err != nil {
		return err
	}
	return result.Close()
}

// Closed reports whether this Connection has been closed.
func (connection *Connection) Closed() bool {
	if connection == nil {
		return true
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.state == nil
}

// Query executes SQL and returns its native result set.
func (connection *Connection) Query(query string) (*Result, error) {
	if strings.IndexByte(query, 0) >= 0 {
		return nil, errors.New("seekdb: query contains a NUL byte")
	}
	state, err := connection.retainState()
	if err != nil {
		return nil, err
	}

	state.mu.Lock()
	handle, nativeErr := nativeQuery(state.handle, query)
	state.mu.Unlock()
	if nativeErr != nil {
		state.release()
		return nil, nativeErr
	}
	return &Result{handle: handle, connection: state}, nil
}

// Begin starts a transaction.
func (connection *Connection) Begin() error {
	return connection.transaction("begin transaction", nativeBegin)
}

// Commit commits the active transaction.
func (connection *Connection) Commit() error {
	return connection.transaction("commit transaction", nativeCommit)
}

// Rollback rolls back the active transaction.
func (connection *Connection) Rollback() error {
	return connection.transaction("rollback transaction", nativeRollback)
}

// Close disconnects this Connection. Outstanding Result values keep the
// native connection alive until they are closed.
func (connection *Connection) Close() error {
	if connection == nil {
		return nil
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.state == nil {
		return nil
	}
	if err := connection.state.release(); err != nil {
		return err
	}
	connection.state = nil
	return nil
}

func (connection *Connection) transaction(operation string, call func(nativeConnection) *Error) error {
	state, err := connection.retainState()
	if err != nil {
		return err
	}
	defer state.release()

	state.mu.Lock()
	nativeErr := call(state.handle)
	state.mu.Unlock()
	if nativeErr != nil {
		nativeErr.Operation = operation
		return nativeErr
	}
	return nil
}

func (connection *Connection) retainState() (*connectionState, error) {
	if connection == nil {
		return nil, ErrClosed
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.state == nil {
		return nil, ErrClosed
	}
	if err := connection.state.retain(); err != nil {
		return nil, err
	}
	return connection.state, nil
}
