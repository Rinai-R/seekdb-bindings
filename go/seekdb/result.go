package seekdb

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNoCurrentRow reports Value or Values before a successful call to Next.
var ErrNoCurrentRow = errors.New("seekdb: result is not positioned on a row")

// TypeID identifies a column type reported by libseekdb.
type TypeID int

const (
	// TypeNull identifies a NULL column type.
	TypeNull TypeID = iota
	// TypeInt64 identifies a signed integer.
	TypeInt64
	// TypeUint64 identifies an unsigned integer.
	TypeUint64
	// TypeFloat identifies a floating-point value.
	TypeFloat
	// TypeDecimal identifies an exact decimal value.
	TypeDecimal
	// TypeDate identifies a date value.
	TypeDate
	// TypeDateTime identifies a datetime value.
	TypeDateTime
	// TypeTimestamp identifies a timestamp value.
	TypeTimestamp
	// TypeVarchar identifies a string value.
	TypeVarchar
)

// Decimal preserves the exact text representation of a SQL decimal.
type Decimal string

// Column describes a result column.
type Column struct {
	Name string
	Type TypeID
}

// Result owns one native libseekdb result set.
type Result struct {
	mu         sync.Mutex
	handle     nativeResult
	connection *connectionState
	err        error
	current    bool
}

// Columns returns the result-set column metadata.
func (result *Result) Columns() ([]Column, error) {
	result.mu.Lock()
	defer result.mu.Unlock()
	if result.handle.pointer == nil {
		return nil, ErrClosed
	}
	result.connection.mu.Lock()
	defer result.connection.mu.Unlock()
	count, code := nativeResultColumnCount(result.handle)
	if code != Success {
		return nil, operationError("get column count", code)
	}
	columns := make([]Column, count)
	for index := range columns {
		name, code := nativeResultColumnName(result.handle, index)
		if code != Success {
			return nil, operationError("get column name", code)
		}
		typeID, code := nativeResultColumnType(result.handle, index)
		if code != Success {
			return nil, operationError("get column type", code)
		}
		columns[index] = Column{Name: name, Type: typeID}
	}
	return columns, nil
}

// RowCount returns the number of buffered rows.
func (result *Result) RowCount() (int64, error) {
	result.mu.Lock()
	defer result.mu.Unlock()
	if result.handle.pointer == nil {
		return 0, ErrClosed
	}
	result.connection.mu.Lock()
	defer result.connection.mu.Unlock()
	count, code := nativeResultRowCount(result.handle)
	if code != Success {
		return 0, operationError("get row count", code)
	}
	return count, nil
}

// Next advances to the next row.
func (result *Result) Next() bool {
	result.mu.Lock()
	defer result.mu.Unlock()
	if result.handle.pointer == nil || result.err != nil {
		return false
	}
	result.connection.mu.Lock()
	defer result.connection.mu.Unlock()
	code := nativeResultNext(result.handle)
	if code == Success {
		result.current = true
		return true
	}
	result.current = false
	if code != NoMoreRows {
		result.err = operationError("advance result", code)
	}
	return false
}

// Err returns the first iteration error, if any.
func (result *Result) Err() error {
	if result == nil {
		return ErrClosed
	}
	result.mu.Lock()
	defer result.mu.Unlock()
	return result.err
}

// Values returns every value in the current row.
func (result *Result) Values() ([]any, error) {
	result.mu.Lock()
	defer result.mu.Unlock()
	if result.handle.pointer == nil {
		return nil, ErrClosed
	}
	if !result.current {
		return nil, ErrNoCurrentRow
	}
	result.connection.mu.Lock()
	defer result.connection.mu.Unlock()
	count, code := nativeResultColumnCount(result.handle)
	if code != Success {
		return nil, operationError("get column count", code)
	}
	values := make([]any, count)
	for index := range values {
		value, err := result.value(index)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

// Value returns one value from the current row.
func (result *Result) Value(index int) (any, error) {
	result.mu.Lock()
	defer result.mu.Unlock()
	if result.handle.pointer == nil {
		return nil, ErrClosed
	}
	if !result.current {
		return nil, ErrNoCurrentRow
	}
	result.connection.mu.Lock()
	defer result.connection.mu.Unlock()
	return result.value(index)
}

// Close releases the native result. It is safe to call repeatedly.
func (result *Result) Close() error {
	if result == nil {
		return nil
	}
	result.mu.Lock()
	defer result.mu.Unlock()
	if result.handle.pointer == nil {
		return nil
	}
	connection := result.connection
	connection.mu.Lock()
	code := nativeResultClose(result.handle)
	connection.mu.Unlock()
	if code != Success {
		return operationError("close result", code)
	}
	result.handle = nativeResult{}
	result.current = false
	result.connection = nil
	return connection.release()
}

func (result *Result) value(index int) (any, error) {
	typeID, code := nativeResultColumnType(result.handle, index)
	if code != Success {
		return nil, operationError("get column type", code)
	}
	text, isNull, code := nativeResultString(result.handle, index)
	if code != Success {
		return nil, operationError("read column", code)
	}
	if isNull || typeID == TypeNull {
		return nil, nil
	}

	switch typeID {
	case TypeInt64:
		value, code := nativeResultInt64(result.handle, index)
		return value, operationError("read signed integer", code)
	case TypeUint64:
		value, code := nativeResultUint64(result.handle, index)
		return value, operationError("read unsigned integer", code)
	case TypeFloat:
		value, code := nativeResultFloat64(result.handle, index)
		return value, operationError("read floating-point value", code)
	case TypeDecimal:
		return Decimal(text), nil
	case TypeDate:
		value, err := time.Parse("2006-01-02", text)
		if err != nil {
			return text, nil
		}
		return value, nil
	case TypeDateTime, TypeTimestamp:
		value, err := time.ParseInLocation("2006-01-02 15:04:05", text, time.Local)
		if err != nil {
			return text, nil
		}
		return value, nil
	case TypeVarchar:
		return text, nil
	default:
		return nil, fmt.Errorf("seekdb: unsupported column type %d", typeID)
	}
}
