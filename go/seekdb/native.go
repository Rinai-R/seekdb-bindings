package seekdb

/*
#cgo CFLAGS: -I${SRCDIR}/../../lib/include
#cgo LDFLAGS: -lseekdb

#include <stdlib.h>
#include "seekdb.h"
*/
import "C"

import "unsafe"

type nativeInstance struct{ pointer unsafe.Pointer }
type nativeConnection struct{ pointer unsafe.Pointer }
type nativeResult struct{ pointer unsafe.Pointer }

func nativeOpen(directory string, parameters []parameter) (nativeInstance, Code) {
	cDirectory := C.CString(directory)
	defer C.free(unsafe.Pointer(cDirectory))

	cParameters, release := makeCParameters(parameters)
	defer release()

	var handle C.SeekdbHandle
	code := Code(C.seekdb_open(cDirectory, cParameters, &handle))
	return nativeInstance{pointer: unsafe.Pointer(handle)}, code
}

func nativeClose(handle nativeInstance) Code {
	return Code(C.seekdb_close(C.SeekdbHandle(handle.pointer)))
}

func nativeConnectionOptions(handle nativeInstance) (ConnectionOptions, Code) {
	var options C.SeekdbConnectionOptions
	code := Code(C.seekdb_connection_options(C.SeekdbHandle(handle.pointer), &options))
	if code != Success {
		return ConnectionOptions{}, code
	}

	result := ConnectionOptions{Port: uint16(options.port)}
	if options.transport != nil {
		result.Transport = Transport(C.GoString(options.transport))
	}
	if options.endpoint != nil {
		result.Endpoint = C.GoString(options.endpoint)
	}
	if options.user != nil {
		result.User = C.GoString(options.user)
	}
	return result, Success
}

func nativeConnect(handle nativeInstance, database string, autocommit bool) (nativeConnection, *Error) {
	var cDatabase *C.char
	if database != "" {
		cDatabase = C.CString(database)
		defer C.free(unsafe.Pointer(cDatabase))
	}

	var connection C.SeekdbConnection
	code := Code(C.seekdb_connect(
		C.SeekdbHandle(handle.pointer),
		cDatabase,
		C.bool(autocommit),
		&connection,
	))
	result := nativeConnection{pointer: unsafe.Pointer(connection)}
	if code == Success {
		return result, nil
	}

	err := nativeError("connect", code, result)
	if result.pointer != nil {
		C.seekdb_disconnect(connection)
	}
	return nativeConnection{}, err
}

func nativeDisconnect(connection nativeConnection) Code {
	return Code(C.seekdb_disconnect(C.SeekdbConnection(connection.pointer)))
}

func nativeQuery(connection nativeConnection, query string) (nativeResult, *Error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var result C.SeekdbResult
	code := Code(C.seekdb_query(
		C.SeekdbConnection(connection.pointer),
		cQuery,
		C.int64_t(len(query)),
		&result,
	))
	if code != Success {
		return nativeResult{}, nativeError("query", code, connection)
	}
	return nativeResult{pointer: unsafe.Pointer(result)}, nil
}

func nativeBegin(connection nativeConnection) *Error {
	code := Code(C.seekdb_trx_begin(C.SeekdbConnection(connection.pointer)))
	return nativeError("begin transaction", code, connection)
}

func nativeCommit(connection nativeConnection) *Error {
	code := Code(C.seekdb_trx_commit(C.SeekdbConnection(connection.pointer)))
	return nativeError("commit transaction", code, connection)
}

func nativeRollback(connection nativeConnection) *Error {
	code := Code(C.seekdb_trx_rollback(C.SeekdbConnection(connection.pointer)))
	return nativeError("rollback transaction", code, connection)
}

func nativeError(operation string, code Code, connection nativeConnection) *Error {
	if code == Success {
		return nil
	}

	err := &Error{Operation: operation, Code: code}
	if connection.pointer == nil {
		return err
	}

	var serverCode C.int
	var message *C.char
	if Code(C.seekdb_last_error(
		C.SeekdbConnection(connection.pointer),
		&serverCode,
		(**C.char)(unsafe.Pointer(&message)),
	)) == Success {
		err.ServerCode = int(serverCode)
		if message != nil {
			err.Message = C.GoString(message)
		}
	}
	return err
}

func nativeResultClose(result nativeResult) Code {
	return Code(C.seekdb_result_free(C.SeekdbResult(result.pointer)))
}

func nativeResultColumnCount(result nativeResult) (int, Code) {
	var count C.int64_t
	code := Code(C.seekdb_result_column_count(C.SeekdbResult(result.pointer), &count))
	return int(count), code
}

func nativeResultColumnName(result nativeResult, index int) (string, Code) {
	var name *C.char
	code := Code(C.seekdb_result_column_name(
		C.SeekdbResult(result.pointer),
		C.int64_t(index),
		(**C.char)(unsafe.Pointer(&name)),
	))
	if code != Success || name == nil {
		return "", code
	}
	return C.GoString(name), Success
}

func nativeResultColumnType(result nativeResult, index int) (TypeID, Code) {
	var typeID C.SeekdbTypeId
	code := Code(C.seekdb_result_column_type_id(
		C.SeekdbResult(result.pointer),
		C.int64_t(index),
		&typeID,
	))
	return TypeID(typeID), code
}

func nativeResultRowCount(result nativeResult) (int64, Code) {
	var count C.int64_t
	code := Code(C.seekdb_result_row_count(C.SeekdbResult(result.pointer), &count))
	return int64(count), code
}

func nativeResultNext(result nativeResult) Code {
	return Code(C.seekdb_result_next(C.SeekdbResult(result.pointer)))
}

func nativeResultInt64(result nativeResult, index int) (int64, Code) {
	var value C.int64_t
	code := Code(C.seekdb_result_get_int64(
		C.SeekdbResult(result.pointer),
		C.int64_t(index),
		&value,
	))
	return int64(value), code
}

func nativeResultUint64(result nativeResult, index int) (uint64, Code) {
	var value C.uint64_t
	code := Code(C.seekdb_result_get_uint64(
		C.SeekdbResult(result.pointer),
		C.int64_t(index),
		&value,
	))
	return uint64(value), code
}

func nativeResultFloat64(result nativeResult, index int) (float64, Code) {
	var value C.double
	code := Code(C.seekdb_result_get_float(
		C.SeekdbResult(result.pointer),
		C.int64_t(index),
		&value,
	))
	return float64(value), code
}

func nativeResultString(result nativeResult, index int) (string, bool, Code) {
	var data *C.char
	var length C.size_t
	var isNull C.int
	code := Code(C.seekdb_result_get_str(
		C.SeekdbResult(result.pointer),
		C.int64_t(index),
		(**C.char)(unsafe.Pointer(&data)),
		&length,
		&isNull,
	))
	if code != Success || isNull != 0 {
		return "", isNull != 0, code
	}
	return C.GoStringN(data, C.int(length)), false, Success
}

func makeCParameters(parameters []parameter) (**C.char, func()) {
	if len(parameters) == 0 {
		return nil, func() {}
	}

	values := make([]*C.char, len(parameters)*2+1)
	for index, parameter := range parameters {
		values[index*2] = C.CString(parameter.key)
		values[index*2+1] = C.CString(parameter.value)
	}
	return (**C.char)(unsafe.Pointer(&values[0])), func() {
		for _, value := range values[:len(values)-1] {
			C.free(unsafe.Pointer(value))
		}
	}
}
