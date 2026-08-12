package seekdb

import (
	"errors"
	"testing"
)

func TestClosedNilHandles(t *testing.T) {
	var instance *Instance
	if !instance.Closed() {
		t.Fatal("nil Instance.Closed() = false, want true")
	}
	if err := instance.Close(); err != nil {
		t.Fatalf("nil Instance.Close() error = %v", err)
	}
	if _, err := instance.ConnectionOptions(); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil Instance.ConnectionOptions() error = %v, want ErrClosed", err)
	}

	var connection *Connection
	if !connection.Closed() {
		t.Fatal("nil Connection.Closed() = false, want true")
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("nil Connection.Close() error = %v", err)
	}
	if _, err := connection.Query("SELECT 1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil Connection.Query() error = %v, want ErrClosed", err)
	}

	var result *Result
	if err := result.Close(); err != nil {
		t.Fatalf("nil Result.Close() error = %v", err)
	}
	if err := result.Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil Result.Err() = %v, want ErrClosed", err)
	}
}

func TestWithParameterUsesLastValue(t *testing.T) {
	config := openConfig{}
	options := []Option{
		WithParameter("memory_limit", "1G"),
		WithParameter("log_disk_size", "2G"),
		WithParameter("memory_limit", "3G"),
	}
	for _, option := range options {
		if err := option(&config); err != nil {
			t.Fatalf("Option() error = %v", err)
		}
	}

	want := []parameter{
		{key: "memory_limit", value: "3G"},
		{key: "log_disk_size", value: "2G"},
	}
	if len(config.parameters) != len(want) {
		t.Fatalf("parameter count = %d, want %d", len(config.parameters), len(want))
	}
	for index := range want {
		if config.parameters[index] != want[index] {
			t.Errorf("parameter %d = %#v, want %#v", index, config.parameters[index], want[index])
		}
	}
}

func TestOpenRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name      string
		directory string
		options   []Option
	}{
		{name: "empty directory"},
		{name: "NUL in directory", directory: "bad\x00directory"},
		{name: "empty parameter key", directory: t.TempDir(), options: []Option{WithParameter("", "value")}},
		{name: "NUL in parameter key", directory: t.TempDir(), options: []Option{WithParameter("bad\x00key", "value")}},
		{name: "NUL in parameter value", directory: t.TempDir(), options: []Option{WithParameter("key", "bad\x00value")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instance, err := Open(test.directory, test.options...)
			if instance != nil {
				instance.Close()
			}
			if err == nil {
				t.Fatal("Open() error = nil, want error")
			}
		})
	}
}

func TestConnectionRejectsNUL(t *testing.T) {
	instance := &Instance{}
	if _, err := instance.Connect("bad\x00database", true); err == nil {
		t.Fatal("Connect() error = nil, want error")
	}
	connection := &Connection{}
	if _, err := connection.Query("bad\x00query"); err == nil {
		t.Fatal("Query() error = nil, want error")
	}
}
