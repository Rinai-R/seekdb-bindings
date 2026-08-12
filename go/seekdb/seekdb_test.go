package seekdb

import (
	"context"
	"errors"
	"testing"
)

func TestOpenAndQuery(t *testing.T) {
	instance, err := Open(t.TempDir(),
		WithParameter("memory_limit", "1G"),
		WithParameter("log_disk_size", "2G"),
	)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() {
		if err := instance.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	options, err := instance.ConnectionOptions()
	if err != nil {
		t.Fatalf("ConnectionOptions() error = %v", err)
	}
	if options.User != "root" {
		t.Fatalf("ConnectionOptions().User = %q, want root", options.User)
	}
	if options.Transport == "" {
		t.Fatal("ConnectionOptions().Transport is empty")
	}

	database, err := instance.OpenDB(context.Background(), "")
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer database.Close()

	var value int
	if err := database.QueryRowContext(context.Background(), "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if value != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", value)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	instance, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := instance.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := instance.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := instance.ConnectionOptions(); !errors.Is(err, ErrClosed) {
		t.Fatalf("ConnectionOptions() error = %v, want ErrClosed", err)
	}
}

func TestOpenRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		options []Option
	}{
		{name: "empty path"},
		{name: "empty parameter key", path: t.TempDir(), options: []Option{WithParameter("", "value")}},
		{name: "NUL in parameter", path: t.TempDir(), options: []Option{WithParameter("key", "bad\x00value")}},
		{name: "zero TCP port", path: t.TempDir(), options: []Option{WithTCPPort(0)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instance, err := Open(test.path, test.options...)
			if instance != nil {
				instance.Close()
			}
			if err == nil {
				t.Fatal("Open() error = nil, want error")
			}
		})
	}
}
