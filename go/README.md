# Go binding

The Go binding owns a local seekdb lifecycle through `libseekdb` and exposes a
verified `database/sql` connection for normal Go database tooling.

## Prerequisites

Install a libseekdb C SDK package and make its headers and library visible to
cgo. The `seekdb` server binary must remain next to the shared library, as in
the official SDK package.

```sh
export CGO_CFLAGS="-I/path/to/libseekdb/include"
export CGO_LDFLAGS="-L/path/to/libseekdb/lib"
export LD_LIBRARY_PATH="/path/to/libseekdb/lib:${LD_LIBRARY_PATH:-}" # Linux
export DYLD_LIBRARY_PATH="/path/to/libseekdb/lib:${DYLD_LIBRARY_PATH:-}" # macOS
```

## Usage

```go
package main

import (
	"context"
	"log"

	"github.com/oceanbase/seekdb-bindings/go/seekdb"
)

func main() {
	instance, err := seekdb.Open("./agent.db")
	if err != nil {
		log.Fatal(err)
	}
	defer instance.Close()

	database, err := instance.OpenDB(context.Background(), "")
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// Use database with database/sql, GORM, sqlc, or another Go SQL tool.
	if _, err := database.Exec(`CREATE DATABASE IF NOT EXISTS agent`); err != nil {
		log.Fatal(err)
	}

	// Defers run in LIFO order, so the SQL connection closes before the
	// lifecycle handle.
}
```

On Windows, `go-sql-driver/mysql` does not support seekdb's named-pipe local
transport. Pass `seekdb.WithTCPPort(port)` to `seekdb.Open` when using
`OpenDB` on Windows.
