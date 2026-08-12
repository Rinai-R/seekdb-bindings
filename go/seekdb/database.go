package seekdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"

	"github.com/go-sql-driver/mysql"
)

// ErrNamedPipeUnsupported reports that go-sql-driver/mysql cannot use the
// named-pipe endpoint returned by libseekdb on Windows.
var ErrNamedPipeUnsupported = errors.New("seekdb: database/sql does not support the named-pipe transport")

// OpenDB opens and verifies a database/sql pool for this Instance. Callers
// must close the returned DB before closing the Instance.
func (instance *Instance) OpenDB(ctx context.Context, databaseName string) (*sql.DB, error) {
	options, err := instance.ConnectionOptions()
	if err != nil {
		return nil, err
	}

	config := mysql.NewConfig()
	config.User = options.User
	config.DBName = databaseName
	config.ParseTime = true
	switch options.Transport {
	case TransportTCP:
		config.Net = "tcp"
		config.Addr = net.JoinHostPort("127.0.0.1", fmt.Sprint(options.Port))
	case TransportUnixSocket:
		config.Net = "unix"
		config.Addr = options.Endpoint
	case TransportNamedPipe:
		return nil, ErrNamedPipeUnsupported
	default:
		return nil, fmt.Errorf("seekdb: unsupported connection transport %q", options.Transport)
	}

	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, fmt.Errorf("seekdb: create database/sql connector: %w", err)
	}
	database := sql.OpenDB(connector)
	if err := database.PingContext(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("seekdb: ping database/sql connection: %w", err)
	}
	return database, nil
}
