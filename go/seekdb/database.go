package seekdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"

	"github.com/go-sql-driver/mysql"
)

// ErrNamedPipeUnsupported reports that go-sql-driver/mysql cannot connect to
// seekdb's Windows named pipe. Open the Instance with WithTCPPort on Windows.
var ErrNamedPipeUnsupported = errors.New("seekdb: database/sql does not support the named-pipe transport; open the instance with WithTCPPort")

// OpenDB opens and verifies a database/sql connection to the Instance.
// Callers must close the returned DB before closing the Instance.
func (instance *Instance) OpenDB(ctx context.Context, database string) (*sql.DB, error) {
	options, err := instance.ConnectionOptions()
	if err != nil {
		return nil, err
	}

	config := mysql.NewConfig()
	config.User = options.User
	config.DBName = database
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
		return nil, fmt.Errorf("seekdb: create MySQL connector: %w", err)
	}
	databaseHandle := sql.OpenDB(connector)
	if err := databaseHandle.PingContext(ctx); err != nil {
		databaseHandle.Close()
		return nil, fmt.Errorf("seekdb: ping embedded instance: %w", err)
	}
	return databaseHandle, nil
}
