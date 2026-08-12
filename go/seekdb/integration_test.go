package seekdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestNativeLifecycleQueryAndTransactions(t *testing.T) {
	directory := shortTempDir(t)
	openDirectory := filepath.Join(directory, "missing", "..")
	instance := openTestInstance(t, openDirectory, WithParameter("memory_limit", "1G"))
	if instance.DataDirectory() != directory {
		t.Fatalf("DataDirectory() = %q, want %q", instance.DataDirectory(), directory)
	}

	duplicate := openTestInstance(t, directory)
	options, err := instance.ConnectionOptions()
	if err != nil {
		t.Fatalf("ConnectionOptions() error = %v", err)
	}
	if options.User != "root" {
		t.Errorf("ConnectionOptions().User = %q, want root", options.User)
	}
	if options.Transport == "" {
		t.Fatal("ConnectionOptions().Transport is empty")
	}

	connection := connectTest(t, instance, "test", true)
	if err := duplicate.Close(); err != nil {
		t.Fatalf("duplicate.Close() error = %v", err)
	}
	if err := instance.Close(); err != nil {
		t.Fatalf("Instance.Close() error = %v", err)
	}
	if !instance.Closed() || !duplicate.Closed() {
		t.Fatal("closed instances did not report Closed()")
	}

	testNativeValues(t, connection)
	testNativeTransactions(t, connection)
	testNativeHybridSearch(t, connection)

	if _, err := connection.Query("SELECT FROM"); err == nil {
		t.Fatal("invalid query error = nil")
	} else {
		var seekdbError *Error
		if !errors.As(err, &seekdbError) {
			t.Fatalf("invalid query error type = %T, want *Error", err)
		}
		if seekdbError.Message == "" {
			t.Errorf("invalid query error = %#v, want server message", seekdbError)
		}
	}

	if err := connection.Close(); err != nil {
		t.Fatalf("Connection.Close() error = %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("second Connection.Close() error = %v", err)
	}
	if !connection.Closed() {
		t.Fatal("Connection.Closed() = false after Close")
	}
}

func TestMultipleInstancesAreIsolated(t *testing.T) {
	root := shortTempDir(t)
	directories := []string{filepath.Join(root, "first"), filepath.Join(root, "second")}

	type openResult struct {
		instance *Instance
		err      error
	}
	results := make(chan openResult, len(directories))
	var waitGroup sync.WaitGroup
	for _, directory := range directories {
		directory := directory
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			instance, err := Open(directory)
			results <- openResult{instance: instance, err: err}
		}()
	}
	waitGroup.Wait()
	close(results)

	instancesByDirectory := make(map[string]*Instance)
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent Open() error = %v", result.err)
		}
		instance := result.instance
		t.Cleanup(func() { instance.Close() })
		instancesByDirectory[instance.DataDirectory()] = instance
	}

	first := connectTest(t, instancesByDirectory[directories[0]], "test", true)
	second := connectTest(t, instancesByDirectory[directories[1]], "test", true)
	execNative(t, first, "CREATE TABLE instance_marker(value INT)")
	execNative(t, first, "INSERT INTO instance_marker VALUES (1)")
	execNative(t, second, "CREATE TABLE instance_marker(value INT)")
	execNative(t, second, "INSERT INTO instance_marker VALUES (2)")
	if got := querySingleInt(t, first, "SELECT value FROM instance_marker"); got != 1 {
		t.Errorf("first marker = %d, want 1", got)
	}
	if got := querySingleInt(t, second, "SELECT value FROM instance_marker"); got != 2 {
		t.Errorf("second marker = %d, want 2", got)
	}
}

func TestDatabaseSQLAndGORM(t *testing.T) {
	instance := openTestInstance(t, shortTempDir(t))
	ctx := testContext(t)
	database, err := instance.OpenDB(ctx, "test")
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	t.Cleanup(func() { database.Close() })

	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	if _, err := transaction.ExecContext(ctx, "CREATE TABLE sql_marker(id INT PRIMARY KEY, value VARCHAR(32))"); err != nil {
		transaction.Rollback()
		t.Fatalf("create sql_marker: %v", err)
	}
	if _, err := transaction.ExecContext(ctx, "INSERT INTO sql_marker VALUES (?, ?)", 1, "database/sql"); err != nil {
		transaction.Rollback()
		t.Fatalf("insert sql_marker: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	var value string
	if err := database.QueryRowContext(ctx, "SELECT value FROM sql_marker WHERE id = ?", 1).Scan(&value); err != nil {
		t.Fatalf("parameterized query: %v", err)
	}
	if value != "database/sql" {
		t.Errorf("database/sql value = %q, want database/sql", value)
	}

	gormDatabase, err := gorm.Open(gormmysql.New(gormmysql.Config{
		Conn:                      database,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	if err := gormDatabase.AutoMigrate(&gormRecord{}); err != nil {
		t.Fatalf("GORM AutoMigrate() error = %v", err)
	}
	want := gormRecord{ID: 7, Name: "seekdb"}
	if err := gormDatabase.Create(&want).Error; err != nil {
		t.Fatalf("GORM Create() error = %v", err)
	}
	var got gormRecord
	if err := gormDatabase.First(&got, want.ID).Error; err != nil {
		t.Fatalf("GORM First() error = %v", err)
	}
	if got != want {
		t.Errorf("GORM record = %#v, want %#v", got, want)
	}
}

func TestParametersAndReopenPersistence(t *testing.T) {
	directory := shortTempDir(t)
	first, err := Open(directory, WithParameter("memory_limit", "1G"))
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	firstConnection, err := first.Connect("test", true)
	if err != nil {
		first.Close()
		t.Fatalf("first Connect() error = %v", err)
	}
	if got := readParameter(t, firstConnection, "memory_limit"); !strings.Contains(got, "1G") {
		t.Errorf("initial memory_limit = %q, want value containing 1G", got)
	}
	execNative(t, firstConnection, "CREATE TABLE reopen_marker(value INT)")
	execNative(t, firstConnection, "INSERT INTO reopen_marker VALUES (42)")
	if err := firstConnection.Close(); err != nil {
		t.Fatalf("first Connection.Close() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Instance.Close() error = %v", err)
	}

	second := openTestInstance(t, directory, WithParameter("memory_limit", "2G"))
	secondConnection := connectTest(t, second, "test", true)
	if got := querySingleInt(t, secondConnection, "SELECT value FROM reopen_marker"); got != 42 {
		t.Errorf("reopened marker = %d, want 42", got)
	}
	if got := readParameter(t, secondConnection, "memory_limit"); !strings.Contains(got, "1G") {
		t.Errorf("reopened memory_limit = %q, want persisted value containing 1G", got)
	}
}

func TestOpenRejectsInvalidNativeParameter(t *testing.T) {
	instance, err := Open(shortTempDir(t), WithParameter("port", "not-a-number"))
	if instance != nil {
		instance.Close()
	}
	if err == nil {
		t.Fatal("Open() error = nil, want invalid native parameter error")
	}
	var seekdbError *Error
	if !errors.As(err, &seekdbError) || seekdbError.Code != InvalidArgument {
		t.Fatalf("Open() error = %v, want InvalidArgument *Error", err)
	}
}

type gormRecord struct {
	ID   uint `gorm:"primaryKey"`
	Name string
}

func (gormRecord) TableName() string { return "go_binding_gorm_records" }

func testNativeValues(t *testing.T, connection *Connection) {
	t.Helper()
	execNative(t, connection, "DROP TABLE IF EXISTS go_binding_values")
	execNative(t, connection, `CREATE TABLE go_binding_values (
		signed_value BIGINT,
		unsigned_value BIGINT UNSIGNED,
		float_value DOUBLE,
		decimal_value DECIMAL(10,2),
		date_value DATE,
		datetime_value DATETIME,
		timestamp_value TIMESTAMP,
		text_value VARCHAR(32),
		null_value VARCHAR(32) NULL
	)`)
	execNative(t, connection, `INSERT INTO go_binding_values VALUES (
		-7, 7, 1.5, 12.34, '2026-08-12', '2026-08-12 12:34:56',
		'2026-08-12 12:34:56', CONCAT('a', CHAR(0), 'b'), NULL
	)`)

	result := queryNative(t, connection, `SELECT signed_value, unsigned_value, float_value,
		decimal_value, date_value, datetime_value, timestamp_value, text_value, null_value
		FROM go_binding_values`)
	columns, err := result.Columns()
	if err != nil {
		t.Fatalf("Columns() error = %v", err)
	}
	if len(columns) != 9 || columns[0].Name != "signed_value" {
		t.Fatalf("Columns() = %#v, want 9 columns beginning with signed_value", columns)
	}
	rowCount, err := result.RowCount()
	if err != nil {
		t.Fatalf("RowCount() error = %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("RowCount() = %d, want 1", rowCount)
	}
	if !result.Next() {
		t.Fatalf("Next() = false, error = %v", result.Err())
	}
	values, err := result.Values()
	if err != nil {
		t.Fatalf("Values() error = %v", err)
	}
	if values[0] != int64(-7) || values[1] != uint64(7) || values[2] != float64(1.5) {
		t.Errorf("numeric values = %#v", values[:3])
	}
	if values[3] != Decimal("12.34") {
		t.Errorf("decimal value = %#v, want Decimal(12.34)", values[3])
	}
	for index, name := range map[int]string{4: "date", 5: "datetime", 6: "timestamp"} {
		if _, ok := values[index].(time.Time); !ok {
			t.Errorf("%s value = %#v (%T), want time.Time", name, values[index], values[index])
		}
	}
	if values[7] != "a\x00b" || values[8] != nil {
		t.Errorf("string/null values = %#v, %#v", values[7], values[8])
	}
	if result.Next() {
		t.Fatal("second Next() = true, want false")
	}
	if err := result.Err(); err != nil {
		t.Fatalf("Result.Err() = %v", err)
	}
}

func testNativeTransactions(t *testing.T, connection *Connection) {
	t.Helper()
	execNative(t, connection, "DROP TABLE IF EXISTS go_binding_transactions")
	execNative(t, connection, "CREATE TABLE go_binding_transactions(value INT)")
	if err := connection.Begin(); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	execNative(t, connection, "INSERT INTO go_binding_transactions VALUES (1)")
	if err := connection.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if got := querySingleInt(t, connection, "SELECT COUNT(*) FROM go_binding_transactions"); got != 0 {
		t.Fatalf("row count after rollback = %d, want 0", got)
	}
	if err := connection.Begin(); err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	execNative(t, connection, "INSERT INTO go_binding_transactions VALUES (2)")
	if err := connection.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got := querySingleInt(t, connection, "SELECT value FROM go_binding_transactions"); got != 2 {
		t.Fatalf("committed value = %d, want 2", got)
	}
}

func testNativeHybridSearch(t *testing.T, connection *Connection) {
	t.Helper()
	statements := []string{
		"DROP TABLE IF EXISTS go_binding_docs",
		`CREATE TABLE go_binding_docs (
			id INT,
			vector VECTOR(3),
			query VARCHAR(255),
			content VARCHAR(255),
			VECTOR INDEX vector_idx(vector) WITH (distance=l2, type=hnsw, lib=vsag),
			FULLTEXT query_idx(query),
			FULLTEXT content_idx(content)
		)`,
		`INSERT INTO go_binding_docs VALUES
			(1, '[1,2,3]', 'hello world', 'oceanbase Elasticsearch database'),
			(2, '[1,2,1]', 'hello world, what is your name', 'oceanbase mysql database'),
			(3, '[1,1,1]', 'hello world, how are you', 'oceanbase oracle database'),
			(4, '[1,3,1]', 'real world, where are you from', 'postgres oracle database'),
			(5, '[1,3,2]', 'real world, how old are you', 'redis oracle database'),
			(6, '[2,1,1]', 'hello world, where are you from', 'starrocks oceanbase database')`,
		`SET @go_binding_search = '{
			"query":{"bool":{"should":[
				{"match":{"query":"hi hello"}},
				{"match":{"content":"oceanbase mysql"}}
			]}},
			"knn":{"field":"vector","k":5,"query_vector":[1,2,3]},
			"_source":["query","content","_keyword_score","_semantic_score"]
		}'`,
	}
	for _, statement := range statements {
		execNative(t, connection, statement)
	}
	result := queryNative(t, connection,
		"SELECT JSON_PRETTY(DBMS_HYBRID_SEARCH.SEARCH('go_binding_docs', @go_binding_search))")
	if !result.Next() {
		t.Fatalf("hybrid search Next() = false, error = %v", result.Err())
	}
	value, err := result.Value(0)
	if err != nil {
		t.Fatalf("hybrid search Value() error = %v", err)
	}
	if !strings.Contains(fmt.Sprint(value), "oceanbase") {
		t.Errorf("hybrid search result does not contain oceanbase: %v", value)
	}
}

func execNative(t *testing.T, connection *Connection, query string) {
	t.Helper()
	if err := connection.Exec(query); err != nil {
		t.Fatalf("Exec() error = %v\nSQL: %s", err, query)
	}
}

func queryNative(t *testing.T, connection *Connection, query string) *Result {
	t.Helper()
	result, err := connection.Query(query)
	if err != nil {
		t.Fatalf("Query() error = %v\nSQL: %s", err, query)
	}
	t.Cleanup(func() { result.Close() })
	return result
}

func querySingleInt(t *testing.T, connection *Connection, query string) int64 {
	t.Helper()
	result := queryNative(t, connection, query)
	if !result.Next() {
		t.Fatalf("Next() = false, error = %v", result.Err())
	}
	value, err := result.Value(0)
	if err != nil {
		t.Fatalf("Value(0) error = %v", err)
	}
	integer, ok := value.(int64)
	if !ok {
		t.Fatalf("Value(0) = %#v (%T), want int64", value, value)
	}
	return integer
}

func readParameter(t *testing.T, connection *Connection, name string) string {
	t.Helper()
	result, err := connection.Query("SHOW PARAMETERS LIKE '" + strings.ReplaceAll(name, "'", "''") + "'")
	if err != nil {
		t.Fatalf("query parameter %q: %v", name, err)
	}
	defer result.Close()

	columns, err := result.Columns()
	if err != nil {
		t.Fatalf("parameter Columns() error = %v", err)
	}
	nameIndex, valueIndex := -1, -1
	for index, column := range columns {
		switch strings.ToLower(column.Name) {
		case "name":
			nameIndex = index
		case "value":
			valueIndex = index
		}
	}
	if nameIndex < 0 || valueIndex < 0 {
		t.Fatalf("parameter columns = %#v, want name and value", columns)
	}
	for result.Next() {
		values, err := result.Values()
		if err != nil {
			t.Fatalf("parameter Values() error = %v", err)
		}
		if fmt.Sprint(values[nameIndex]) == name {
			return fmt.Sprint(values[valueIndex])
		}
	}
	if err := result.Err(); err != nil {
		t.Fatalf("iterate parameter rows: %v", err)
	}
	t.Fatalf("parameter %q not found", name)
	return ""
}

func openTestInstance(t *testing.T, directory string, options ...Option) *Instance {
	t.Helper()
	instance, err := Open(directory, options...)
	if err != nil {
		t.Fatalf("Open(%q) error = %v", directory, err)
	}
	t.Cleanup(func() {
		if err := instance.Close(); err != nil {
			t.Errorf("Instance.Close() error = %v", err)
		}
	})
	return instance
}

func connectTest(t *testing.T, instance *Instance, database string, autocommit bool) *Connection {
	t.Helper()
	connection, err := instance.Connect(database, autocommit)
	if err != nil {
		t.Fatalf("Connect(%q) error = %v", database, err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("Connection.Close() error = %v", err)
		}
	})
	return connection
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("", "seekdb-go-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("RemoveAll(%q) error = %v", directory, err)
		}
	})
	return directory
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}
