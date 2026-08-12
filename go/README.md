# Go binding

The Go binding exposes both the native `libseekdb` API and a `database/sql`
connection for Go database tooling.

## Native API

```go
instance, err := seekdb.Open("./seekdb.db")
if err != nil {
	log.Fatal(err)
}
defer instance.Close()

connection, err := instance.Connect("test", false)
if err != nil {
	log.Fatal(err)
}
defer connection.Close()

result, err := connection.Query("SELECT 1")
if err != nil {
	log.Fatal(err)
}
defer result.Close()

for result.Next() {
	values, err := result.Values()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(values)
}
if err := result.Err(); err != nil {
	log.Fatal(err)
}
```

Native connections retain the instance state they use, so they remain valid
after their originating `Instance` value is closed.

## database/sql and GORM

```go
database, err := instance.OpenDB(context.Background(), "test")
if err != nil {
	log.Fatal(err)
}
defer database.Close()

gormDatabase, err := gorm.Open(mysql.New(mysql.Config{Conn: database}), &gorm.Config{})
```

Close every external `database/sql` connection before closing its `Instance`.
On Windows, `OpenDB` currently returns `ErrNamedPipeUnsupported`; the native
`Connect` API supports the transport exposed by `libseekdb`.

## Native SDK

The package links to `libseekdb` through cgo. Make the shared library visible
to the compiler and runtime. The `seekdb` executable must remain next to the
shared library, matching the existing SDK package layout.
