module github.com/databricks/databricks-sql-go

go 1.18

require (
	github.com/apache/thrift v0.12.0
	github.com/pkg/errors v0.9.1
)

replace github.com/apache/thrift v0.12.0 => ./vendor/github.com/apache/thrift
