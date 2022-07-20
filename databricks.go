package dbsql

import (
	"database/sql"
	"io"
	"io/ioutil"
	"time"
)

func init() {
	sql.Register("databricks", &Driver{})
}

// Options for driver connection
type Options struct {
	Host     string
	Port     string
	Token    string
	HTTPPath string
	MaxRows  int64
	Timeout  int
	Loc      *time.Location

	LogOut io.Writer
}

func (o *Options) Equal(o2 *Options) bool {
	return o.Host == o2.Host &&
		o.Port == o2.Port &&
		o.Token == o2.Token &&
		o.HTTPPath == o2.HTTPPath &&
		o.MaxRows == o2.MaxRows &&
		o.Timeout == o2.Timeout &&
		o.Loc.String() == o2.Loc.String()
}

var (
	// DefaultOptions for the driver
	DefaultOptions = Options{Port: "443", MaxRows: 10000, LogOut: ioutil.Discard}
)
