package hive

import (
	"github.com/pkg/errors"
)

type errorStackTracer interface {
	StackTrace() errors.StackTrace
}

//adds a stack trace if not already present
func WithStack(err error) error {
	if _, ok := err.(errorStackTracer); ok {
		return err
	}
	// newError := errors.WithStack(err)
	// fmt.Printf("%+v\n", newError)
	return errors.WithStack(err)
}
