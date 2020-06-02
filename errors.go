package storeql

import (
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type PqErr struct {
	err error
}

func pqErr(err error) *PqErr {
	return &PqErr{err: err}
}

func (err *PqErr) Is(errChecker func(error) *pq.Error) bool {
	if err == nil {
		return false
	}
	return errChecker(errors.Cause(err.err)) != nil
}

func (err *PqErr) Error() string {
	return err.err.Error()
}

var ErrNoId = errors.New("storable entity given has no id")
