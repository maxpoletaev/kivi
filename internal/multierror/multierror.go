package multierror

import (
	"fmt"
	"strings"
	"sync"
)

type Error[T comparable] struct {
	mu     sync.Mutex
	errors map[T]error
}

func New[T comparable]() *Error[T] {
	return &Error[T]{
		errors: make(map[T]error),
	}
}

func (me *Error[T]) Error() string {
	var msg string
	for k, v := range me.errors {
		msg += fmt.Sprintf("%v:%s; ", k, v)
	}

	return strings.TrimRight(msg, "; ")
}

func (me *Error[T]) Unwrap() []error {
	errs := make([]error, 0, len(me.errors))
	for _, v := range me.errors {
		errs = append(errs, v)
	}

	return errs
}

func (me *Error[T]) Len() int {
	return len(me.errors)
}

func (me *Error[T]) Add(key T, err error) {
	me.mu.Lock()
	me.errors[key] = err
	me.mu.Unlock()
}

func (me *Error[T]) Get(key T) (error, bool) {
	if v := me.errors[key]; v != nil {
		return v, true
	}

	return nil, false
}

func (me *Error[T]) Ret() error {
	if len(me.errors) == 0 {
		return nil
	}

	return me
}
