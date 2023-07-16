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

func (m *Error[T]) Error() string {
	var msg string
	for k, v := range m.errors {
		msg += fmt.Sprintf("%v:%s; ", k, v)
	}

	return strings.TrimRight(msg, "; ")
}

func (m *Error[T]) Unwrap() []error {
	errs := make([]error, 0, len(m.errors))
	for _, v := range m.errors {
		errs = append(errs, v)
	}

	return errs
}

func (m *Error[T]) Len() int {
	return len(m.errors)
}

func (m *Error[T]) Add(key T, err error) {
	m.mu.Lock()
	m.errors[key] = err
	m.mu.Unlock()
}

func (m *Error[T]) Get(key T) (error, bool) {
	if v := m.errors[key]; v != nil {
		return v, true
	}

	return nil, false
}

func (m *Error[T]) First() error {
	if len(m.errors) == 0 {
		return nil
	}

	for _, v := range m.errors {
		return v
	}

	return nil
}

func (m *Error[T]) Combined() error {
	if len(m.errors) == 0 {
		return nil
	}

	return m
}
