package multierror

import (
	"fmt"
	"strings"
	"sync"
)

// Error is a generic error type that allows to combine multiple errors into one.
// It is useful when you need to return multiple errors from a function.
type Error[T comparable] struct {
	mu     sync.Mutex
	errors map[T]error
}

// New creates a new Error.
func New[T comparable]() *Error[T] {
	return &Error[T]{
		errors: make(map[T]error),
	}
}

// Error returns a string representation of the error.
func (m *Error[T]) Error() string {
	var msg string
	for k, v := range m.errors {
		msg += fmt.Sprintf("%v:%s; ", k, v)
	}

	return strings.TrimRight(msg, "; ")
}

// Unwrap returns a slice of errors.
func (m *Error[T]) Unwrap() []error {
	errs := make([]error, 0, len(m.errors))
	for _, v := range m.errors {
		errs = append(errs, v)
	}

	return errs
}

// Len returns the number of errors.
func (m *Error[T]) Len() int {
	return len(m.errors)
}

// Add adds an error to the Error.
func (m *Error[T]) Add(key T, err error) {
	m.mu.Lock()
	m.errors[key] = err
	m.mu.Unlock()
}

// Get returns an error by key.
func (m *Error[T]) Get(key T) (error, bool) {
	if v := m.errors[key]; v != nil {
		return v, true
	}

	return nil, false
}

// First returns the first error.
func (m *Error[T]) First() error {
	if len(m.errors) == 0 {
		return nil
	}

	for _, v := range m.errors {
		return v
	}

	return nil
}

// Combined returns the Error if it contains any errors, nil otherwise.
func (m *Error[T]) Combined() error {
	if len(m.errors) == 0 {
		return nil
	}

	return m
}
