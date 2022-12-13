package multierror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiError_Error(t *testing.T) {
	m := New[string]()
	m.Add("1", errors.New("error1"))
	m.Add("2", errors.New("error2"))
	assert.Equal(t, "1:error1; 2:error2", m.Error())
}

func TestMultiError_ToError(t *testing.T) {
	m := New[string]()
	assert.Nil(t, m.Ret())
	m.Add("1", errors.New("error"))
	assert.NotNil(t, m.Ret())
}
