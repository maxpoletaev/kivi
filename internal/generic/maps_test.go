package generic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapKeys(t *testing.T) {
	mapA := map[string]bool{"key1": true, "key2": true}
	mapB := map[string]bool{"key2": true, "key3": true}
	keys := MapKeys(mapA, mapB)
	assert.ElementsMatch(t, keys, []string{"key1", "key2", "key3"})
}

func TestMapValues(t *testing.T) {

}

func TestMapCopy(t *testing.T) {
	mapA := map[string]bool{"key1": true, "key2": true}
	mapB := make(map[string]bool)

	MapCopy(mapA, mapB)

	assert.Equal(t, mapA, mapB)
}
