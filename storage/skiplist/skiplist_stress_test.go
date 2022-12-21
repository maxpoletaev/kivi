package skiplist

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/maxpoletaev/kv/internal/set"
)

// TestSkiplist_Concurrency conurrently inserts and reads 10 keys from a 100 goroutines.
// It then ensures that all keys are present, and no data-races occur. This test is intended
// to be run with the -race flag.
func TestSkiplistStress(t *testing.T) {
	const concurrency = 1000
	const numKeys = 100

	wg := sync.WaitGroup{}
	wg.Add(concurrency * 2)

	run := make(chan bool)
	usedKeys := set.New[int]()
	list := New[int, bool](IntComparator)

	// Simulate concurrent reads and writes.
	for i := 0; i < concurrency; i++ {
		key := rand.Intn(numKeys) + 1
		usedKeys.Add(key)

		go func() {
			<-run
			list.Insert(key, true)
			wg.Done()
		}()

		go func() {
			<-run
			list.Get(key)
			wg.Done()
		}()
	}

	close(run)
	wg.Wait()

	validateInternalState(t, list)

	// Ensure all keys are present.
	for _, key := range usedKeys.Values() {
		if _, err := list.Get(key); err != nil {
			t.Errorf("key %d not found", key)
		}
	}
}
