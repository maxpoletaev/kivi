package skiplist

import (
	"math/rand"
	"sync"
	"testing"
)

// TestSkiplistStress_ConcurrentGetInsert conurrently inserts and reads 10 keys from a 500 goroutines.
// It then ensures that all keys are present, and no data-races occur. This test is intended
// to be run with the -race flag.
func TestSkiplistStress_ConcurrentGetInsert(t *testing.T) {
	const (
		concurrency = 500
		numKeys     = 10
	)

	wg := sync.WaitGroup{}
	wg.Add(concurrency * 2)

	run := make(chan bool)
	usedKeys := map[int]struct{}{}
	list := New[int, bool](IntComparator)

	// Simulate concurrent reads and writes.
	for i := 0; i < concurrency; i++ {
		key := rand.Intn(numKeys) + 1
		usedKeys[key] = struct{}{}

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
	for key := range usedKeys {
		if _, ok := list.Get(key); !ok {
			t.Errorf("key %d not found", key)
		}
	}
}
