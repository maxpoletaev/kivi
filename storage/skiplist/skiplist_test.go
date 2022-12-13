package skiplist

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeSkiplist(t *testing.T) {
	list := construct([][]int{
		{1, 2, 3, 4, 5, 6},
		{1, 3, 5},
		{1, 5},
	}, IntComparator, struct{}{})

	assert.Len(t, list.head.next, maxHeight)
	assert.Equal(t, 3, list.Height())
	assert.Equal(t, 6, list.Size())

	// Ensure sequence on the first level
	assert.Equal(t, 1, list.head.next[0].key)
	assert.Equal(t, 2, list.head.next[0].next[0].key)
	assert.Equal(t, 3, list.head.next[0].next[0].next[0].key)

	// Ensure the sequence is correct on the second level
	assert.Equal(t, 1, list.head.next[1].key)
	assert.Equal(t, 3, list.head.next[1].next[1].key)
	assert.Equal(t, 5, list.head.next[1].next[1].next[1].key)

	// Ensure the sequence is correct on the third level.
	assert.Equal(t, 1, list.head.next[2].key)
	assert.Equal(t, 5, list.head.next[2].next[2].key)

	// Ensure thre links are correct between the second and first levels.
	assert.Equal(t, 2, list.head.next[1].next[0].key)
	assert.Equal(t, 4, list.head.next[1].next[1].next[0].key)
	assert.Equal(t, 6, list.head.next[1].next[1].next[1].next[0].key)

	// Ensure thre links are correct between the third and second levels.
	assert.Equal(t, 3, list.head.next[2].next[1].key)
	assert.Nil(t, list.head.next[2].next[2].next[1])
}

func TestMakeSkiplistFails(t *testing.T) {
	assert.PanicsWithValue(t, "invalid list: node 3 is defined at level 0 but missing from level 1", func() {
		construct([][]int{
			{1, 2, 4, 5},
			{1, 3, 4},
		}, IntComparator, false)
	})
}

func TestSkiplist_findGreaterOrEqual(t *testing.T) {
	type test struct {
		list          [][]int
		keyToFind     int
		expectedFound int
		wantNil       bool
		wantSentinel  bool
	}

	tests := map[string]test{
		"EqualInTheMiddleMultilevel": {
			list: [][]int{
				{1, 2, 3, 4, 5, 6, 7},
				{1, 4, 6},
				{1, 6},
				{1},
			},
			keyToFind:     5,
			expectedFound: 5,
		},
		"EqualInTheMiddleSingleLevel": {
			list: [][]int{
				{1, 2, 3, 4, 5, 6, 7},
			},
			keyToFind:     4,
			expectedFound: 4,
		},
		"EqualAtTheBeginningSingleLevel": {
			list: [][]int{
				{1, 2, 3, 4, 5, 6, 7},
			},
			keyToFind:     1,
			expectedFound: 1,
		},
		"EqualAtTheBeginningMultilevel": {
			list: [][]int{
				{1, 2, 3, 4, 5, 6, 7},
				{1, 4, 6},
				{1, 6},
				{1},
			},
			keyToFind:     1,
			expectedFound: 1,
		},
		"EqualAtTheEndMultilevel": {
			list: [][]int{
				{1, 2, 3, 4, 5, 6, 7},
				{1, 4, 6},
				{1, 6},
				{1},
			},
			keyToFind:     7,
			expectedFound: 7,
		},
		"EqualSingleValueList": {
			list: [][]int{
				{10},
			},
			keyToFind:     10,
			expectedFound: 10,
		},
		"GreaterInTheMiddleMultilevel": {
			list: [][]int{
				{1, 2, 3, 4, 6, 7, 8, 9, 10},
				{1, 4, 6, 9},
				{1, 6},
				{1},
			},
			keyToFind:     5,
			expectedFound: 6,
		},
		"GreaterAtTheEndMultiLevel": {
			list: [][]int{
				{1, 2, 3, 4, 6, 7, 8, 10},
				{1, 4, 6},
				{1, 6},
				{1},
			},
			keyToFind:     9,
			expectedFound: 10,
		},
		"KeyIsGreatherThanTheLastElementMultiLevel": {
			list: [][]int{
				{1, 2, 3, 4, 6, 7, 8, 10},
				{1, 4, 6},
				{1, 6},
				{1},
			},
			keyToFind: 11,
			wantNil:   true,
		},
		"KeyIsLessThanTheFirstElementMultiLevel": {
			list: [][]int{
				{1, 2, 3, 4, 6, 7, 8, 9, 10},
				{1, 4, 6, 9},
				{1, 6},
				{1},
			},
			keyToFind:     0,
			expectedFound: 1,
		},
		"EmptyList": {
			list:      [][]int{},
			keyToFind: 1,
			wantNil:   true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			list := construct(tt.list, IntComparator, false)

			found := list.findGreaterOrEqual(tt.keyToFind, nil)

			if tt.wantNil {
				require.Nil(t, found)
				return
			}

			if tt.wantSentinel {
				require.NotNil(t, found)
				return
			}

			require.NotNil(t, found)
			assert.Equal(t, tt.expectedFound, found.key, "wrong node returned")
		})
	}
}

func TestSkiplist_findGreaterOrEqual_WithSearchPath(t *testing.T) {
	list := construct([][]int{
		{1, 2, 3, 4, 6, 7, 8, 9, 10},
		{1, 4, 6, 9},
		{1, 6},
		{1},
	}, IntComparator, false)

	var searchPath listNodes[int, bool]

	list.findGreaterOrEqual(9, &searchPath)

	assert.Equal(t, 1, searchPath[3].key)
	assert.Equal(t, 6, searchPath[2].key)
	assert.Equal(t, 9, searchPath[1].key)
	assert.Equal(t, 9, searchPath[0].key)
}

func TestSkiplist_Insert(t *testing.T) {
	type test struct {
		initialKeys [][]int
		insertKeys  []int
		insertValue string
		wantKeys    []int
		assertFunc  func(t *testing.T, l *Skiplist[int, string])
	}

	tests := map[string]test{
		"InsertWithExistingKeyReplacement": {
			initialKeys: [][]int{
				{1, 2, 3, 4, 5},
				{1, 3},
				{1},
			},
			insertValue: "new value",
			insertKeys:  []int{4},
			wantKeys:    []int{1, 2, 3, 4, 5},
			assertFunc: func(t *testing.T, l *Skiplist[int, string]) {
				value, err := l.Get(4)
				require.NoError(t, err)
				assert.Equal(t, "new value", value)
			},
		},
		"InsertSingleValueIntoEmptyList": {
			initialKeys: [][]int{},
			insertKeys:  []int{10},
			insertValue: "value",
			wantKeys:    []int{10},
			assertFunc: func(t *testing.T, l *Skiplist[int, string]) {
				require.Equal(t, 10, l.head.next[0].key)
				require.Equal(t, "value", l.head.next[0].value)
			},
		},
		"InsertMultipleSortedValuesIntoEmptyListWithoutRebalancing": {
			initialKeys: [][]int{},
			insertKeys:  []int{1, 2},
			wantKeys:    []int{1, 2},
			assertFunc:  func(t *testing.T, l *Skiplist[int, string]) {},
		},
		"InsertMultipleSortedValuesIntoEmptyListWithRebalancing": {
			initialKeys: [][]int{},
			insertKeys:  []int{1, 13, 24, 91, 122, 216},
			wantKeys:    []int{1, 13, 24, 91, 122, 216},
			assertFunc:  func(t *testing.T, l *Skiplist[int, string]) {},
		},
		"InsertMultipleUnsortedValuesIntoEmptyList": {
			initialKeys: [][]int{},
			insertKeys:  []int{441, 10, 174, 83, 16, 44, 0, 15},
			wantKeys:    []int{0, 10, 15, 16, 44, 83, 174, 441},
			assertFunc:  func(t *testing.T, l *Skiplist[int, string]) {},
		},
		"InsertValuesToTheMiddleOfTheList": {
			initialKeys: [][]int{
				{0, 10, 15, 20, 25, 30},
				{15, 25},
				{25},
			},
			insertKeys: []int{13, 17},
			wantKeys:   []int{0, 10, 13, 15, 17, 20, 25, 30},
			assertFunc: func(t *testing.T, l *Skiplist[int, string]) {},
		},
		"InsertValuesToTheStartOfTheList": {
			initialKeys: [][]int{
				{10, 20, 30, 40, 50},
				{20, 40, 50},
				{40},
			},
			insertKeys: []int{1, 3},
			wantKeys:   []int{1, 3, 10, 20, 30, 40, 50},
			assertFunc: func(t *testing.T, l *Skiplist[int, string]) {},
		},
		"InsertValueToTheEndOfTheList": {
			initialKeys: [][]int{
				{10, 20, 30, 40, 50},
				{20, 40, 50},
				{40},
			},
			insertKeys: []int{200, 100, 55},
			wantKeys:   []int{10, 20, 30, 40, 50, 55, 100, 200},
			assertFunc: func(t *testing.T, l *Skiplist[int, string]) {},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			list := construct(tt.initialKeys, IntComparator, "")

			for _, key := range tt.insertKeys {
				list.Insert(key, tt.insertValue)
			}

			actualKeys := make([]int, 0, list.Size())
			iterator := list.Scan()

			for iterator.HasNext() {
				key, _ := iterator.Next()
				actualKeys = append(actualKeys, key)
			}

			assert.Equal(t, tt.wantKeys, actualKeys)

			tt.assertFunc(t, list)
		})
	}
}

func TestSkiplist_Scan(t *testing.T) {
	type test struct {
		prepareFunc  func(l *Skiplist[int, string])
		wantSequence []int
	}

	tests := map[string]test{
		"ScanEmptyList": {
			prepareFunc:  func(l *Skiplist[int, string]) {},
			wantSequence: []int{},
		},
		"ScanSingleValue": {
			prepareFunc: func(l *Skiplist[int, string]) {
				l.Insert(100, "value")
			},
			wantSequence: []int{100},
		},
		"ScanMultipleValues": {
			prepareFunc: func(l *Skiplist[int, string]) {
				l.Insert(6, "")
				l.Insert(2, "")
				l.Insert(5, "")
				l.Insert(3, "")
				l.Insert(1, "")
				l.Insert(7, "")
				l.Insert(4, "")
			},
			wantSequence: []int{1, 2, 3, 4, 5, 6, 7},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			list := New[int, string](IntComparator)

			tt.prepareFunc(list)

			iterator := list.Scan()
			actualSeq := make([]int, 0, list.Size())

			for iterator.HasNext() {
				key, _ := iterator.Next()
				actualSeq = append(actualSeq, key)
			}

			assert.Equal(t, tt.wantSequence, actualSeq)
		})
	}
}

func TestSkiplist_DataRaces(t *testing.T) {
	run := make(chan bool)
	concurrency := 10

	keys := make([]int, 0, concurrency)
	for k := 0; k < concurrency; k++ {
		keys = append(keys, k)
	}

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	wg := sync.WaitGroup{}
	wg.Add(concurrency * 2)

	list := New[int, bool](IntComparator)

	// Simulate concurrent reads and writes.
	for k := 0; k < concurrency; k++ {
		go func(key int) {
			<-run

			list.Insert(key, true)
			wg.Done()
		}(k)

		go func(key int) {
			<-run

			list.Get(key)
			wg.Done()
		}(k)
	}

	close(run)
	wg.Wait()
}
