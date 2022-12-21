package skiplist

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildList is a helper to simplify skiplist construction during tests.
func buildList[K comparable, V any](keys [][]K, keyComparator Comparator[K], defaultValue V) *Skiplist[K, V] {
	nodes := make(map[K]*listNode[K, V], 0)
	head := &listNode[K, V]{}
	size := 0

	if len(keys) > 0 {
		size = len(keys[0])

		for _, key := range keys[0] {
			node := &listNode[K, V]{key: key}
			node.storeValue(defaultValue)
			nodes[key] = node
		}
	}

	for level := 0; level < len(keys); level++ {
		tail := head.next[level]

		for _, key := range keys[level] {
			node := nodes[key]
			if node == nil {
				panic(fmt.Sprintf("invalid list: node %v is defined at level 0 but missing from level %d", key, level))
			}

			if tail == nil {
				head.next[level] = node
				tail = node
			} else {
				tail.next[level] = node
				tail = node
			}
		}
	}

	return &Skiplist[K, V]{
		head:        head,
		size:        int32(size),
		height:      int32(len(keys)),
		compareKeys: keyComparator,
	}
}

// dumpListNodes returns a copy of the skiplist's internal state, for testing.
func dumpListNodes[K comparable, V any](l *Skiplist[K, V]) [][]*listNode[K, V] {
	height := l.Height()

	levels := make([][]*listNode[K, V], height)

	for level := 0; level < height; level++ {
		node := l.head.loadNext(level)

		for node != nil {
			levels[level] = append(levels[level], node)
			node = node.loadNext(level)
		}
	}

	return levels
}

func validateInternalState[K comparable, V any](t *testing.T, list *Skiplist[K, V]) {
	t.Helper()

	var hasErrors bool

	listDump := dumpListNodes(list)

	for i := 1; i < len(listDump); i++ {
		if len(listDump[i]) > len(listDump[i-1]) {
			t.Errorf("level %d has more keys than level %d", i, i-1)
			hasErrors = true
		}
	}

	for level, nodes := range listDump {
		keyCount := make(map[K]int)
		for _, node := range nodes {
			keyCount[node.key]++
		}

		// Ensure no duplicate keys on each level.
		for key, count := range keyCount {
			if count > 1 {
				t.Errorf("duplicate key %v at level %d (repeats %d times)", key, level, count)
				hasErrors = true
			}
		}

		// Ensure keys are in the right order on each level.
		for i := 1; i < len(nodes); i++ {
			if list.compareKeys(nodes[i].key, nodes[i-1].key) < 0 {
				t.Errorf("wrong key order at level %d: %v < %v", level, nodes[i].key, nodes[i-1].key)
				hasErrors = true
			}
		}
	}

	if hasErrors {
		fmt.Println("skiplist dump:")

		for levels, nodes := range listDump {
			keys := make([]K, len(nodes))
			for i, node := range nodes {
				keys[i] = node.key
			}

			fmt.Printf("    L%d: %v\n", levels, keys)
		}
	}
}

func TestBuildList(t *testing.T) {
	list := buildList([][]int{
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

	validateInternalState(t, list)
}

func TestMakeSkiplistFails(t *testing.T) {
	assert.PanicsWithValue(t, "invalid list: node 3 is defined at level 0 but missing from level 1", func() {
		buildList([][]int{
			{1, 2, 4, 5},
			{1, 3, 4},
		}, IntComparator, false)
	})
}

func TestSkiplist_findLess(t *testing.T) {
	table := map[string]struct {
		list           [][]int
		key            int
		wantHead       bool
		wantNil        bool
		wantKey        int
		wantSearchPath []int
	}{
		"LessInTheMiddle": {
			list: [][]int{
				{1, 2, 3, 4, 6, 7, 8, 9, 10},
				{1, 3, 6, 9},
				{1, 3},
				{1},
			},
			key:            5,
			wantKey:        4,
			wantSearchPath: []int{4, 3, 3, 1},
		},
		"LessAtTheEnd": {
			list: [][]int{
				{1, 2, 3, 4, 6, 7, 8, 9, 10},
				{1, 3, 6, 9},
				{1, 3},
				{1},
			},
			key:            11,
			wantKey:        10,
			wantSearchPath: []int{10, 9, 3, 1},
		},
		"LessAtTheBeginning": {
			list: [][]int{
				{1, 2, 3, 4, 6, 7, 8, 9, 10},
				{1, 3, 6, 9},
				{1, 3},
				{1},
			},
			key:            1,
			wantHead:       true,
			wantSearchPath: []int{0, 0, 0, 0},
		},
		"EmptyList": {
			list:           [][]int{},
			key:            1,
			wantNil:        true,
			wantSearchPath: []int{},
		},
		"SingleNodeList": {
			list: [][]int{
				{1},
			},
			key:            10,
			wantKey:        1,
			wantSearchPath: []int{1},
		},
		"LessInTheMiddleSingleLevel": {
			list: [][]int{
				{1, 2, 3, 4, 6, 7, 8, 9, 10},
			},
			key:            5,
			wantKey:        4,
			wantSearchPath: []int{4},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			var searchPath listNodes[int, bool]

			list := buildList(tt.list, IntComparator, false)
			found := list.findLess(tt.key, &searchPath)

			if tt.wantNil {
				require.Nil(t, found, "node found")
				return
			} else if tt.wantHead {
				require.Equal(t, list.head, found)
				return
			}

			require.NotNil(t, found, "node not found")
			assert.Equal(t, tt.wantKey, found.key, "wrong node returned")

			pathKeys := make([]int, 0, maxHeight)
			for _, node := range searchPath {
				if node == nil {
					break
				}
				pathKeys = append(pathKeys, node.key)
			}

			assert.Equal(t, tt.wantSearchPath, pathKeys, "wrong search path")

			validateInternalState(t, list)
		})
	}
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
		"InsertInTheMiddleWithReplacement": {
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
		"InsertAtTheStartWithReplacement": {
			initialKeys: [][]int{
				{1, 2, 3, 4, 5},
				{1, 3},
				{1},
			},
			insertValue: "new value",
			insertKeys:  []int{1},
			wantKeys:    []int{1, 2, 3, 4, 5},
			assertFunc: func(t *testing.T, l *Skiplist[int, string]) {
				value, err := l.Get(1)
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
				require.Equal(t, "value", l.head.next[0].loadValue())
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
		if strings.HasPrefix(name, "_") {
			continue
		}

		t.Run(name, func(t *testing.T) {
			list := buildList(tt.initialKeys, IntComparator, "")

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

func TestSkiplist_Remove(t *testing.T) {
	type test struct {
		prepareFunc func(l *Skiplist[int, string])
		removeKeys  []int
		wantKeys    []int
	}

	tests := map[string]test{
		"RemoveFromEmptyList": {
			prepareFunc: func(l *Skiplist[int, string]) {},
			removeKeys:  []int{1},
			wantKeys:    []int{},
		},
		"RemoveSingleValue": {
			prepareFunc: func(l *Skiplist[int, string]) {
				l.Insert(1, "value")
			},
			removeKeys: []int{1},
			wantKeys:   []int{},
		},
		"RemoveMultipleValues": {
			prepareFunc: func(l *Skiplist[int, string]) {
				l.Insert(1, "value")
				l.Insert(2, "value")
				l.Insert(3, "value")
				l.Insert(4, "value")
				l.Insert(5, "value")
			},
			removeKeys: []int{1, 3, 5},
			wantKeys:   []int{2, 4},
		},
		"RemoveValuesFromTheMiddleOfTheList": {
			prepareFunc: func(l *Skiplist[int, string]) {
				l.Insert(1, "value")
				l.Insert(2, "value")
				l.Insert(3, "value")
				l.Insert(4, "value")
				l.Insert(5, "value")
			},
			removeKeys: []int{2, 4},
			wantKeys:   []int{1, 3, 5},
		},
		"RemoveValuesFromTheStartOfTheList": {
			prepareFunc: func(l *Skiplist[int, string]) {
				l.Insert(1, "value")
				l.Insert(2, "value")
				l.Insert(3, "value")
				l.Insert(4, "value")
				l.Insert(5, "value")
			},
			removeKeys: []int{1, 2},
			wantKeys:   []int{3, 4, 5},
		},
		"RemoveValuesFromTheEndOfTheList": {
			prepareFunc: func(l *Skiplist[int, string]) {
				l.Insert(1, "value")
				l.Insert(2, "value")
				l.Insert(3, "value")
				l.Insert(4, "value")
				l.Insert(5, "value")
			},
			removeKeys: []int{4, 5},
			wantKeys:   []int{1, 2, 3},
		},
		"RemoveNonExistingValue": {
			prepareFunc: func(l *Skiplist[int, string]) {
				l.Insert(1, "value")
				l.Insert(2, "value")
				l.Insert(3, "value")
				l.Insert(4, "value")
				l.Insert(5, "value")
			},
			removeKeys: []int{6},
			wantKeys:   []int{1, 2, 3, 4, 5},
		},
	}

	for name, tt := range tests {

		t.Run(name, func(t *testing.T) {
			list := New[int, string](IntComparator)

			tt.prepareFunc(list)

			for _, key := range tt.removeKeys {
				list.Remove(key)
			}

			actualKeys := make([]int, 0, list.Size())

			for iter := list.Scan(); iter.HasNext(); {
				key, _ := iter.Next()
				actualKeys = append(actualKeys, key)
			}

			assert.Equal(t, tt.wantKeys, actualKeys)
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
