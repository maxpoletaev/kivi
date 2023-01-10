package skiplist

import (
	"math/rand"
	"testing"
)

func BenchmarkSkiplist_Search(b *testing.B) {
	list := New[int, int](IntComparator)
	for i := 0; i < 100; i++ {
		list.Insert(i, 0)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		list.Get(rand.Intn(100))
	}
}

func BenchmarkSkiplist_Insert_SingleThread(b *testing.B) {
	list := New[int, int](IntComparator)

	for i := 0; i < b.N; i++ {
		list.Insert(rand.Intn(100), 0)
	}
}

func BenchmarkSkiplist_Insert_MultiThread(b *testing.B) {
	sk := New[int, int](IntComparator)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sk.Insert(rand.Intn(100), 0)
		}
	})
}
