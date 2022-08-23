package collections

import "github.com/maxpoletaev/kv/internal/generics"

type Set[T comparable] map[T]struct{}

func (s Set[T]) Add(val T) {
	s[val] = struct{}{}
}

func (s Set[T]) And(ss Set[T]) Set[T] {
	newset := make(Set[T], len(s)+len(ss))
	generics.MapCopy(s, newset)
	generics.MapCopy(ss, newset)
	return newset
}

func (s Set[T]) Or(ss Set[T]) Set[T] {
	newset := make(Set[T], generics.Max(len(s), len(ss)))

	for _, val := range generics.MapKeys(s, ss) {
		_, ok1 := s[val]
		_, ok2 := ss[val]

		if ok1 && ok2 {
			newset.Add(val)
		}
	}

	return newset
}

func (s Set[T]) Remove(val T) {
	delete(s, val)
}

func (s Set[T]) Values() []T {
	return generics.MapKeys(s)
}

func (s Set[T]) Has(val T) bool {
	if _, ok := s[val]; ok {
		return true
	}

	return false
}

func New[T comparable](sl ...T) Set[T] {
	set := make(Set[T], len(sl))
	for _, val := range sl {
		set.Add(val)
	}
	return set
}
