package set

import "github.com/maxpoletaev/kiwi/internal/generic"

type Set[T comparable] map[T]struct{}

func (s Set[T]) Add(val T) {
	s[val] = struct{}{}
}

func (s Set[T]) And(ss Set[T]) Set[T] {
	newset := make(Set[T], len(s)+len(ss))
	generic.MapCopy(s, newset)
	generic.MapCopy(ss, newset)
	return newset
}

func (s Set[T]) Or(ss Set[T]) Set[T] {
	newset := make(Set[T], generic.Max(len(s), len(ss)))

	for _, val := range generic.MapKeys(s, ss) {
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
	return generic.MapKeys(s)
}

func (s Set[T]) Has(val T) bool {
	if _, ok := s[val]; ok {
		return true
	}

	return false
}

func (s Set[T]) Equals(ss Set[T]) bool {
	if len(s) != len(ss) {
		return false
	}

	size := generic.Max(len(s), len(ss))
	seen := make(map[T]int, size)

	for k := range s {
		seen[k]++
	}

	for k := range ss {
		if seen[k] == 0 {
			return false
		}

		seen[k]++
	}

	for _, v := range seen {
		if v != 2 {
			return false
		}
	}

	return true
}

func FromSlice[T comparable](sl []T) Set[T] {
	set := make(Set[T], len(sl))
	for _, val := range sl {
		set.Add(val)
	}
	return set
}

func New[T comparable](sl ...T) Set[T] {
	set := make(Set[T], len(sl))
	for _, val := range sl {
		set.Add(val)
	}
	return set
}
