package generic

func MapKeys[K comparable, V any](maps ...map[K]V) []K {
	uniqueKeys := make(map[K]struct{})

	for _, m := range maps {
		for k := range m {
			uniqueKeys[k] = struct{}{}
		}
	}

	keys := make([]K, 0, len(uniqueKeys))
	for k := range uniqueKeys {
		keys = append(keys, k)
	}

	return keys
}

func MapValues[K comparable, V any](maps ...map[K]V) []V {
	var cap int
	for _, m := range maps {
		cap += len(m)
	}

	values := make([]V, 0, cap)

	for _, m := range maps {
		for _, v := range m {
			values = append(values, v)
		}
	}

	return values
}

func MapCopy[K, V comparable](src, dst map[K]V) {
	for k, v := range src {
		dst[k] = v
	}
}
