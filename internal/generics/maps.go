package generics

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

func MapCopy[K, V comparable](src, dst map[K]V) {
	for k, v := range src {
		dst[k] = v
	}
}