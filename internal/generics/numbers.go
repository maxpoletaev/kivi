package generics

type Number interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

func Sum[V Number](values ...V) (sum V) {
	for _, n := range values {
		sum += n
	}

	return
}

func Max[V Number](values ...V) V {
	if len(values) == 0 {
		panic("must have at least one value")
	}

	current := values[0]

	for i := 1; i < len(values); i++ {
		if values[i] > current {
			current = values[i]
		}
	}

	return current
}
