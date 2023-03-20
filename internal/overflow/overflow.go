package overflow

const (
	Less    = -1
	Equal   = 0
	Greater = 1
)

func sameSign(a, b int32) bool {
	return (a >= 0) == (b >= 0)
}

func CompareInt32(a, b int32) int {
	if sameSign(a, b) {
		if a > b {
			return Greater
		} else if a < b {
			return Less
		} else {
			return Equal
		}
	} else {
		if a < 0 {
			return Less
		} else {
			return Greater
		}
	}
}
