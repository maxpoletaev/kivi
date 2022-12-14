package rolling

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInc(t *testing.T) {
	counter := NewCounter[uint8]()

	for i := 0; i < int(math.Pow(2, 8))-1; i++ {
		if _, rollover := counter.Inc(); rollover {
			t.Errorf("unexpected rollover")
		}
	}

	require.Equal(t, uint8(255), counter.Value())
	require.False(t, counter.Rollover())

	value, rollover := counter.Inc()
	require.Equal(t, uint8(0), value)
	require.True(t, rollover)

	require.Equal(t, uint8(0), counter.Value())
	require.True(t, counter.Rollover())

}

func TestLess(t *testing.T) {
	counter1 := NewCounter[uint8]()
	counter2 := NewCounter[uint8]()

	for i := 0; i < int(math.Pow(2, 8))-1; i++ {
		counter1.Inc()
		counter2.Inc()
	}

	require.False(t, Less(counter1, counter2))

	counter2.Inc()
	require.True(t, Less(counter1, counter2))

	counter1.Inc()
	require.False(t, Less(counter1, counter2))
}
