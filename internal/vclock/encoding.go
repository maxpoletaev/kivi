package vclock

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/maxpoletaev/kivi/internal/generic"
)

// ToString encodes the vector clock into a string representation that can be used
// for comparison. The format is as follows: {k1=v1,k2=v2,...,kn=vn} where k is
// the node ID and v is the counter value. The counter value is encoded using
// base 36.
func ToString(vc Version) string {
	keys := generic.MapKeys(vc)
	slices.Sort(keys)

	var s strings.Builder
	s.WriteByte('{')

	for _, k := range keys {
		v := vc[k]
		if v == 0 {
			continue
		}

		if s.Len() > 1 {
			s.WriteByte(',')
		}

		var (
			key = strconv.FormatUint(uint64(k), 10)
			val = strings.ToUpper(strconv.FormatUint(v, 36))
		)

		s.WriteString(key)
		s.WriteByte('=')
		s.WriteString(val)
	}

	s.WriteByte('}')

	return s.String()
}

// FromString decodes the vector clock from a string representation. The format is the
// same as the one used by ToString. If the string is empty, an empty vector clock
// is returned.
func FromString(encoded string) (Version, error) {
	vc := make(Version)
	if encoded == "" {
		return vc, nil
	}

	if encoded[0] != '{' || encoded[len(encoded)-1] != '}' {
		return vc, fmt.Errorf("invalid vector clock format")
	}

	for _, p := range strings.Split(encoded[1:len(encoded)-1], ",") {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}

		if kv[1][0] == '!' {
			kv[1] = kv[1][1:]
		}

		k, c := kv[0], strings.ToLower(kv[1])
		key, err1 := strconv.ParseUint(k, 10, 64)
		val, err2 := strconv.ParseUint(c, 36, 32)

		if err := errors.Join(err1, err2); err != nil {
			return vc, err
		}

		vc[uint32(key)] = val
	}

	return vc, nil
}

// MustFromString is like FromString but panics if the string cannot be decoded.
func MustFromString(encoded string) Version {
	vc, err := FromString(encoded)
	if err != nil {
		panic(err)
	}

	return vc
}
