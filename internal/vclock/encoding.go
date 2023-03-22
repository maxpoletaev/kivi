package vclock

import (
	"encoding/base64"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kiwi/internal/vclock/pb"
)

// Encode encodes the vector clock into a base64-encoded string.
// The encoded string are always the same for the same vector clock,
// so it can safely be used for comparison.
func Encode(v *Vector) (string, error) {
	vc := &pb.VectorClock{
		Clocks: v.clocks,
	}

	// TODO: Protobuf docs say that deterministic encoding is not guaranteed
	//  to be stable across different versions of the library, so we should
	//  probably use a different encoding.
	opt := proto.MarshalOptions{Deterministic: true}

	b, err := opt.Marshal(vc)
	if err != nil {
		return "", fmt.Errorf("failed to encode to proto: %w", err)
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

// MustEncode is like Encode, but panics on error.
func MustEncode(v *Vector) string {
	s, err := Encode(v)
	if err != nil {
		panic(err)
	}

	return s
}

// NewEncoded creates a base64-encoded string with the given vector
// values. It is equivalent to MustEncode(New(v)).
func NewEncoded(v ...V) string {
	return MustEncode(New(v...))
}

// Decode decodes a base64-encoded string into a vector clock.
func Decode(s string) (*Vector, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	vc := &pb.VectorClock{}
	if err := proto.Unmarshal(b, vc); err != nil {
		return nil, fmt.Errorf("failed to decode proto: %w", err)
	}

	clocks := vc.Clocks
	if clocks == nil {
		clocks = make(V)
	}

	return &Vector{
		clocks: clocks,
	}, nil
}

// MustDecode is like Decode, but panics on error.
func MustDecode(s string) *Vector {
	v, err := Decode(s)
	if err != nil {
		panic(err)
	}

	return v
}
