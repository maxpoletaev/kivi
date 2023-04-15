package protoio

import (
	"google.golang.org/protobuf/proto"
)

const headerSize = 12

// RandomAccessReadeer is an interface for reading protobuf messages from a seekable source.
type RandomAccessReadeer interface {
	ReadAt(entry proto.Message, offset int64) (int, error)
}

// SequentialReader is an interface for reading protobuf messages
// sequentially from a source that does not support seeking.
type SequentialReader interface {
	ReadNext(entry proto.Message) (int, error)
	SkipN(n int) error
	Skip() error
	Offset() int64
}

// SequentialWriter is an interface for writing protobuf messages to a
// sequential source. The offset of the next write can be queried with
// the Offset method.
type SequentialWriter interface {
	Append(entry proto.Message) (int, error)
	Offset() int64
}
