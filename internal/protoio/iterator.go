package protoio

import (
	"errors"
	"io"

	"google.golang.org/protobuf/proto"
)

type Iterator[T proto.Message] struct {
	new    func() T
	reader *Reader
	Item   T
}

func NewIterator[T proto.Message](reader *Reader, newItem func() T) *Iterator[T] {
	return &Iterator[T]{
		new:    newItem,
		reader: reader,
	}
}

func (i *Iterator[T]) Next() error {
	var (
		item  = i.new()
		empty T
	)

	if _, err := i.reader.ReadNext(item); err != nil {
		if errors.Is(err, io.EOF) {
			i.Item = empty
			return nil
		}

		return err
	}

	i.Item = item

	return nil
}
