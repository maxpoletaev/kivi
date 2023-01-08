package lsmtree

import (
	"io"

	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
)

type Iterator struct {
	next   *proto.DataEntry
	reader *protoio.Reader
}

func (i *Iterator) HasNext() bool {
	return i.next != nil
}

func (i *Iterator) Next() (*proto.DataEntry, error) {
	next := i.next

	msg := &proto.DataEntry{}

	if _, err := i.reader.ReadNext(msg); err != nil {
		if err != io.EOF {
			return nil, err
		}

		i.next = nil
	}

	return next, nil
}
