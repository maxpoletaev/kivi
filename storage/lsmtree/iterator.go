package lsmtree

import (
	"io"

	"github.com/maxpoletaev/kiwi/internal/protoio"
	"github.com/maxpoletaev/kiwi/storage/lsmtree/proto"
)

type Iterator struct {
	Item   *proto.DataEntry
	reader *protoio.Reader
}

func (i *Iterator) Next() error {
	entry := &proto.DataEntry{}

	if _, err := i.reader.ReadNext(entry); err != nil {
		if err == io.EOF {
			i.Item = nil
			return nil
		}

		return err
	}

	i.Item = entry

	return nil
}
