package lsmtree

import (
	"fmt"
	"os"

	"github.com/maxpoletaev/kv/internal/bloom"
	"github.com/maxpoletaev/kv/internal/opengroup"
	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
)

func (mt *Memtable) Flush(opts TableOpts) error {
	if err := mt.walFile.Close(); err != nil {
		return fmt.Errorf("failed to close wal file: %w", err)
	}

	og := opengroup.New()
	defer og.CloseAll()

	indexFile := og.Open(opts.IndexPath, os.O_CREATE|os.O_WRONLY, 0o644)
	dataFile := og.Open(opts.DataPath, os.O_CREATE|os.O_WRONLY, 0o644)
	bloomFile := og.Open(opts.BloomPath, os.O_CREATE|os.O_WRONLY, 0o644)

	if err := og.Err(); err != nil {
		return fmt.Errorf("failed to open files: %w", err)
	}

	bf := bloom.New(make([]byte, opts.BloomSize), opts.BloomFuncs)
	indexWriter := protoio.NewWriter(indexFile)
	dataWriter := protoio.NewWriter(dataFile)

	var lastOffset int64

	for it := mt.entries.Scan(); it.HasNext(); {
		key, entry := it.Next()

		bf.Add([]byte(key))

		offset := dataWriter.Offset()

		if _, err := dataWriter.Append(entry); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}

		// The index file is sparse, so we only write an index entry if the gap between
		// the current offset and the last offset is larger than the threshold.
		if lastOffset == 0 || offset-lastOffset >= opts.IndexGap {
			indexEntry := &proto.IndexEntry{
				DataOffset: int64(offset),
				Key:        key,
			}

			if _, err := indexWriter.Append(indexEntry); err != nil {
				return fmt.Errorf("failed to write index entry: %w", err)
			}

			lastOffset = offset
		}
	}

	if _, err := bloomFile.Write(bf.Bytes()); err != nil {
		return fmt.Errorf("failed to write bloom filter: %w", err)
	}

	if err := os.Remove(mt.walFile.Name()); err != nil {
		return fmt.Errorf("failed to remove wal file: %w", err)
	}

	return nil
}
