package lsmtree

import (
	"fmt"
	"os"

	"github.com/maxpoletaev/kv/internal/bloom"
	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
)

type SSTableFlusher struct {
	IndexFilePath      string
	DataFilePath       string
	BloomFilePath      string
	SparseIndexGap     int64
	BloomFilterSize    int
	BloomFilterHashers int
}

func (f *SSTableFlusher) createDataWriter() (*protoio.Writer, *os.File, error) {
	file, err := os.OpenFile(f.DataFilePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open data file: %w", err)
	}

	return protoio.NewWriter(file), file, nil
}

func (f *SSTableFlusher) createIndexWriter() (*protoio.Writer, *os.File, error) {
	file, err := os.OpenFile(f.IndexFilePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open index file: %w", err)
	}

	return protoio.NewWriter(file), file, nil
}

func (f *SSTableFlusher) Flush(memtable *Memtable) error {
	dataWriter, dataFile, err := f.createDataWriter()
	if err != nil {
		return err
	}
	defer dataFile.Close()

	indexWriter, indexFile, err := f.createIndexWriter()
	if err != nil {
		return err
	}
	defer indexFile.Close()

	buf := make([]byte, f.BloomFilterSize)
	bloomfilter := bloom.New(buf, f.BloomFilterHashers)

	var lastOffset int64

	// Iterate over the memtable and write the data and index files.
	for it := memtable.Iter(); it.HasNext(); {
		key, entry := it.Next()

		bloomfilter.Add([]byte(key))

		offset := dataWriter.Offset()

		if _, err := dataWriter.Append(entry); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}

		// The index file is sparse, so we only write an index entry if the gap between
		// the current offset and the last offset is larger than the threshold.
		if lastOffset == 0 || offset-lastOffset >= f.SparseIndexGap {
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

	if err := os.WriteFile(f.BloomFilePath, bloomfilter.Bytes(), 0o600); err != nil {
		return fmt.Errorf("failed to write bloom filter: %w", err)
	}

	return nil
}
