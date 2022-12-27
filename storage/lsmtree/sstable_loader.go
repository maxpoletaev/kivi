package lsmtree

import (
	"fmt"
	"io"
	"os"

	"github.com/maxpoletaev/kv/internal/bloom"
	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
	"github.com/maxpoletaev/kv/storage/skiplist"
	"golang.org/x/exp/mmap"
)

type SSTableLoader struct {
	IndexFilePath      string
	DataFilePath       string
	BloomFilePath      string
	MmapDataFile       bool
	BloomFilterSize    int
	BloomFilterHashers int
}

func (l *SSTableLoader) loadIndex() (*skiplist.Skiplist[string, int64], error) {
	index := skiplist.New[string, int64](
		skiplist.StringComparator,
	)

	file, err := os.OpenFile(l.IndexFilePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	defer file.Close()

	reader := protoio.NewReader(file)

	for {
		var entry proto.IndexEntry

		if _, err := reader.ReadNext(&entry); err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("failed to read index entry: %w", err)
		}

		index.Insert(entry.Key, entry.DataOffset)
	}

	return index, nil
}

func (l *SSTableLoader) loadBloomFilter() (*bloom.Filter, error) {
	buf, err := os.ReadFile(l.BloomFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bloom filter data: %w", err)
	}

	if len(buf) != l.BloomFilterSize {
		return nil, fmt.Errorf("invalid bloom filter size: %d, want: %d", len(buf), l.BloomFilterSize)
	}

	return bloom.New(buf, l.BloomFilterHashers), nil
}

func (l *SSTableLoader) openDataFile() (readerAtCloser, error) {
	if l.MmapDataFile {
		file, err := mmap.Open(l.DataFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open data file: %w", err)
		}

		return file, nil
	}

	file, err := os.OpenFile(l.DataFilePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	return file, nil
}

func (l *SSTableLoader) Load() (*SSTable, error) {
	index, err := l.loadIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	dataFile, err := l.openDataFile()
	if err != nil {
		return nil, fmt.Errorf("failed to load data: %w", err)
	}

	bloomFilter, err := l.loadBloomFilter()
	if err != nil {
		return nil, fmt.Errorf("failed to load bloom filter: %w", err)
	}

	table := &SSTable{
		index:       index,
		dataFile:    dataFile,
		bloomfilter: bloomFilter,
	}

	return table, nil
}
