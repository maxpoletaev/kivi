package lsmtree

import (
	"fmt"
	"io"

	"github.com/maxpoletaev/kv/internal/bloom"
	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
	"github.com/maxpoletaev/kv/storage/skiplist"
)

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

// SSTable is a sorted string table. It is a collection of key/value pairs
// that are sorted by key. It is immutable, and is used to store data on disk.
type SSTable struct {
	index       *skiplist.Skiplist[string, int64]
	dataFile    readerAtCloser
	bloomfilter *bloom.Filter
}

// Close closes the SSTable, freeing up any resources it is using.
// Once closed, any current or subsequent calls to Get will fail.
// Note that ine index reader is already closed during loadIndex.
func (ss *SSTable) Close() error {
	if err := ss.dataFile.Close(); err != nil {
		return fmt.Errorf("failed to close data reader: %w", err)
	}

	return nil
}

// MayContain checks the underlying bloom filter to see if the key is in the SSTable.
// This is a fast operation, and can be used to avoid accessing the disk if the key
// is not present. Yet, it may return false positives.
func (ss *SSTable) MayContain(key string) bool {
	return ss.bloomfilter.Check([]byte(key))
}

func (ss *SSTable) Get(key string) (*proto.DataEntry, bool, error) {
	// Check the bloom filter first, if it's not there, it's not in the SSTable.
	if !ss.bloomfilter.Check([]byte(key)) {
		return nil, false, nil
	}

	// Find the closest offset in the sparse index.
	_, offset, found := ss.index.LessOrEqual(key)
	if !found {
		return nil, false, nil
	}

	// We create a new reader for each get. It is important for the reader to use pread
	// instead of read, so that we do not need to synchronize with other readers.
	reader := protoio.NewReader(ss.dataFile)
	entry := &proto.DataEntry{}

	// Scan through the data file until we find the key we're looking for, or we bump
	// into a key which is greater. Although the data is sorted, binary search is not
	// applicable here, because the size of each entry is different. But since the size
	// of the block is quite small, and we read sequentially, this is not a big deal.
	for len(entry.Key) == 0 || key > entry.Key {
		read, err := reader.ReadAt(entry, offset)
		if err != nil {
			if err == io.EOF {
				return nil, false, nil
			}

			return nil, false, fmt.Errorf("failed to read data entry: %w", err)
		}

		offset += int64(read)
	}

	// Record doesn't exist or was deleted.
	if entry.Key != key {
		return nil, false, nil
	}

	// Tombstone records are deleted records.
	if entry.Tombstone {
		return nil, true, nil
	}

	return entry, true, nil
}
