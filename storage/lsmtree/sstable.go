package lsmtree

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/exp/mmap"
	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kiwi/internal/bloom"
	"github.com/maxpoletaev/kiwi/internal/opengroup"
	"github.com/maxpoletaev/kiwi/internal/protoio"
	"github.com/maxpoletaev/kiwi/internal/skiplist"
	"github.com/maxpoletaev/kiwi/storage/lsmtree/proto"
)

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

// SSTable is a sorted string table. It is a collection of key/value pairs
// that are sorted by key. It is immutable, and is used to store data on disk.
type SSTable struct {
	*SSTableInfo
	index       *skiplist.Skiplist[string, int64]
	dataFile    readerAtCloser
	bloomFilter *bloom.Filter
}

// OpenTable opens an SSTable from the given paths. All files must exist,
// and the parameters of the bloom filter must match the parameters used
// to create the SSTable.
func OpenTable(info *SSTableInfo, prefix string, useMmap bool) (*SSTable, error) {
	og := opengroup.New()
	defer og.CloseAll()

	indexFile := og.Open(filepath.Join(prefix, info.IndexFile), os.O_RDONLY, 0)
	bloomFile := og.Open(filepath.Join(prefix, info.BloomFile), os.O_RDONLY, 0)

	if err := og.Err(); err != nil {
		return nil, fmt.Errorf("failed to open files: %w", err)
	}

	cmp := skiplist.StringComparator
	index := skiplist.New[string, int64](cmp)
	indexReader := protoio.NewReader(indexFile)

	for {
		var entry proto.IndexEntry

		if _, err := indexReader.ReadNext(&entry); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("failed to read index entry: %w", err)
		}

		index.Insert(entry.Key, entry.DataOffset)
	}

	bloomData, err := io.ReadAll(bloomFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}

	bf := &proto.BloomFilter{}
	if err := protobuf.Unmarshal(bloomData, bf); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bloom filter: %w", err)
	}

	if len(bf.Data) != int(bf.NumBytes) {
		return nil, fmt.Errorf("invalid bloom filter size")
	}

	if bf.Crc32 > 0 && crc32.ChecksumIEEE(bf.Data) != bf.Crc32 {
		return nil, fmt.Errorf("bloom filter checksum mismatch")
	}

	if useMmap {
		dataFile, err := mmap.Open(filepath.Join(prefix, info.DataFile))
		if err != nil {
			return nil, fmt.Errorf("failed to mmap data file: %w", err)
		}

		return &SSTable{
			SSTableInfo: info,
			index:       index,
			dataFile:    dataFile,
			bloomFilter: bloom.New(bf.Data, int(bf.NumHashes)),
		}, nil
	}

	dataFile, err := os.OpenFile(
		filepath.Join(prefix, info.DataFile), os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	return &SSTable{
		SSTableInfo: info,
		index:       index,
		dataFile:    dataFile,
		bloomFilter: bloom.New(bf.Data, int(bf.NumHashes)),
	}, nil
}

// Close closes the SSTable, freeing up any resources it is using.
// Once closed, any current or subsequent calls to Get will fail.
// Note that ine index reader is already closed during loadIndex.
func (sst *SSTable) Close() error {
	if err := sst.dataFile.Close(); err != nil {
		return fmt.Errorf("failed to close data reader: %w", err)
	}

	return nil
}

// Iterator returns an iterator over the SSTable.
func (sst *SSTable) Iterator() *Iterator {
	return &Iterator{
		reader: protoio.NewReader(sst.dataFile),
	}
}

// MayContain checks the underlying bloom filter to see if the key is in the SSTable.
// This is a fast operation, and can be used to avoid accessing the disk if the key
// is not present. Yet, it may return false positives.
func (sst *SSTable) MayContain(key string) bool {
	return sst.bloomFilter.Check([]byte(key))
}

func (sst *SSTable) Get(key string) (*proto.DataEntry, bool, error) {
	// Check the bloom filter first, if it's not there, it's not in the SSTable.
	if !sst.bloomFilter.Check([]byte(key)) {
		return nil, false, nil
	}

	// Find the closest offset in the sparse index.
	_, offset, found := sst.index.LessOrEqual(key)
	if !found {
		return nil, false, nil
	}

	// We create a new reader for each get. It is important for the reader to use the
	// pread syscall instead of read, so that we do not need to synchronize with
	// other readers.
	reader := protoio.NewReader(sst.dataFile)
	entry := &proto.DataEntry{}

	// Scan through the data file until we find the key we're looking for, or we bump
	// into a key which is greater. Although the data is sorted, binary search is not
	// applicable here, because the size of each entry is different. But since the size
	// of the block is quite small, and we read sequentially, this is not a big deal.
	for len(entry.Key) == 0 || key > entry.Key {
		read, err := reader.ReadAt(entry, offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
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

	return entry, true, nil
}
