package lsmtree

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/exp/mmap"

	"github.com/maxpoletaev/kv/internal/bloom"
	"github.com/maxpoletaev/kv/internal/opengroup"
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

func (ss *SSTable) Count() (count int) {
	reader := protoio.NewReader(ss.dataFile)

	for {
		if err := reader.Skip(); err != nil {
			if err == io.EOF {
				break
			}

			panic(fmt.Errorf("failed to skip entry: %w", err))
		}

		count++
	}

	return count
}

// Contains checks the underlying bloom filter to see if the key is in the SSTable.
// This is a fast operation, and can be used to avoid accessing the disk if the key
// is not present. Yet, it may return false positives.
func (ss *SSTable) Contains(key string) bool {
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

type TableOpts struct {
	IndexPath    string
	DataPath     string
	BloomPath    string
	MmapDataFile bool
	BloomSize    int
	BloomFuncs   int
	IndexGap     int64
}

// OpenSSTable opens an SSTable from the given paths. All files must exist,
// and the parameters of the bloom filter must match the parameters used
// to create the SSTable.
func OpenSSTable(opts TableOpts) (*SSTable, error) {
	og := opengroup.New()
	defer og.CloseAll()

	indexFile := og.Open(opts.IndexPath, os.O_RDONLY, 0)
	bloomFile := og.Open(opts.BloomPath, os.O_RDONLY, 0)

	if err := og.Err(); err != nil {
		return nil, fmt.Errorf("failed to open files: %w", err)
	}

	cmp := skiplist.StringComparator
	index := skiplist.New[string, int64](cmp)
	indexReader := protoio.NewReader(indexFile)

	for {
		var entry proto.IndexEntry

		if _, err := indexReader.ReadNext(&entry); err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("failed to read index entry: %w", err)
		}

		index.Insert(entry.Key, entry.DataOffset)
	}

	bloomData, err := io.ReadAll(bloomFile)
	if err != nil || len(bloomData) != opts.BloomSize {
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}

	if opts.MmapDataFile {
		dataFile, err := mmap.Open(opts.DataPath)
		if err != nil {
			return nil, fmt.Errorf("failed to mmap data file: %w", err)
		}

		return &SSTable{
			index:       index,
			dataFile:    dataFile,
			bloomfilter: bloom.New(bloomData, opts.BloomFuncs),
		}, nil
	}

	dataFile, err := os.OpenFile(opts.DataPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	return &SSTable{
		index:       index,
		dataFile:    dataFile,
		bloomfilter: bloom.New(bloomData, opts.BloomFuncs),
	}, nil
}
