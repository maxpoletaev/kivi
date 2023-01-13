package lsmtree

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kiwi/internal/bloom"
	"github.com/maxpoletaev/kiwi/internal/heap"
	"github.com/maxpoletaev/kiwi/internal/opengroup"
	"github.com/maxpoletaev/kiwi/internal/protoio"
	"github.com/maxpoletaev/kiwi/storage/lsmtree/proto"
)

type heapItem struct {
	tableID int64
	entry   *proto.DataEntry
}

func mergeTables(tables []*SSTable, opts flushOpts) (*SSTable, error) {
	iterators := make(map[int64]*Iterator, len(tables))

	pq := heap.New(func(a, b *heapItem) bool {
		// Prioritize the newest value if the keys are the same.
		if a.entry.Key == b.entry.Key {
			return a.tableID > b.tableID
		}

		// Otherwise, prioritize the lowest key.
		return a.entry.Key < b.entry.Key
	})

	og := opengroup.New()

	var err error

	defer func() {
		og.CloseAll()

		if err != nil {
			og.RemoveAll()
		}
	}()

	info := &SSTableInfo{
		ID:        opts.tableID,
		Level:     opts.level,
		IndexFile: fmt.Sprintf("sst-%d.L%d.index", opts.tableID, opts.level),
		DataFile:  fmt.Sprintf("sst-%d.L%d.data", opts.tableID, opts.level),
		BloomFile: fmt.Sprintf("sst-%d.L%d.bloom", opts.tableID, opts.level),
	}

	dataFile := og.Open(filepath.Join(opts.prefix, info.DataFile), os.O_RDWR|os.O_CREATE, 0o644)
	indexFile := og.Open(filepath.Join(opts.prefix, info.IndexFile), os.O_WRONLY|os.O_CREATE, 0o644)
	bloomFile := og.Open(filepath.Join(opts.prefix, info.BloomFile), os.O_WRONLY|os.O_CREATE, 0o644)
	if err := og.Err(); err != nil {
		return nil, err
	}

	// Initialize the priority queue with the first entry from each table.
	for _, table := range tables {
		iter := table.Iterator()

		if err = iter.Next(); err != nil {
			return nil, err
		}

		if iter.Item == nil {
			continue
		}

		pq.Push(&heapItem{
			tableID: table.ID,
			entry:   iter.Item,
		})

		iterators[table.ID] = iter
	}

	indexWriter := protoio.NewWriter(indexFile)
	dataWriter := protoio.NewWriter(dataFile)

	var (
		lastOffset int64
		lastKey    string
	)

	for pq.Len() > 0 {
		item := pq.Pop()
		iter := iterators[item.tableID]

		if item.entry.Key != lastKey {
			lastKey = item.entry.Key

			offset := dataWriter.Offset()

			if _, err = dataWriter.Append(item.entry); err != nil {
				return nil, err
			}

			if lastOffset == 0 || offset-lastOffset > opts.indexGap {
				if _, err = indexWriter.Append(&proto.IndexEntry{
					Key:        item.entry.Key,
					DataOffset: offset,
				}); err != nil {
					return nil, err
				}

				lastOffset = offset
			}
		}

		if err = iter.Next(); err != nil {
			return nil, err
		}

		if iter.Item != nil {
			pq.Push(&heapItem{
				tableID: item.tableID,
				entry:   iter.Item,
			})
		}
	}

	info.NumEntries = int64(dataWriter.Count())

	if _, err = dataFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to start of data file: %w", err)
	}

	// The size of the bloom filter is based on the number of entries in the data file.
	// We did not know the number of entries in advance, so we have to re-read the entire data file.
	bloomfilter := bloom.NewWithProbability(dataWriter.Count(), opts.bloomProb)
	dataReader := protoio.NewReader(dataFile)
	entry := &proto.DataEntry{}

	for {
		if _, err = dataReader.ReadNext(entry); err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("failed to read data entry: %w", err)
		}

		bloomfilter.Add([]byte(entry.Key))
	}

	var (
		bloomData []byte
		sst       *SSTable
	)

	if bloomData, err = protobuf.Marshal(&proto.BloomFilter{
		Crc32:     crc32.ChecksumIEEE(bloomfilter.Bytes()),
		NumHashes: int32(bloomfilter.Hashes()),
		NumBytes:  int32(bloomfilter.SizeBytes()),
		Data:      bloomfilter.Bytes(),
	}); err != nil {
		return nil, fmt.Errorf("failed to marshal bloom filter: %w", err)
	}

	if _, err = bloomFile.Write(bloomData); err != nil {
		return nil, fmt.Errorf("failed to write bloom filter: %w", err)
	}

	if sst, err = OpenTable(info, opts.prefix, opts.mmapOpen); err != nil {
		return nil, fmt.Errorf("failed to open table: %w", err)
	}

	return sst, nil
}
