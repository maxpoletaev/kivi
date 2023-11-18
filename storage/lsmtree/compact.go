package lsmtree

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kivi/internal/bloom"
	"github.com/maxpoletaev/kivi/internal/filegroup"
	"github.com/maxpoletaev/kivi/internal/heap"
	"github.com/maxpoletaev/kivi/internal/protoio"
	"github.com/maxpoletaev/kivi/storage/lsmtree/proto"
)

type heapItem struct {
	tableID int64
	entry   *proto.DataEntry
}

func mergeTables(tables []*SSTable, opts flushOpts) (_ *SSTable, err error) {
	iterators := make(map[int64]*protoio.Iterator[*proto.DataEntry], len(tables))

	pq := heap.New(func(a, b *heapItem) bool {
		// Prioritize the newest value if the keys are the same.
		if a.entry.Key == b.entry.Key {
			return a.tableID > b.tableID
		}

		// Otherwise, prioritize the lowest key.
		return a.entry.Key < b.entry.Key
	})

	fg := filegroup.New()

	defer func() {
		// Close the opened files. At this point, we don't care much if there was an
		// error because the sync is done explicitly at the end of the function.
		if err2 := fg.Close(); err2 != nil {
			fmt.Printf("defer: failed to close files: %v\n", err2)
		}

		// In case there was an error during the function execution, do our best to
		// remove the files that were created, so that we don't leave any garbage.
		if err != nil {
			if err2 := fg.Remove(); err2 != nil {
				fmt.Printf("defer: failed to remove files: %v\n", err2)
			}
		}
	}()

	info := &SSTableInfo{
		ID:        opts.tableID,
		Level:     opts.level,
		IndexFile: fmt.Sprintf("sst-%d.L%d.index", opts.tableID, opts.level),
		DataFile:  fmt.Sprintf("sst-%d.L%d.data", opts.tableID, opts.level),
		BloomFile: fmt.Sprintf("sst-%d.L%d.bloom", opts.tableID, opts.level),
	}

	dataFile := fg.Open(filepath.Join(opts.prefix, info.DataFile), os.O_RDWR|os.O_CREATE, 0o644)
	indexFile := fg.Open(filepath.Join(opts.prefix, info.IndexFile), os.O_WRONLY|os.O_CREATE, 0o644)
	bloomFile := fg.Open(filepath.Join(opts.prefix, info.BloomFile), os.O_WRONLY|os.O_CREATE, 0o644)

	if err = fg.Err(); err != nil {
		return nil, fmt.Errorf("failed to open files: %w", err)
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

			info.NumEntries++
		}

		iter := iterators[item.tableID]

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

	if _, err = dataFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to start of data file: %w", err)
	}

	// The size of the bloom filter is based on the number of entries in the data file.
	// We did not know the number of entries in advance, so we have to re-read the entire data file.
	bloomFilter := bloom.NewWithProbability(int(info.NumEntries), opts.bloomProb)
	dataReader := protoio.NewReader(dataFile)
	entry := &proto.DataEntry{}

	for {
		if _, err = dataReader.ReadNext(entry); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("failed to read data entry: %w", err)
		}

		bloomFilter.Add(unsafeBytes(entry.Key))
	}

	var (
		bloomData []byte
		sst       *SSTable
	)

	bloomData, err = protobuf.Marshal(&proto.BloomFilter{
		Crc32:     crc32.ChecksumIEEE(bloomFilter.Bytes()),
		NumHashes: int32(bloomFilter.Hashes()),
		NumBytes:  int32(bloomFilter.Size()),
		Data:      bloomFilter.Bytes(),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to marshal bloom filter: %w", err)
	}

	if _, err = bloomFile.Write(bloomData); err != nil {
		return nil, fmt.Errorf("failed to write bloom filter: %w", err)
	}

	if err = fg.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync files: %w", err)
	}

	if sst, err = OpenTable(info, opts.prefix, opts.useMmap); err != nil {
		return nil, fmt.Errorf("failed to open table: %w", err)
	}

	return sst, nil
}
