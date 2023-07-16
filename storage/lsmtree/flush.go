package lsmtree

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kivi/internal/bloom"
	"github.com/maxpoletaev/kivi/internal/filegroup"
	"github.com/maxpoletaev/kivi/internal/protoio"
	"github.com/maxpoletaev/kivi/storage/lsmtree/proto"
)

type flushOpts struct {
	prefix    string
	tableID   int64
	indexGap  int64
	useMmap   bool
	bloomProb float64
	level     int
}

// flushToDisk writes the contents of the memtable to disk and returns an SSTable
// that can be used to read the data. The memtable must be closed before calling
// this function to guarantee that it is not modified while the flush. The parameters
// of the bloom filter are calculated based on the number of entries in the memtable.
func flushToDisk(mem *Memtable, opts flushOpts) (sst *SSTable, err error) {
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
		ID:         opts.tableID,
		Level:      opts.level,
		NumEntries: int64(mem.Len()),
		IndexFile:  fmt.Sprintf("sst-%d.L%d.index", opts.tableID, opts.level),
		DataFile:   fmt.Sprintf("sst-%d.L%d.data", opts.tableID, opts.level),
		BloomFile:  fmt.Sprintf("sst-%d.L%d.bloom", opts.tableID, opts.level),
	}

	indexFile := fg.Open(filepath.Join(opts.prefix, info.IndexFile), os.O_CREATE|os.O_WRONLY, 0o644)
	bloomFile := fg.Open(filepath.Join(opts.prefix, info.BloomFile), os.O_CREATE|os.O_WRONLY, 0o644)
	dataFile := fg.Open(filepath.Join(opts.prefix, info.DataFile), os.O_CREATE|os.O_WRONLY, 0o644)

	if err = fg.Err(); err != nil {
		return nil, fmt.Errorf("failed to open files: %w", err)
	}

	bf := bloom.NewWithProbability(mem.Len(), opts.bloomProb)
	indexWriter := protoio.NewWriter(indexFile)
	dataWriter := protoio.NewWriter(dataFile)

	var (
		bloomBytes []byte
		lastOffset int64
	)

	for it := mem.entries.Scan(); it.HasNext(); {
		key, entry := it.Next()

		bf.Add(unsafeBytes(key))

		offset := dataWriter.Offset()

		if _, err = dataWriter.Append(entry); err != nil {
			return nil, fmt.Errorf("failed to write entry: %w", err)
		}

		// The index file is sparse, so we only write an index entry if the gap between
		// the current offset and the last offset is larger than the threshold.
		if lastOffset == 0 || offset-lastOffset >= opts.indexGap {
			indexEntry := &proto.IndexEntry{
				DataOffset: offset,
				Key:        key,
			}

			if _, err = indexWriter.Append(indexEntry); err != nil {
				return nil, fmt.Errorf("failed to write index entry: %w", err)
			}

			lastOffset = offset
		}
	}

	bloomBytes, err = protobuf.Marshal(&proto.BloomFilter{
		Crc32:     crc32.ChecksumIEEE(bf.Bytes()),
		NumBytes:  int32(bf.BytesSize()),
		NumHashes: int32(bf.Hashes()),
		Data:      bf.Bytes(),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to marshal bloom filter: %w", err)
	}

	if _, err = bloomFile.Write(bloomBytes); err != nil {
		return nil, fmt.Errorf("failed to write bloom filter: %w", err)
	}

	// Explicitly sync the files to ensure that they are flushed to disk.
	if err = fg.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync files: %w", err)
	}

	// Use the size of the data file as the size of the table,
	// as it includes both the size of the keys and the values.
	info.Size = dataWriter.Offset()

	// Open the flushed table for reading. This should be done before discarding
	// the memtable as we want to ensure that the table is readable.
	sst, err = OpenTable(info, opts.prefix, opts.useMmap)
	if err != nil {
		return nil, fmt.Errorf("failed to open table: %w", err)
	}

	return sst, nil
}
