package lsmtree

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kivi/internal/bloom"
	"github.com/maxpoletaev/kivi/internal/opengroup"
	"github.com/maxpoletaev/kivi/internal/protoio"
	"github.com/maxpoletaev/kivi/storage/lsmtree/proto"
)

type flushOpts struct {
	prefix    string
	tableID   int64
	indexGap  int64
	mmapOpen  bool
	bloomProb float64
	level     int
}

// flushToDisk writes the contents of the memtable to disk and returns an SSTable
// that can be used to read the data. The memtable must be closed before calling
// this function to guarantee that it is not modified while the flush. The parameters
// of the bloom filter are calculated based on the number of entries in the memtable.
func flushToDisk(mem *Memtable, opts flushOpts) (sst *SSTable, err error) {
	og := opengroup.New()

	defer func() {
		og.CloseAll()

		if err != nil {
			og.RemoveAll()
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

	indexFile := og.Open(filepath.Join(opts.prefix, info.IndexFile), os.O_CREATE|os.O_WRONLY, 0o644)
	bloomFile := og.Open(filepath.Join(opts.prefix, info.BloomFile), os.O_CREATE|os.O_WRONLY, 0o644)
	dataFile := og.Open(filepath.Join(opts.prefix, info.DataFile), os.O_CREATE|os.O_WRONLY, 0o644)

	if err = og.Err(); err != nil {
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

		bf.Add([]byte(key))

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
		NumBytes:  int32(bf.SizeBytes()),
		NumHashes: int32(bf.Hashes()),
		Data:      bf.Bytes(),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to marshal bloom filter: %w", err)
	}

	if _, err = bloomFile.Write(bloomBytes); err != nil {
		return nil, fmt.Errorf("failed to write bloom filter: %w", err)
	}

	// Use the size of the data file as the size of the table,
	// as it includes both the size of the keys and the values.
	info.Size = dataWriter.Offset()

	// Open the flushed table for reading. This should be done before discarding
	// the memtable as we want to ensure that the table is readable.
	sst, err = OpenTable(info, opts.prefix, opts.mmapOpen)
	if err != nil {
		return nil, fmt.Errorf("failed to open table: %w", err)
	}

	return sst, nil
}
