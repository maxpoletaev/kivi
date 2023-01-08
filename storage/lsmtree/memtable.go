package lsmtree

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
	"github.com/maxpoletaev/kv/storage/skiplist"
)

type Memtable struct {
	*MemtableInfo
	entries   *skiplist.Skiplist[string, *proto.DataEntry]
	walWriter protoio.SequentialWriter
	walFile   *os.File
	dataSize  int64
}

func createMemtable(prefix string) (*Memtable, error) {
	id := time.Now().UnixMicro()
	walFileName := fmt.Sprintf("mem-%d.wal", id)

	walFile, err := os.OpenFile(
		filepath.Join(prefix, walFileName), os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create wal file: %w", err)
	}

	entries := skiplist.New[string, *proto.DataEntry](skiplist.StringComparator)
	writer := protoio.NewWriter(walFile)
	info := &MemtableInfo{
		WALFile: walFileName,
		ID:      id,
	}

	return &Memtable{
		MemtableInfo: info,
		entries:      entries,
		walWriter:    writer,
		walFile:      walFile,
	}, nil
}

func openMemtable(info *MemtableInfo, prefix string) (*Memtable, error) {
	walFile, err := os.OpenFile(
		filepath.Join(prefix, info.WALFile), os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	entries := skiplist.New[string, *proto.DataEntry](skiplist.StringComparator)
	reader := protoio.NewReader(walFile)

	for {
		entry := &proto.DataEntry{}

		if _, err := reader.ReadNext(entry); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		entries.Insert(entry.Key, entry)
	}

	// Seek to the end of the file to append new entries.
	offset, err := walFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek wal file: %w", err)
	}

	return &Memtable{
		MemtableInfo: info,
		entries:      entries,
		dataSize:     offset,
		walFile:      walFile,
		walWriter:    protoio.NewWriter(walFile),
	}, nil
}

// Get returns an entry with the given key. If the entry does not exist or is
// a tombstone, the second return value is false.
func (mt *Memtable) Get(key string) (*proto.DataEntry, bool) {
	entry, ok := mt.entries.Get(key)

	if !ok || entry.Tombstone {
		return nil, false
	}

	return entry, true
}

// Put inserts a new entry into the memtable. The entry is first appended to the
// WAL file and then inserted into the memtable. If the entry already exists in
// the memtable, it is overwritten. Removing an entry is done by inserting a
// entry with a tombstone flag set to true.
func (mt *Memtable) Put(entry *proto.DataEntry) error {
	n, err := mt.walWriter.Append(entry)
	if err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	mt.entries.Insert(entry.Key, entry)

	atomic.AddInt64(&mt.dataSize, int64(n))

	return nil
}

// Iter returns an iterator over the memtable.
func (mt *Memtable) Iter() *skiplist.Iterator[string, *proto.DataEntry] {
	return mt.entries.Scan()
}

// Contains returns true if the memtable contains an entry with the given key.
func (mt *Memtable) Contains(key string) bool {
	return mt.entries.Contains(key)
}

// Len returns the number of entries in the memtable.
func (mt *Memtable) Len() int {
	return mt.entries.Size()
}

// Size returns the size of the memtable in bytes, represented by the size of the WAL file.
func (mt *Memtable) Size() int64 {
	return atomic.LoadInt64(&mt.dataSize)
}

// Close closes the underlying WAL file. The memtable can still be used for reads
// after closing, but the writes will cause panic. Close should not be called
// concurrently with Put.
func (mt *Memtable) Close() error {
	if err := mt.walFile.Close(); err != nil {
		return fmt.Errorf("failed to close wal file: %w", err)
	}

	return nil
}

// Discard removes data files associated with the memtable. It is used when
// the memtable is no longer needed, e.g. when it is merged into a SSTable.
// Memtable should be closed before calling this method.
func (mt *Memtable) Discard() error {
	if err := os.Remove(mt.walFile.Name()); err != nil {
		return fmt.Errorf("failed to remove wal file: %w", err)
	}

	return nil
}

func (mt *Memtable) CloseAndDiscard() error {
	if err := mt.Close(); err != nil {
		return err
	}

	return mt.Discard()
}
