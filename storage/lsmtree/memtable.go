package lsmtree

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
	"github.com/maxpoletaev/kv/storage/skiplist"
)

type Memtable struct {
	entries   *skiplist.Skiplist[string, *proto.DataEntry]
	walWriter protoio.SequentialWriter
	walFile   *os.File
	size      int64
}

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

	atomic.AddInt64(&mt.size, int64(n))

	return nil
}

func (mt *Memtable) Iter() *skiplist.Iterator[string, *proto.DataEntry] {
	return mt.entries.Scan()
}

func (mt *Memtable) Contains(key string) bool {
	return mt.entries.Contains(key)
}

func (mt *Memtable) Len() int {
	return mt.entries.Size()
}

func (mt *Memtable) Size() int64 {
	return atomic.LoadInt64(&mt.size)
}

func (mt *Memtable) Close() error {
	if err := mt.walFile.Close(); err != nil {
		return fmt.Errorf("failed to close wal file: %w", err)
	}

	if err := os.Remove(mt.walFile.Name()); err != nil {
		return fmt.Errorf("failed to remove wal file: %w", err)
	}

	return nil
}

func CreateMemtable(walPath string) (*Memtable, error) {
	walFile, err := os.OpenFile(
		walPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	entries := skiplist.New[string, *proto.DataEntry](skiplist.StringComparator)
	writer := protoio.NewWriter(walFile)

	return &Memtable{
		entries:   entries,
		walWriter: writer,
	}, nil
}

func RestoreMemtable(walPath string) (*Memtable, error) {
	walFile, err := os.OpenFile(walPath, os.O_RDONLY, 0)
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

	if err := walFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close wal file: %w", err)
	}

	// Reopen the WAL file in write-only mode to append new entries.
	walFile, err = os.OpenFile(walPath, os.O_WRONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	// Seek to the end of the file to append new entries.
	pos, err := walFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek wal file: %w", err)
	}

	writer := protoio.NewWriter(walFile)

	return &Memtable{
		entries:   entries,
		walFile:   walFile,
		walWriter: writer,
		size:      pos,
	}, nil
}
