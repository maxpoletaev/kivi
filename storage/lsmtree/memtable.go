package lsmtree

import (
	"fmt"
	"io"
	"os"

	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
	"github.com/maxpoletaev/kv/storage/skiplist"
)

type Memtable struct {
	id        int
	entries   *skiplist.Skiplist[string, *proto.DataEntry]
	walWriter protoio.SequentialWriter
	walFile   *os.File
}

func createMemtable(id int, walFilename string) (*Memtable, error) {
	walFile, err := os.OpenFile(
		walFilename, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	entries := skiplist.New[string, *proto.DataEntry](skiplist.StringComparator)
	writer := protoio.NewWriter(walFile)

	return &Memtable{
		id:        id,
		entries:   entries,
		walWriter: writer,
	}, nil
}

// restoreMemtable restores a memtable from a WAL file. The produced memtable is
// read-only after this operation and is only applicable for flushing to disk.
func restoreMemtable(id int, walFilename string) (*Memtable, error) {
	walFile, err := os.OpenFile(walFilename, os.O_RDONLY|os.O_EXCL, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	entries := skiplist.New[string, *proto.DataEntry](skiplist.StringComparator)

	reader := protoio.NewReader(walFile)

	for {
		entry := &proto.DataEntry{}

		if _, err := reader.ReadNext(entry); err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("failed to read entry: %w", err)
		}

		entries.Insert(entry.Key, entry)
	}

	if err := walFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close wal file: %w", err)
	}

	// Reopen the WAL file in write-only mode to append new entries.
	walFile, err = os.OpenFile(walFilename, os.O_WRONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	// Seek to the end of the file to append new entries.
	if _, err := walFile.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("failed to seek wal file: %w", err)
	}

	writer := protoio.NewWriter(walFile)

	return &Memtable{
		id:        id,
		entries:   entries,
		walFile:   walFile,
		walWriter: writer,
	}, nil
}

func (mt *Memtable) ID() int {
	return mt.id
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
	if _, err := mt.walWriter.Append(entry); err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	mt.entries.Insert(entry.Key, entry)

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

func (mt *Memtable) Discard() error {
	if err := mt.walFile.Close(); err != nil {
		return fmt.Errorf("failed to close wal file: %w", err)
	}

	if err := os.Remove(mt.walFile.Name()); err != nil {
		return fmt.Errorf("failed to remove wal file: %w", err)
	}

	return nil
}
