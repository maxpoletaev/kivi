package lsmtree

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/maxpoletaev/kivi/internal/protoio"
	"github.com/maxpoletaev/kivi/storage/lsmtree/proto"
)

type MemtableInfo struct {
	ID      int64
	WALFile string
}

type SSTableInfo struct {
	ID         int64
	Level      int
	NumEntries int64
	Size       int64
	IndexFile  string
	DataFile   string
	BloomFile  string
}

// loggedState is a persistent state of the LSM-Tree keeping track of all memtables and sstables
// merges and flushes. It is mainly used to restore the state of the LSM-Tree after a restart,
// and in the garbage collection process, to determine which memtables and sstables are still
// in use. The state itself is stored as a sequence of changes in a log file. It is not safe
// to modify the state concurrently, so additional synchronization is required.
type loggedState struct {
	ActiveMemtables []*MemtableInfo
	ActiveSSTables  []*SSTableInfo
	MergedSSTables  []*SSTableInfo

	logWriter protoio.SequentialWriter
	logFile   *os.File
}

// newLoggedState creates a new state manager. If the log file already exists, the state will be
// restored from it, otherwise a new log file will be created. All changes are immediately
// flushed to the disk due to the file opened with O_SYNC flag.
func newLoggedState(filename string) (*loggedState, error) {
	logFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	sm := &loggedState{
		logFile:         logFile,
		logWriter:       protoio.NewWriter(logFile),
		ActiveMemtables: make([]*MemtableInfo, 0),
		ActiveSSTables:  make([]*SSTableInfo, 0),
	}

	// Try restoring the state from the log file if it is not empty.
	if stat, err := logFile.Stat(); err != nil {
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	} else if stat.Size() > 0 {
		if err = sm.restore(); err != nil {
			return nil, fmt.Errorf("failed restore state: %w", err)
		}
	}

	return sm, nil
}

func (ls *loggedState) applySegmentCreated(c *proto.SegmentCreated) {
	ls.ActiveMemtables = append(ls.ActiveMemtables, fromProtoMemtableInfo(c.Memtable))
}

func (ls *loggedState) applySegmentFlushed(c *proto.SegmentFlushed) {
	memtables := make([]*MemtableInfo, 0)

	for _, memtable := range ls.ActiveMemtables {
		if memtable.ID != c.MemtableId {
			memtables = append(memtables, memtable)
		}
	}

	ls.ActiveSSTables = append(ls.ActiveSSTables, fromProtoSSTableInfo(c.Sstable))
	ls.ActiveMemtables = memtables
}

func (ls *loggedState) applySegmentsMerged(c *proto.SegmentsMerged) {
	mergedIDs := make(map[int64]struct{})
	for _, id := range c.OldSstableIds {
		mergedIDs[id] = struct{}{}
	}

	active := make([]*SSTableInfo, 0, len(ls.ActiveSSTables)-len(mergedIDs))
	merged := make([]*SSTableInfo, 0, len(mergedIDs))

	for _, sstable := range ls.ActiveSSTables {
		if _, ok := mergedIDs[sstable.ID]; ok {
			merged = append(merged, sstable)
			continue
		}

		active = append(active, sstable)
	}

	ls.ActiveSSTables = append(active, fromProtoSSTableInfo(c.NewSstable))
	ls.MergedSSTables = append(ls.MergedSSTables, merged...)
}

func (ls *loggedState) applyChange(change *proto.StateLogEntry) {
	switch change.ChangeType {
	case *proto.StateChangeType_SEGMENT_CREATED.Enum():
		ls.applySegmentCreated(change.GetSegmentCreated())
	case *proto.StateChangeType_SEGMENT_FLUSHED.Enum():
		ls.applySegmentFlushed(change.GetSegmentFlushed())
	case *proto.StateChangeType_SEGMENTS_MERGED.Enum():
		ls.applySegmentsMerged(change.GetSegmentsMerged())
	}
}

func (ls *loggedState) restore() error {
	reader := protoio.NewReader(ls.logFile)
	change := &proto.StateLogEntry{}

	for {
		if _, err := reader.ReadNext(change); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("failed to read log record: %w", err)
		}

		ls.applyChange(change)

		change.Reset()
	}

	return nil
}

func (ls *loggedState) logAndApply(change *proto.StateLogEntry) error {
	change.Timestamp = time.Now().UnixMilli()

	if _, err := ls.logWriter.Append(change); err != nil {
		return fmt.Errorf("failed to log segment change: %w", err)
	}

	ls.applyChange(change)

	return nil
}

// LogMemtableCreated is called when a new memtable is created.
func (ls *loggedState) LogMemtableCreated(memtInfo *MemtableInfo) error {
	return ls.logAndApply(&proto.StateLogEntry{
		Timestamp:  time.Now().UnixMilli(),
		ChangeType: proto.StateChangeType_SEGMENT_CREATED,
		SegmentCreated: &proto.SegmentCreated{
			Memtable: toProtoMemtableInfo(memtInfo),
		},
	})
}

// LogMemtableFlushed is called when a memtable is flushed to a new sstable.
func (ls *loggedState) LogMemtableFlushed(memtID int64, sstInfo *SSTableInfo) error {
	return ls.logAndApply(&proto.StateLogEntry{
		Timestamp:  time.Now().UnixMilli(),
		ChangeType: proto.StateChangeType_SEGMENT_FLUSHED,
		SegmentFlushed: &proto.SegmentFlushed{
			Sstable:    toProtoSSTableInfo(sstInfo),
			MemtableId: memtID,
		},
	})
}

// LogSSTablesMerged is called when a set of sstables are merged into a new sstable.
func (ls *loggedState) LogSSTablesMerged(oldTableIDs []int64, newTableInfo *SSTableInfo) error {
	return ls.logAndApply(&proto.StateLogEntry{
		Timestamp:  time.Now().UnixMilli(),
		ChangeType: proto.StateChangeType_SEGMENTS_MERGED,
		SegmentsMerged: &proto.SegmentsMerged{
			NewSstable:    toProtoSSTableInfo(newTableInfo),
			OldSstableIds: oldTableIDs,
		},
	})
}

// Close closes the state manager.
func (ls *loggedState) Close() error {
	return ls.logFile.Close()
}
