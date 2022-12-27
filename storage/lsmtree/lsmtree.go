package lsmtree

import (
	"container/list"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
)

type LSMTree struct {
	mut             sync.RWMutex
	memtable        *Memtable
	ssTables        *list.List // *SSTable
	flushQueue      *list.List // *Memtable
	logger          log.Logger
	dataRoot        string
	flushInProgress uint32
	sparseIndexGap  int64
	mmapDataFiles   bool
	maxMemtableSize int
	bloomFilterSize int
	bloomHashers    int
}

func New(conf Config) (*LSMTree, error) {
	logger := log.With(conf.Logger, "component", "lsmtree")
	flushQueue := list.New()
	ssTables := list.New()

	segments, err := listSegments(conf.DataRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to list segments: %w", err)
	}

	var memtable *Memtable

	for _, seg := range segments {
		if _, err := os.Stat(seg.WalFile()); !os.IsNotExist(err) {
			logger.Log("msg", "restoring memtable from WAL", "wal", seg.WalFile())

			mt, err := restoreMemtable(seg.ID, seg.WalFile())
			if err != nil {
				return nil, err
			}

			if memtable != nil {
				flushQueue.PushBack(memtable)
				memtable = mt
			}

			memtable = mt

			continue
		}

		logger.Log(
			"msg", "loading SSTable",
			"data", seg.DataFile(),
			"mmap", conf.MmapDataFiles,
		)

		loader := &SSTableLoader{
			IndexFilePath:      seg.IndexFile(),
			DataFilePath:       seg.DataFile(),
			BloomFilePath:      seg.BloomFile(),
			BloomFilterSize:    conf.BloomFilterBytes,
			BloomFilterHashers: conf.BloomFilterHashFuncs,
			MmapDataFile:       conf.MmapDataFiles,
		}

		sstable, err := loader.Load()
		if err != nil {
			return nil, err
		}

		ssTables.PushBack(sstable)
	}

	if memtable == nil {
		logger.Log("msg", "creating new segment")

		seg := NewSegment(0, conf.DataRoot)

		mt, err := createMemtable(seg.ID, seg.WalFile())
		if err != nil {
			return nil, err
		}

		memtable = mt
	}

	lsm := &LSMTree{
		flushQueue: flushQueue,
		memtable:   memtable,
		ssTables:   ssTables,
		logger:     logger,

		dataRoot:        conf.DataRoot,
		mmapDataFiles:   conf.MmapDataFiles,
		maxMemtableSize: conf.MaxMemtableRecords,
		bloomFilterSize: conf.BloomFilterBytes,
		bloomHashers:    conf.BloomFilterHashFuncs,
		sparseIndexGap:  conf.SparseIndexGapBytes,
	}

	if flushQueue.Len() > 0 {
		level.Info(logger).Log("msg", "flushing memtables")

		// In case there are segments to be flushed, we wait for this to finish.
		// This is ok to do synchronously here since we are only starting.
		if err := lsm.flush(); err != nil {
			return nil, err
		}
	}

	return lsm, nil
}

func (lsm *LSMTree) flushIfNeeded() error {
	lsm.mut.RLock()
	currentLen := lsm.memtable.Len()
	lsm.mut.RUnlock()

	if currentLen >= lsm.maxMemtableSize {
		lsm.mut.Lock()

		// Check again, as it might have been flushed already.
		if lsm.memtable.Len() < lsm.maxMemtableSize {
			lsm.mut.Unlock()
			return nil
		}

		nextID := lsm.memtable.ID() + 1

		seg := NewSegment(nextID, lsm.dataRoot)

		mt, err := createMemtable(seg.ID, seg.WalFile())
		if err != nil {
			lsm.mut.Unlock()
			return err
		}

		lsm.flushQueue.PushBack(lsm.memtable)
		lsm.memtable = mt
		lsm.mut.Unlock()

		// Fixme: potential goroutine leak here if we flush too often.
		// We should probably use a channel to signal the flusher to start.
		go func() {
			if err := lsm.flush(); err != nil {
				level.Error(lsm.logger).Log("msg", "failed to flush memtable", "err", err)
			}
		}()
	}

	return nil
}

func (lsm *LSMTree) flush() error {
	if !atomic.CompareAndSwapUint32(&lsm.flushInProgress, 0, 1) {
		return nil
	}

	for lsm.flushQueue.Len() > 0 {
		el := lsm.flushQueue.Front()

		mt := el.Value.(*Memtable)

		seg := NewSegment(mt.ID(), lsm.dataRoot)

		lsm.logger.Log("msg", "flushing segment", "id", mt.ID())

		flusher := &SSTableFlusher{
			IndexFilePath:      seg.IndexFile(),
			DataFilePath:       seg.DataFile(),
			BloomFilePath:      seg.BloomFile(),
			SparseIndexGap:     lsm.sparseIndexGap,
			BloomFilterSize:    lsm.bloomFilterSize,
			BloomFilterHashers: lsm.bloomHashers,
		}

		// Flush the memtable to disk as an sstable.
		if err := flusher.Flush(mt); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}

		loader := &SSTableLoader{
			IndexFilePath:      seg.IndexFile(),
			DataFilePath:       seg.DataFile(),
			BloomFilePath:      seg.BloomFile(),
			BloomFilterSize:    lsm.bloomFilterSize,
			BloomFilterHashers: lsm.bloomHashers,
			MmapDataFile:       true,
		}

		// Load it back into memory as an sstable.
		sstable, err := loader.Load()
		if err != nil {
			return fmt.Errorf("failed to load sstable: %w", err)
		}

		lsm.mut.Lock()

		lsm.ssTables.PushBack(sstable)

		lsm.flushQueue.Remove(el)

		lsm.mut.Unlock()

		mt.Discard()
	}

	atomic.StoreUint32(&lsm.flushInProgress, 0)

	return nil
}

func (lsm *LSMTree) Get(key string) (*proto.DataEntry, bool, error) {
	lsm.mut.RLock()
	defer lsm.mut.RUnlock()

	// Search in the current memtable.
	if value, found := lsm.memtable.Get(key); found {
		return value, true, nil
	}

	// Search in the memtables that are waiting to be flushed, from newest to oldest.
	for el := lsm.flushQueue.Back(); el != nil; el = el.Prev() {
		mt := el.Value.(*Memtable)
		if value, found := mt.Get(key); found {
			return value, true, nil
		}
	}

	// Search in the sstables on disk, from newest to oldest.
	for el := lsm.ssTables.Back(); el != nil; el = el.Prev() {
		sst := el.Value.(*SSTable)

		if value, found, err := sst.Get(key); err != nil {
			return nil, false, fmt.Errorf("failed to get key: %w", err)
		} else if found {
			return value, true, nil
		}
	}

	return nil, false, nil
}

func (lsm *LSMTree) Put(entry *proto.DataEntry) error {
	if err := lsm.flushIfNeeded(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	lsm.memtable.Put(entry)

	return nil
}

func (lsm *LSMTree) Close() error {
	return nil
}
