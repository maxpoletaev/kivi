package lsmtree

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kivi/storage/lsmtree/proto"
)

// LSMTree is a persistent key-value store based on the LSM-Tree data structure. It is a
// write-optimized, which means that it is optimized for writes, but reads may be slow.
type LSMTree struct {
	dataRoot   string
	memtable   *Memtable
	flushQueue *list.List // *Memtable
	ssTables   *list.List // *SSTable
	wg         sync.WaitGroup
	mut        sync.RWMutex
	quit       chan struct{}
	state      *loggedState
	logger     log.Logger
	conf       Config
	dirty      int32
	inFlush    int32
}

// Create initializes a new LSM-Tree instance in the directory given in the config.
// It restores the state of the tree from the previous run if it exists. Otherwise
// it creates a new tree.
func Create(conf Config) (*LSMTree, error) {
	var (
		logger     = log.With(conf.Logger, "component", "lsm")
		stateFile  = filepath.Join(conf.DataRoot, "STATE")
		flushQueue = list.New()
		sstables   = list.New()
	)

	state, err := newLoggedState(stateFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create state: %w", err)
	}

	// Restore the state of the tree from the previous run.
	for _, info := range state.SSTables() {
		sst, err := OpenTable(info, conf.DataRoot, conf.MmapDataFiles)
		if err != nil {
			return nil, fmt.Errorf("failed to open sstable: %w", err)
		}

		sstables.PushBack(sst)
	}

	// In case there are wal files left from the previous run, we need to restore
	// the memtables and flush them to the disk. This may potetially create a lot
	// of small sstables, but the compaction should take care of it eventually.
	for _, info := range state.Memtables() {
		memt, err := openMemtable(info, conf.DataRoot)
		if err != nil {
			return nil, fmt.Errorf("failed to restore memtable: %w", err)
		}

		flushQueue.PushBack(memt)
	}

	lsm := &LSMTree{
		quit:       make(chan struct{}),
		dataRoot:   conf.DataRoot,
		flushQueue: flushQueue,
		ssTables:   sstables,
		logger:     logger,
		state:      state,
		conf:       conf,
	}

	// Wait for the flush to finish before returning, so that we have no memtables
	// in the queue when the tree is ready to use.
	if flushQueue.Len() > 0 {
		if err := lsm.flushWaiting(); err != nil {
			return nil, err
		}

		for _, rule := range lsm.conf.CompactionRules {
			if err := lsm.compactLevel(&rule); err != nil {
				return nil, err
			}
		}

		if err := lsm.collectGarbage(); err != nil {
			return nil, err
		}
	}

	// Periodically sync the contents of the WAL to disk.
	lsm.startSyncLoop()

	return lsm, nil
}

func (lsm *LSMTree) scheduleFlush() error {
	lsm.mut.RLock()

	// The memtable is not full yet, no need to flush.
	if lsm.memtable == nil || lsm.memtable.Size() < lsm.conf.MaxMemtableSize {
		lsm.mut.RUnlock()
		return nil
	}

	// Reacquire the lock for writing, as we are going to swap the memtable.
	lsm.mut.RUnlock()
	lsm.mut.Lock()

	// Check again, in case the memtable was flushed by another goroutine.
	if lsm.memtable.Size() < lsm.conf.MaxMemtableSize {
		lsm.mut.Unlock()
		return nil
	}

	// The active memtable is moved to the flush queue and will be flushed to disk in background.
	lsm.flushQueue.PushBack(lsm.memtable)
	lsm.memtable = nil
	lsm.mut.Unlock()

	// Start a background goroutine to flush the memtable to disk, only
	// if there is no other flush in progress.
	if atomic.CompareAndSwapInt32(&lsm.inFlush, 0, 1) {
		lsm.wg.Add(1)

		go func() {
			defer lsm.wg.Done()
			defer atomic.StoreInt32(&lsm.inFlush, 0)

			if err := lsm.flushWaiting(); err != nil {
				level.Error(lsm.logger).Log("msg", "failed to flush memtable", "err", err)
				return
			}

			for _, rule := range lsm.conf.CompactionRules {
				if err := lsm.compactLevel(&rule); err != nil {
					level.Error(lsm.logger).Log("msg", "failed to compact", "err", err)
					return
				}
			}

			if err := lsm.collectGarbage(); err != nil {
				level.Error(lsm.logger).Log("msg", "failed to collect garbage", "err", err)
				return
			}
		}()
	}

	return nil
}

func (lsm *LSMTree) flushWaiting() error {
	for {
		lsm.mut.Lock()

		if lsm.flushQueue.Len() == 0 {
			lsm.mut.Unlock()
			return nil
		}

		el := lsm.flushQueue.Front()
		memt := el.Value.(*Memtable)

		// Unlock before flushing, so that we can continue accepting writes.
		lsm.mut.Unlock()

		level.Info(lsm.logger).Log("msg", "flushing memtable to disk", "id", memt.ID)

		// Flush the memtable to disk. As long as it's in the list, the memtable remains readable.
		// At this point, the active memtable is already replaced with a new one, so no new writes
		// will be added to this one.
		sst, err := flushToDisk(memt, flushOpts{
			bloomProb: lsm.conf.BloomFilterProbability,
			indexGap:  lsm.conf.SparseIndexGapBytes,
			tableID:   time.Now().UnixMicro(),
			prefix:    lsm.dataRoot,
		})
		if err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}

		lsm.mut.Lock()

		err = lsm.state.LogMemtableFlushed(memt.ID, sst.SSTableInfo)
		if err != nil {
			lsm.mut.Unlock()
			return fmt.Errorf("failed to log segment flushed: %w", err)
		}

		// Swap the memtable with an ssTable.
		lsm.flushQueue.Remove(el)
		lsm.ssTables.PushBack(sst)

		lsm.mut.Unlock()

		// Discard the memtable. This will remove the WAL file.
		if err := memt.Discard(); err != nil {
			return fmt.Errorf("failed to discard memtable: %w", err)
		}
	}
}

func (lsm *LSMTree) compactLevel(rule *CompactionRule) error {
	lsm.mut.RLock()

	var (
		toMerge   []*SSTable
		levelSize int64
	)

	for el := lsm.ssTables.Front(); el != nil; el = el.Next() {
		sst := el.Value.(*SSTable)

		if sst.Level == rule.Level {
			toMerge = append(toMerge, sst)
			levelSize += sst.Size
		}
	}

	lsm.mut.RUnlock()

	switch {
	case len(toMerge) < 2:
		return nil // Nothing to compact.
	case rule.MaxSegments == 0 && rule.MaxLevelSize == 0:
		return nil // Empty/invalid compaction rule.
	case rule.MaxSegments > 0 && len(toMerge) < rule.MaxSegments:
		return nil // Not enough segments to compact.
	case rule.MaxLevelSize > 0 && levelSize < rule.MaxLevelSize:
		return nil // Level is not big enough to compact.
	}

	level.Info(lsm.logger).Log(
		"msg", "compacting",
		"level", rule.Level,
		"num_segments", len(toMerge),
		"target_level", rule.TargetLevel,
	)

	sst, err := mergeTables(toMerge, flushOpts{
		bloomProb: lsm.conf.BloomFilterProbability,
		indexGap:  lsm.conf.SparseIndexGapBytes,
		mmapOpen:  lsm.conf.MmapDataFiles,
		tableID:   time.Now().UnixMilli(),
		level:     rule.TargetLevel,
		prefix:    lsm.dataRoot,
	})

	if err != nil {
		return fmt.Errorf("failed to merge tables: %w", err)
	}

	lsm.mut.Lock()
	defer lsm.mut.Unlock()

	mergedIDs := make([]int64, len(toMerge))
	for i, sst := range toMerge {
		mergedIDs[i] = sst.ID
	}

	if err := lsm.state.LogSSTablesMerged(mergedIDs, sst.SSTableInfo); err != nil {
		return fmt.Errorf("failed to log segment compacted: %w", err)
	}

	mergedSet := make(map[int64]struct{}, len(mergedIDs))
	for _, id := range mergedIDs {
		mergedSet[id] = struct{}{}
	}

	// Remove the merged tables from the list.
	for el := lsm.ssTables.Front(); el != nil; el = el.Next() {
		sst := el.Value.(*SSTable)

		if _, ok := mergedSet[sst.ID]; ok {
			lsm.ssTables.Remove(el)
		}
	}

	// Add the new table to the list.
	lsm.ssTables.PushBack(sst)

	return nil
}

func (lsm *LSMTree) sync() error {
	// We use an atomic here to avoid taking the lock if we don't need to.
	if !atomic.CompareAndSwapInt32(&lsm.dirty, 1, 0) {
		return nil
	}

	lsm.mut.Lock()
	defer lsm.mut.Unlock()

	if lsm.memtable != nil {
		if err := lsm.memtable.Sync(); err != nil {
			return err
		}
	}

	return nil
}

func (lsm *LSMTree) startSyncLoop() {
	lsm.wg.Add(1)

	go func() {
		defer lsm.wg.Done()

		for {
			select {
			case <-lsm.quit:
				return
			case <-time.After(1 * time.Second):
				if err := lsm.sync(); err != nil {
					level.Error(lsm.logger).Log("msg", "failed to sync memtable", "err", err)
				}
			}
		}
	}()
}

func (lsm *LSMTree) collectGarbage() error {
	lsm.mut.RLock()
	toRemove := make([]*SSTableInfo, len(lsm.state.Garbage()))
	copy(toRemove, lsm.state.Garbage())
	lsm.mut.RUnlock()

	if len(toRemove) == 0 {
		return nil
	}

	var (
		statePath    = filepath.Join(lsm.dataRoot, "STATE")
		newStatePath = filepath.Join(lsm.dataRoot, "STATE.new")
	)

	// Create a new state file next to the old one.
	newState, err := newLoggedState(newStatePath)
	if err != nil {
		return fmt.Errorf("failed to create new logged state: %w", err)
	}

	unlockOnReturn := true
	lsm.mut.Lock()

	defer func() {
		if unlockOnReturn {
			lsm.mut.Unlock()
		}
	}()

	// Copy the actual sstables to the new state.
	for _, sstInfo := range lsm.state.SSTables() {
		if err := newState.LogSSTablesMerged(nil, sstInfo); err != nil {
			return fmt.Errorf("failed write to the new state: %w", err)
		}
	}

	// Copy the active memtables to the new state.
	for _, memtInfo := range lsm.state.Memtables() {
		if err := newState.LogMemtableCreated(memtInfo); err != nil {
			return fmt.Errorf("failed write to the new state: %w", err)
		}
	}

	_ = lsm.state.Close()
	_ = newState.Close()

	// Replace the old state file with the new one.
	if err := os.Rename(newStatePath, statePath); err != nil {
		return fmt.Errorf("failed to rename new state file: %w", err)
	}

	// Reopen the state file and replace the state in the LSM tree.
	lsm.state, err = newLoggedState(statePath)
	if err != nil {
		return fmt.Errorf("failed to open new state file: %w", err)
	}

	// Release the lock before we start removing the garbage files, so that we don’t
	// block the tree for too long while doing I/O.
	unlockOnReturn = false
	lsm.mut.Unlock()

	for _, sstInfo := range toRemove {
		filesToRemove := []string{sstInfo.IndexFile, sstInfo.DataFile, sstInfo.BloomFile}
		level.Debug(lsm.logger).Log("msg", "removing garbage sstable", "id", sstInfo.ID)

		for _, filename := range filesToRemove {
			if err := os.Remove(filepath.Join(lsm.dataRoot, filename)); err != nil {
				level.Error(lsm.logger).Log("msg", "failed to remove garbage file", "filename", filename, "err", err)
			}
		}
	}

	return nil
}

// Get returns the value for the given key, if it exists. It checks the active memtable first,
// then the memtables that are waiting to be flushed, and finally the sstables on disk. Note that
// the returned entry is a pointer to the actual entry in the memtable or sstable, so it should not
// be modified.
func (lsm *LSMTree) Get(key string) (*proto.DataEntry, bool, error) {
	lsm.mut.RLock()
	defer lsm.mut.RUnlock()

	// Check the active memtable first.
	if lsm.memtable != nil {
		if entry, found := lsm.memtable.Get(key); found {
			return entry, true, nil
		}
	}

	// Check the memtables that are waiting to be flushed, from newest to oldest.
	for el := lsm.flushQueue.Back(); el != nil; el = el.Prev() {
		mt := el.Value.(*Memtable)

		if entry, found := mt.Get(key); found {
			return entry, true, nil
		}
	}

	// Check the sstables on disk, from newest to oldest.
	for el := lsm.ssTables.Back(); el != nil; el = el.Prev() {
		sst := el.Value.(*SSTable)

		if entry, found, err := sst.Get(key); err != nil {
			return nil, false, err
		} else if found {
			return entry, true, nil
		}
	}

	return nil, false, nil
}

func (lsm *LSMTree) putToMem(entry *proto.DataEntry) error {
	for {
		lsm.mut.RLock()

		if lsm.memtable != nil {
			defer lsm.mut.RUnlock()

			if err := lsm.memtable.Put(entry); err != nil {
				return fmt.Errorf("failed to put entry: %w", err)
			}

			atomic.StoreInt32(&lsm.dirty, 1)

			return nil
		}

		lsm.mut.RUnlock()
		lsm.mut.Lock()

		// If there is no active memtable, create one. We postpone this operation until the first
		// write, so that we don't create an empty wal file if there are no writes at all.
		if lsm.memtable == nil {
			memt, err := createMemtable(lsm.dataRoot)
			if err != nil {
				lsm.mut.Unlock()
				return fmt.Errorf("failed to create memtable: %w", err)
			}

			if err := lsm.state.LogMemtableCreated(memt.MemtableInfo); err != nil {
				lsm.mut.Unlock()

				if err := memt.Discard(); err != nil {
					level.Error(lsm.logger).Log("msg", "failed to discard memtable", "err", err)
				}

				return fmt.Errorf("failed to log segment created: %w", err)
			}

			lsm.memtable = memt

			lsm.mut.Unlock()
		}
	}
}

// Put puts a data entry into the LSM tree. It will first check if the active memtable is full,
// and if so, it will create a new one and flush the old one to disk. If the memtable is not
// full, it will add the entry to the active memtable.
func (lsm *LSMTree) Put(entry *proto.DataEntry) error {
	if err := lsm.scheduleFlush(); err != nil {
		return err
	}

	if err := lsm.putToMem(entry); err != nil {
		return err
	}

	return nil
}

// Close closes the LSM tree. It will wait for all pending flushes to complete, and then close
// all the sstables and the state file. One should ensure that no reads or writes are happening
// when calling this method.
func (lsm *LSMTree) Close() error {
	close(lsm.quit)
	lsm.wg.Wait()

	lsm.mut.Lock()
	defer lsm.mut.Unlock()

	for sst := lsm.ssTables.Front(); sst != nil; sst = sst.Next() {
		if err := sst.Value.(*SSTable).Close(); err != nil {
			return fmt.Errorf("failed to close sstable: %w", err)
		}
	}

	if err := lsm.state.Close(); err != nil {
		return fmt.Errorf("failed to close state: %w", err)
	}

	return nil
}
