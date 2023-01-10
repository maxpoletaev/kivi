package lsmtree

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
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
	stop       chan struct{}
	state      *loggedState
	logger     log.Logger
	conf       Config
	inFlush    int32
}

// Create initializes a new LSM-Tree instance in the directory given in the config.
// It restores the state of the tree from the previous run if it exists. Otherwise
// it creates a new tree.
func Create(conf Config) (*LSMTree, error) {
	logger := log.With(conf.Logger, "component", "lsm")
	flushQueue := list.New()
	sstables := list.New()

	state, err := newLoggedState(conf.DataRoot)
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
		stop:       make(chan struct{}),
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
	}

	return lsm, nil
}

func (lsm *LSMTree) sheduleFlush() error {
	lsm.mut.RLock()

	// The memtable is not full yet, no need to flush.
	if lsm.memtable == nil || lsm.memtable.Size() < lsm.conf.MaxMemtableSize {
		lsm.mut.RUnlock()
		return nil
	}

	// Reacquire the lock for writing, as we are goint to swap the memtable.
	lsm.mut.RUnlock()
	lsm.mut.Lock()

	// Check again, in case the memtable was flushed by another goroutine.
	if lsm.memtable.Size() < lsm.conf.MaxMemtableSize {
		lsm.mut.Unlock()
		return nil
	}

	// Close the memtable to make it read-only.
	if err := lsm.memtable.Close(); err != nil {
		lsm.mut.Unlock()
		return fmt.Errorf("failed to close memtable: %w", err)
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

		// Flush the memtable to disk. This will close the memtable and remove the WAL file.
		// As long as it's in the list, the memtable remains readable. At this point, the active
		// memtable is already replaced with a new one, so no new writes will be added to this one.
		sst, err := flushToDisk(memt, flushOpts{
			bloomProb: lsm.conf.BloomFilterProbability,
			indexGap:  lsm.conf.SparseIndexGapBytes,
			tableID:   time.Now().UnixMicro(),
			prefix:    lsm.dataRoot,
		})
		if err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}

		// Atomically replace the memtable with the ssTable, and reflect that in the state.
		if err := func() error {
			lsm.mut.Lock()
			defer lsm.mut.Unlock()

			if err := lsm.state.MemtableFlushed(memt.ID, sst.SSTableInfo); err != nil {
				return fmt.Errorf("failed to log segment flushed: %w", err)
			}

			lsm.flushQueue.Remove(el)
			lsm.ssTables.PushBack(sst)

			return nil
		}(); err != nil {
			return err
		}

		// Discard the memtable. This will remove the WAL file.
		if err := memt.Discard(); err != nil {
			return fmt.Errorf("failed to discard memtable: %w", err)
		}
	}
}

// Get returns the value for the given key, if it exists. It checks the active memtable first,
// then the memtables that are waiting to be flushed, and finally the sstables on disk. Note that
// the retuned entry is a pointer to the actual entry in the memtable or sstable, so it should not
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

			if err := lsm.state.MemtableCreated(memt.MemtableInfo); err != nil {
				lsm.mut.Unlock()
				_ = memt.CloseAndDiscard()
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
	if err := lsm.sheduleFlush(); err != nil {
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
	close(lsm.stop)
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
