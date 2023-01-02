package lsmtree

import (
	"container/list"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
)

type LSMTree struct {
	dataRoot   string
	memtable   *Memtable
	flushQueue *list.List // *Memtable
	ssTables   *list.List // *SSTable
	wg         sync.WaitGroup
	mut        sync.RWMutex
	logger     log.Logger
	conf       Config
	inFlush    int32
	nextID     int
}

func New(conf Config) (*LSMTree, error) {
	logger := log.With(conf.Logger, "component", "lsm")
	flushQueue := list.New()
	sstables := list.New()

	segments, err := listSegments(conf.DataRoot)
	if err != nil {
		return nil, err
	}

	for _, seg := range segments {
		if !seg.valid() {
			level.Warn(logger).Log("msg", "invalid segment", "id", seg.id, "level", seg.level)
			continue
		}

		level.Info(logger).Log("msg", "loading segment", "id", seg.id, "level", seg.level)

		sst, err := OpenSSTable(TableOpts{
			IndexPath:    seg.indexPath,
			DataPath:     seg.dataPath,
			BloomPath:    seg.bloomPath,
			MmapDataFile: conf.MmapDataFiles,
			BloomSize:    conf.BloomFilterBytes,
			IndexGap:     conf.SparseIndexGapBytes,
			BloomFuncs:   conf.BloomFilterHashFuncs,
		})
		if err != nil {
			return nil, err
		}

		sstables.PushBack(sst)
	}

	wals, err := listWALs(conf.DataRoot)
	if err != nil {
		return nil, err
	}

	for _, wal := range wals {
		mt, err := RestoreMemtable(wal.path)
		if err != nil {
			return nil, err
		}

		flushQueue.PushBack(mt)
	}

	var nextID int
	if len(segments) > 0 {
		nextID = segments[len(segments)-1].id + 1
	}

	lsm := &LSMTree{
		dataRoot:   conf.DataRoot,
		flushQueue: flushQueue,
		ssTables:   sstables,
		nextID:     nextID,
		logger:     logger,
		conf:       conf,
	}

	if flushQueue.Len() > 0 {
		if lsm.flush(); err != nil {
			return nil, err
		}
	}

	return lsm, nil
}

func (lsm *LSMTree) flushIfNeeded() error {
	lsm.mut.RLock()

	if lsm.memtable == nil {
		lsm.mut.RUnlock()
		return nil
	}

	// If the memtable is not full, do nothing.
	if lsm.memtable.Size() < lsm.conf.MaxMemtableSize {
		lsm.mut.RUnlock()
		return nil
	}

	lsm.mut.RUnlock()
	lsm.mut.Lock()
	defer lsm.mut.Unlock()

	// Check again, in case the memtable was flushed by another goroutine.
	if lsm.memtable.Size() < lsm.conf.MaxMemtableSize {
		return nil
	}

	lsm.flushQueue.PushBack(lsm.memtable)

	lsm.memtable = nil

	lsm.wg.Add(1)

	go func() {
		defer lsm.wg.Done()

		if err := lsm.flush(); err != nil {
			level.Error(lsm.logger).Log("msg", "failed to flush memtable", "err", err)
		}
	}()

	return nil
}

func (lsm *LSMTree) flush() error {
	if !atomic.CompareAndSwapInt32(&lsm.inFlush, 0, 1) {
		return nil
	}

	lsm.mut.Lock()
	defer lsm.mut.Unlock()

	for lsm.flushQueue.Len() > 0 {
		mt := lsm.flushQueue.Front().Value.(*Memtable)
		lsm.flushQueue.Remove(lsm.flushQueue.Front())

		if err := lsm.flushTable(mt); err != nil {
			return err
		}
	}

	atomic.StoreInt32(&lsm.inFlush, 0)

	return nil
}

func (lsm *LSMTree) flushTable(mt *Memtable) error {
	level.Info(lsm.logger).Log("msg", "flushing segment", "level", lsm.nextID)

	opts := TableOpts{
		IndexPath:    filepath.Join(lsm.dataRoot, fmt.Sprintf("%05d.L0.index", lsm.nextID)),
		DataPath:     filepath.Join(lsm.dataRoot, fmt.Sprintf("%05d.L0.data", lsm.nextID)),
		BloomPath:    filepath.Join(lsm.dataRoot, fmt.Sprintf("%05d.L0.bloom", lsm.nextID)),
		IndexGap:     lsm.conf.SparseIndexGapBytes,
		MmapDataFile: lsm.conf.MmapDataFiles,
		BloomSize:    lsm.conf.BloomFilterBytes,
		BloomFuncs:   lsm.conf.BloomFilterHashFuncs,
	}

	// Flush the memtable to disk. This will close the memtable and remove the WAL file.
	// As long as it's in the list, the memtable remains readable. At this point, the active
	// memtable is already replaced with a new one, so no new writes will be added to this one.
	if err := mt.Flush(opts); err != nil {
		return err
	}

	// And open it back as an SSTable with the same options.
	sst, err := OpenSSTable(opts)
	if err != nil {
		return err
	}

	lsm.ssTables.PushBack(sst)

	lsm.nextID++

	return nil
}

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
	defer lsm.mut.Unlock()

	// If there is no active memtable, create one. We postpone this operation until the first
	// write, so that we don't create an empty wal file if there are no writes at all.
	if lsm.memtable == nil {
		walName := fmt.Sprintf("%d.wal", time.Now().UnixMilli())

		mt, err := CreateMemtable(filepath.Join(lsm.dataRoot, walName))
		if err != nil {
			return fmt.Errorf("failed to create memtable: %w", err)
		}

		lsm.memtable = mt
	}

	if err := lsm.memtable.Put(entry); err != nil {
		return fmt.Errorf("failed to put entry: %w", err)
	}

	return nil
}

func (lsm *LSMTree) Put(entry *proto.DataEntry) error {
	if err := lsm.flushIfNeeded(); err != nil {
		return err
	}

	if err := lsm.putToMem(entry); err != nil {
		return err
	}

	return nil
}

func (lsm *LSMTree) Close() error {
	lsm.wg.Wait()
	return nil
}
