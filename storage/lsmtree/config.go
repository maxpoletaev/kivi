package lsmtree

import (
	"io/fs"

	"github.com/go-kit/log"
)

type Config struct {
	// Logger is the logger used to log the events inside the LSM-tree,
	// such as flushing memtables to disk. Defaults to a no-op logger.
	Logger log.Logger
	// DataRoot is the directory where the lsm-tree will be stored. Has no effect
	// if DataFS is specified. Defaults to the current working directory.
	DataRoot string
	// DataFS is the filesystem where the lsm-tree will be stored. If not specified,
	// the default filesystem will be used with DataRoot as the root.
	DataFS fs.FS
	// MaxMemtableRecords is the maximum number of entries in the memtable before
	// it is flushed to disk. Defaults to 1000.
	MaxMemtableRecords int
	// BloomFilterBytes is the size of the bloom filter in bytes. Defaults to 128KB.
	BloomFilterBytes int
	// BloomFilterHashers is the number of hashers used in the bloom filter. Defaults to 10.
	BloomFilterHashFuncs int
	// SparseIndexGapBytes is the size of the gap in bytes between the index entries in the
	// sparse index. Larger gaps result in smaller index files, but slower lookups. Defaults
	// to 64KB.
	SparseIndexGapBytes int64
	// MmapDataFiles enables memory mapping of the data file. Although it may have a positive
	// impact on performance due to reduced number of syscalls, it is generally advised not to
	// use mmap in databases, so it is disabled by default. Please check out the following
	// paper for more details: https://db.cs.cmu.edu/mmap-cidr2022/
	MmapDataFiles bool
}

func DefaultConfig() Config {
	return Config{
		Logger:               log.NewNopLogger(),
		BloomFilterBytes:     128 * 1024, // 128KB
		SparseIndexGapBytes:  64 * 1024,  // 64KB
		MmapDataFiles:        false,
		MaxMemtableRecords:   1000,
		BloomFilterHashFuncs: 10,
	}
}
