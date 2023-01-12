package lsmtree

import (
	"github.com/go-kit/log"
)

type CompactionRule struct {
	// Level is the level of the segment that will be compacted.
	Level int
	// TargetLevel is the level where the compacted segment will be placed.
	TargetLevel int
	// MaxSegments is the maximum number of segments that can be in the level.
	MaxSegments int
	// MaxLevelSize is the maximum size of the level in bytes.
	MaxLevelSize int64
}

type Config struct {
	// Logger is the logger used to log the events inside the LSM-tree,
	// such as flushing memtables to disk. Defaults to a no-op logger.
	Logger log.Logger
	// DataRoot is the directory where the lsm-tree will be stored. Has no effect
	// if DataFS is specified. Defaults to the current working directory.
	DataRoot string
	// MaxMemtableSize is the maximum number of entries in the memtable before
	// it is flushed to disk. Defaults to 1000.
	MaxMemtableSize int64
	// BloomFilterProbability is the probability of false positives in the bloom filter.
	// It will be used to dynamically calculate the number of hash functions and the size
	// of the bloom filter. Defaults to 0.01 which means that there is a 1% chance of
	// false positives.
	BloomFilterProbability float64
	// SparseIndexGapBytes is the size of the gap in bytes between the index entries in the
	// sparse index. Larger gaps result in smaller index files, but slower lookups. Defaults
	// to 64KB.
	SparseIndexGapBytes int64
	// MmapDataFiles enables memory mapping of the data file. Although it may have a positive
	// impact on performance due to reduced number of syscalls, it is generally advised not to
	// use mmap in databases, so it is disabled by default. Please check out the following
	// paper for more details: https://db.cs.cmu.edu/mmap-cidr2022/
	MmapDataFiles bool
	// CompactionRules is a list of compaction rules that will be used to determine
	// when to compact the segments. Defaults to a single rule that compacts the
	// segments in the level 0 when there are more than 10 of them.
	CompactionRules []CompactionRule
}

func DefaultConfig() Config {
	return Config{
		Logger:                 log.NewNopLogger(),
		SparseIndexGapBytes:    64 * 1024,        // 8KB
		MaxMemtableSize:        16 * 1024 * 1024, // 16MB
		MmapDataFiles:          false,
		BloomFilterProbability: 0.01,
		CompactionRules: []CompactionRule{
			{
				Level:       0,
				TargetLevel: 1,
				MaxSegments: 10,
			},
			{
				Level:       1,
				TargetLevel: 1,
				MaxSegments: 1,
			},
		},
	}
}
