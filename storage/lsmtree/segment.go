package lsmtree

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type segmentDescriptor struct {
	id        int
	level     int
	indexPath string
	dataPath  string
	bloomPath string
}

func (s *segmentDescriptor) valid() bool {
	return s.indexPath != "" && s.dataPath != "" && s.bloomPath != ""
}

func listSegments(path string) ([]segmentDescriptor, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// Map of segment id to segment file set.
	segmentsByID := make(map[int]*segmentDescriptor)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// The file name is in the format of <id>.L<level>.<type>
		parts := strings.Split(file.Name(), ".")
		if len(parts) != 3 {
			continue
		}

		var (
			idPart    = parts[0]
			levelPart = parts[1]
			extPart   = parts[2]
		)

		// The id is a number, but it may have leading zeros.
		id, err := strconv.Atoi(idPart)
		if err != nil {
			continue
		}

		// The level is a number prefixed with an "L".
		level, err := strconv.Atoi(levelPart[1:])
		if err != nil || levelPart[0] != 'L' {
			continue
		}

		// Id is unique, but level is not, there may be multiple segments on the same level.
		// Each segment is made up of multiple files, such as index and data files.
		if _, ok := segmentsByID[id]; !ok {
			segmentsByID[id] = &segmentDescriptor{
				id:    id,
				level: level,
			}
		}

		filePath := filepath.Join(path, file.Name())
		segment := segmentsByID[id]

		switch extPart {
		case "index":
			segment.indexPath = filePath
		case "data":
			segment.dataPath = filePath
		case "bloom":
			segment.bloomPath = filePath
		default:
			continue
		}
	}

	segments := make([]segmentDescriptor, 0)
	for _, fileset := range segmentsByID {
		if fileset.valid() {
			segments = append(segments, *fileset)
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].id < segments[j].id
	})

	return segments, nil
}

type walDescriptor struct {
	timestamp int64
	path      string
}

func listWALs(path string) ([]walDescriptor, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	wals := make([]walDescriptor, 0)
	for _, file := range files {

		// The file name is in the format of <timestamp>.wal
		parts := strings.Split(file.Name(), ".")
		if len(parts) != 2 {
			continue
		}

		var (
			tsPart  = parts[0]
			extPart = parts[1]
		)

		if extPart != "wal" {
			continue
		}

		timestamp, err := strconv.ParseInt(tsPart, 10, 64)
		if err != nil {
			continue
		}

		wals = append(wals, walDescriptor{
			path:      filepath.Join(path, file.Name()),
			timestamp: timestamp,
		})
	}

	sort.Slice(wals, func(i, j int) bool {
		return wals[i].timestamp < wals[j].timestamp
	})

	return wals, nil
}
