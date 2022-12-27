package lsmtree

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Segment struct {
	ID   int
	Path string
}

func (s *Segment) WalFile() string {
	return fmt.Sprintf("%s/%s.wal", s.Path, formatSegmentID(s.ID))
}

func (s *Segment) IndexFile() string {
	return fmt.Sprintf("%s/%s.index", s.Path, formatSegmentID(s.ID))
}

func (s *Segment) DataFile() string {
	return fmt.Sprintf("%s/%s.data", s.Path, formatSegmentID(s.ID))
}

func (s *Segment) BloomFile() string {
	return fmt.Sprintf("%s/%s.bloom", s.Path, formatSegmentID(s.ID))
}

func NewSegment(id int, path string) Segment {
	return Segment{
		ID:   id,
		Path: path,
	}
}

func listSegments(path string) ([]Segment, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	segments := make([]Segment, 0)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var name string
		if strings.HasSuffix(file.Name(), ".wal") {
			name = strings.TrimSuffix(file.Name(), ".wal")
		} else if strings.HasSuffix(file.Name(), ".index") {
			name = strings.TrimSuffix(file.Name(), ".index")
		} else {
			continue
		}

		id, err := parseSegmentID(name)
		if err != nil {
			return nil, err
		}

		segments = append(segments, NewSegment(id, path))
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID < segments[j].ID
	})

	return segments, nil
}

func formatSegmentID(id int) string {
	return fmt.Sprintf("%03d", id)
}

func parseSegmentID(name string) (int, error) {
	name = strings.TrimLeft(name, "0")
	if name == "" {
		return 0, nil
	}

	return strconv.Atoi(name)
}
