package filegroup

import (
	"os"

	"github.com/maxpoletaev/kivi/internal/multierror"
)

// FileGroup is a helper for opening and closing multiple files at once.
type FileGroup struct {
	files  map[string]*os.File
	errors multierror.Error[string]
}

// New creates a new FileGroup.
func New() *FileGroup {
	return &FileGroup{
		files: make(map[string]*os.File),
	}
}

// Open opens a file and adds it to the FileGroup.
func (fg *FileGroup) Open(name string, flag int, mode os.FileMode) *os.File {
	f, err := os.OpenFile(name, flag, mode)
	if err != nil {
		fg.errors.Add(name, err)
		return nil
	}

	fg.files[name] = f

	return f
}

// OpenErr returns an error if any of the files in the FileGroup failed to open.
func (fg *FileGroup) OpenErr() error {
	return fg.errors.Combined()
}

// Close closes all files in the FileGroup.
func (fg *FileGroup) Close() error {
	errs := multierror.New[string]()

	for name, f := range fg.files {
		if err := f.Close(); err != nil {
			errs.Add(name, err)
		}
	}

	return errs.Combined()
}

// Cleanup removes all files that were created by the FileGroup.
func (fg *FileGroup) Cleanup() error {
	errs := multierror.New[string]()

	for name := range fg.files {
		if e := os.Remove(name); e != nil {
			errs.Add(name, e)
		}
	}

	return errs.Combined()
}

// Sync flushes all files in the FileGroup to disk.
func (fg *FileGroup) Sync() error {
	errs := multierror.New[string]()

	for name, f := range fg.files {
		if e := f.Sync(); e != nil {
			errs.Add(name, e)
		}
	}

	return errs.Combined()
}
