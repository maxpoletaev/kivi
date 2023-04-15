package filegroup

import (
	"os"

	"github.com/maxpoletaev/kivi/internal/multierror"
)

type FileGroup struct {
	files  map[string]*os.File
	errors multierror.Error[string]
}

func New() *FileGroup {
	return &FileGroup{
		files: make(map[string]*os.File),
	}
}

func (fg *FileGroup) Open(name string, flag int, mode os.FileMode) *os.File {
	f, err := os.OpenFile(name, flag, mode)
	if err != nil {
		fg.errors.Add(name, err)
		return nil
	}

	fg.files[name] = f

	return f
}

func (fg *FileGroup) Err() error {
	return fg.errors.Combined()
}

func (fg *FileGroup) Close() error {
	errs := multierror.New[string]()

	for name, f := range fg.files {
		if err := f.Close(); err != nil {
			errs.Add(name, err)
		}
	}

	return errs.Combined()
}

// Remove removes all files that were created by the FileGroup.
func (fg *FileGroup) Remove() error {
	errs := multierror.New[string]()

	for name := range fg.files {
		if e := os.Remove(name); e != nil {
			errs.Add(name, e)
		}
	}

	return errs.Combined()
}

func (fg *FileGroup) Sync() error {
	errs := multierror.New[string]()

	for name, f := range fg.files {
		if e := f.Sync(); e != nil {
			errs.Add(name, e)
		}
	}

	return errs.Combined()
}
