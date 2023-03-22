package opengroup

import (
	"os"

	"github.com/maxpoletaev/kivi/internal/multierror"
)

type Opener struct {
	files  map[string]*os.File
	errors multierror.Error[string]
}

func New() *Opener {
	return &Opener{
		files: make(map[string]*os.File),
	}
}

func (o *Opener) Open(name string, flag int, mode os.FileMode) *os.File {
	f, err := os.OpenFile(name, flag, mode)
	if err != nil {
		o.errors.Add(name, err)
		return nil
	}

	o.files[name] = f

	return f
}

func (o *Opener) Close(name string) error {
	f, ok := o.files[name]
	if !ok {
		return nil
	}

	delete(o.files, name)

	return f.Close()
}

func (o *Opener) Err() error {
	return o.errors.Combined()
}

func (o *Opener) CloseAll() error {
	errs := multierror.New[string]()

	for name := range o.files {
		if e := o.Close(name); e != nil {
			errs.Add(name, e)
		}
	}

	return errs.Combined()
}

func (o *Opener) RemoveAll() error {
	errs := multierror.New[string]()

	for name := range o.files {
		if e := os.Remove(name); e != nil {
			errs.Add(name, e)
		}
	}

	return errs.Combined()
}
