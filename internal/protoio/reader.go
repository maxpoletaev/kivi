package protoio

import (
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"google.golang.org/protobuf/proto"
)

var ErrDataCorrupted = fmt.Errorf("data corrupted")

type Reader struct {
	file      io.ReaderAt
	mut       sync.Mutex
	entryBuf  []byte
	headerBuf []byte
	offset    int64
}

func NewReader(source io.ReaderAt) *Reader {
	return &Reader{
		file:      source,
		entryBuf:  make([]byte, 0),
		headerBuf: make([]byte, headerSize),
	}
}

func (r *Reader) readHeader(h *entryHeader) (int, error) {
	read, err := r.file.ReadAt(r.headerBuf, r.offset)
	if err != nil {
		return 0, err
	}

	if read != len(r.headerBuf) {
		return 0, io.ErrUnexpectedEOF
	}

	decodeHeader(h, r.headerBuf)

	r.offset += int64(read)

	return read, nil
}

func (r *Reader) readEntry(h entryHeader, entry proto.Message) (int, error) {
	if cap(r.entryBuf) < int(h.dataSize) {
		r.entryBuf = make([]byte, h.dataSize)
	} else {
		r.entryBuf = r.entryBuf[:h.dataSize]
	}

	read, err := r.file.ReadAt(r.entryBuf, r.offset)
	if err != nil {
		return 0, err
	}

	if uint64(read) != h.dataSize {
		return 0, io.ErrUnexpectedEOF
	}

	if h.crc != crc32.ChecksumIEEE(r.entryBuf) {
		return 0, ErrDataCorrupted
	}

	if err := proto.Unmarshal(r.entryBuf, entry); err != nil {
		return 0, err
	}

	r.offset += int64(read)

	return read, nil
}

func (r *Reader) read(msg proto.Message) (int, error) {
	var header entryHeader

	n1, err := r.readHeader(&header)
	if err != nil {
		return 0, err
	}

	n2, err := r.readEntry(header, msg)
	if err != nil {
		return 0, err
	}

	read := n1 + n2

	return read, nil
}

func (r *Reader) ReadAt(msg proto.Message, offset int64) (int, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	r.offset = offset

	return r.read(msg)
}

func (r *Reader) ReadNext(msg proto.Message) (int, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	n, err := r.read(msg)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (r *Reader) SkipN(n int) error {
	r.mut.Lock()
	defer r.mut.Unlock()

	var h entryHeader

	for i := 0; i < n; i++ {
		if _, err := r.readHeader(&h); err != nil {
			return err
		}

		r.offset += int64(h.dataSize)
	}

	return nil
}

func (r *Reader) Skip() error {
	r.mut.Lock()
	defer r.mut.Unlock()

	return r.SkipN(1)
}

func (r *Reader) Offset() int64 {
	r.mut.Lock()
	defer r.mut.Unlock()

	return r.offset
}
