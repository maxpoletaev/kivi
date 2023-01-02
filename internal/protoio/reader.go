package protoio

import (
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

type Reader struct {
	headerBuf [headerSize]byte
	file      io.ReaderAt
	offset    int64
}

func NewReader(source io.ReaderAt) *Reader {
	return &Reader{
		file: source,
	}
}

func (r *Reader) readHeader(h *entryHeader) (int, error) {
	buf := r.headerBuf[:]

	read, err := r.file.ReadAt(buf, r.offset)
	if err != nil {
		return 0, err
	}

	if read != headerSize {
		return 0, io.ErrUnexpectedEOF
	}

	if err := decodeHeader(h, buf); err != nil {
		return 0, fmt.Errorf("failed to decode header: %w", err)
	}

	r.offset += int64(read)

	return read, nil
}

func (r *Reader) readEntry(h entryHeader, entry proto.Message) (int, error) {
	buf := make([]byte, h.dataSize)

	read, err := r.file.ReadAt(buf, r.offset)
	if err != nil {
		return 0, err
	}

	if uint64(read) != h.dataSize {
		return 0, io.ErrUnexpectedEOF
	}

	if err := proto.Unmarshal(buf, entry); err != nil {
		return 0, err
	}

	r.offset += int64(read)

	return read, nil
}

func (r *Reader) read(msg proto.Message) (int, error) {
	var header entryHeader

	headerSize, err := r.readHeader(&header)
	if err != nil {
		return 0, err
	}

	entrySize, err := r.readEntry(header, msg)
	if err != nil {
		return 0, err
	}

	read := headerSize + entrySize

	return read, nil
}

func (r *Reader) ReadAt(msg proto.Message, offset int64) (int, error) {
	r.offset = offset

	return r.read(msg)
}

func (r *Reader) ReadNext(msg proto.Message) (int, error) {
	n, err := r.read(msg)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (r *Reader) SkipN(n int) error {
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
	return r.SkipN(1)
}

func (r *Reader) Offset() int64 {
	return r.offset
}
