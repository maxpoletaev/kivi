package protoio

import (
	"fmt"
	"io"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

type Writer struct {
	file   io.Writer
	offset int64
	count  int64
}

func NewWriter(file io.Writer) *Writer {
	var offset int64
	if seeker, ok := file.(io.Seeker); ok {
		offset, _ = seeker.Seek(0, io.SeekCurrent)
	}

	return &Writer{
		file:   file,
		offset: offset,
	}
}

func (w *Writer) writeEntry(entry proto.Message) (int, error) {
	dataBuf, err := proto.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("proto marshaling failed: %w", err)
	}

	header := entryHeader{
		dataSize:  uint64(len(dataBuf)),
		separator: entrySeparator,
	}

	headerBuf := make([]byte, headerSize)
	if err := encodeHeader(&header, headerBuf); err != nil {
		return 0, err
	}

	headerN, err := w.file.Write(headerBuf)
	if err != nil {
		return 0, fmt.Errorf("failed to write header file: %w", err)
	}

	dataN, err := w.file.Write(dataBuf)
	if err != nil {
		return 0, fmt.Errorf("failed to write into a file file: %w", err)
	}

	n := headerN + dataN

	atomic.AddInt64(&w.offset, int64(n))

	atomic.AddInt64(&w.count, 1)

	return n, nil
}

func (w *Writer) Append(entry proto.Message) (int, error) {
	n, err := w.writeEntry(entry)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (w *Writer) Count() int {
	return int(atomic.LoadInt64(&w.count))
}

func (w *Writer) Offset() int64 {
	return atomic.LoadInt64(&w.offset)
}
