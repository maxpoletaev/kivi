package protoio

import (
	"fmt"
	"hash/crc32"
	"io"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/runtime/protoiface"
)

type Writer struct {
	pbopts    *proto.MarshalOptions
	file      io.Writer
	separator uint16
	entryBuf  []byte
	headerBuf []byte
	offset    int64
	count     int
}

func NewWriter(file io.Writer) *Writer {
	var offset int64
	if seeker, ok := file.(io.Seeker); ok {
		offset, _ = seeker.Seek(0, io.SeekCurrent)
	}

	return &Writer{
		file:      file,
		offset:    offset,
		entryBuf:  make([]byte, 0),
		headerBuf: make([]byte, headerSize),
		pbopts:    &proto.MarshalOptions{},
		separator: defaultSeparator,
	}
}

func (w *Writer) Append(entry proto.Message) (int, error) {
	out, err := w.pbopts.MarshalState(protoiface.MarshalInput{
		Message: entry.ProtoReflect(),
		Buf:     w.entryBuf[:0],
	})
	if err != nil {
		return 0, fmt.Errorf("proto marshaling failed: %w", err)
	}

	defer func() {
		// The orignal buffer could have been resized by the MarshalState call.
		// We need to update the reference to the buffer to the new one.
		w.entryBuf = out.Buf
	}()

	if err = encodeHeader(&entryHeader{
		separator: w.separator,
		dataSize:  uint64(len(out.Buf)),
		crc:       crc32.ChecksumIEEE(out.Buf),
	}, w.headerBuf); err != nil {
		return 0, err
	}

	n1, err := w.file.Write(w.headerBuf)
	if err != nil {
		return 0, err
	}

	n2, err := w.file.Write(out.Buf)
	if err != nil {
		return 0, err
	}

	w.offset += int64(n1 + n2)

	w.count++

	return n1 + n2, nil
}

func (w *Writer) Offset() int64 {
	return w.offset
}

func (w *Writer) Count() int {
	return w.count
}
