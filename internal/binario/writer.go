package binario

import (
	"encoding/binary"
	"io"
)

type Writer struct {
	writer    io.Writer
	byteOrder binary.ByteOrder
}

func NewWriter(writer io.Writer, byteOrder binary.ByteOrder) *Writer {
	return &Writer{
		writer:    writer,
		byteOrder: byteOrder,
	}
}

func (w *Writer) WriteUint8(value uint8) error {
	_, err := w.writer.Write([]byte{value})
	return err
}

func (w *Writer) WriteUint16(value uint16) error {
	bf := make([]byte, 2)
	w.byteOrder.PutUint16(bf, value)
	_, err := w.writer.Write(bf)

	return err
}

func (w *Writer) WriteUint32(value uint32) error {
	bf := make([]byte, 4)
	w.byteOrder.PutUint32(bf, value)
	_, err := w.writer.Write(bf)

	return err
}

func (w *Writer) WriteUint64(value uint64) error {
	bf := make([]byte, 8)
	w.byteOrder.PutUint64(bf, value)
	_, err := w.writer.Write(bf)

	return err
}

func (w *Writer) WriteBytes(value []byte) error {
	length := uint32(len(value))
	if err := w.WriteUint32(length); err != nil {
		return err
	}

	_, err := w.writer.Write(value)

	return err
}

func (w *Writer) WriteString(value string) error {
	return w.WriteBytes([]byte(value))
}

func (w *Writer) WriteVarUint(value uint64) error {
	for value >= 0x80 {
		if err := w.WriteUint8(uint8(value) | 0x80); err != nil {
			return err
		}

		value >>= 7
	}

	return w.WriteUint8(uint8(value))
}
