package binario

import (
	"encoding/binary"
	"io"
)

type Reader struct {
	byteOrder binary.ByteOrder
	reader    io.Reader
}

func NewReader(reader io.Reader, byteOrder binary.ByteOrder) *Reader {
	return &Reader{
		reader:    reader,
		byteOrder: byteOrder,
	}
}

func (r *Reader) ReadUint8() (uint8, error) {
	bs := make([]byte, 1)
	if _, err := r.reader.Read(bs); err != nil {
		return 0, err
	}

	return bs[0], nil
}

func (r *Reader) ReadUint16() (uint16, error) {
	bs := make([]byte, 2)
	if _, err := r.reader.Read(bs); err != nil {
		return 0, err
	}

	return r.byteOrder.Uint16(bs), nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	bs := make([]byte, 4)
	if _, err := r.reader.Read(bs); err != nil {
		return 0, err
	}

	return r.byteOrder.Uint32(bs), nil
}

func (r *Reader) ReadUint64() (uint64, error) {
	bs := make([]byte, 8)
	if _, err := r.reader.Read(bs); err != nil {
		return 0, err
	}

	return r.byteOrder.Uint64(bs), nil
}

func (r *Reader) ReadBytes() ([]byte, error) {
	length, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	bs := make([]byte, length)
	if _, err := r.reader.Read(bs); err != nil {
		return nil, err
	}

	return bs, nil
}

func (r *Reader) ReadString() (string, error) {
	bs, err := r.ReadBytes()
	return string(bs), err
}

func (r *Reader) ReadVarUint() (uint64, error) {
	var value uint64
	var shift uint

	for {
		b, err := r.ReadUint8()
		if err != nil {
			return 0, err
		}

		value |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}

		shift += 7
	}

	return value, nil
}
