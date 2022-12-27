package protoio

import (
	"encoding/binary"
	"errors"
)

const (
	entrySeparator uint16 = 0xAFAF
	headerSize     int    = 12
)

var (
	errWrongSeparator    = errors.New("separator mismatch")
	errInvalidHeaderSize = errors.New("invalid header size")
)

type entryHeader struct {
	separator uint16
	dataSize  uint64
}

func encodeHeader(h *entryHeader, b []byte) error {
	if len(b) < headerSize {
		return errInvalidHeaderSize
	}

	binary.LittleEndian.PutUint16(b[0:2], entrySeparator)
	binary.LittleEndian.PutUint64(b[2:10], h.dataSize)

	return nil
}

func decodeHeader(h *entryHeader, b []byte) error {
	if len(b) < headerSize {
		return errInvalidHeaderSize
	}

	h.separator = binary.LittleEndian.Uint16(b[0:2])
	h.dataSize = binary.LittleEndian.Uint64(b[2:10])

	if h.separator != entrySeparator {
		return errWrongSeparator
	}

	return nil
}
