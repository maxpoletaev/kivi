package protoio

import (
	"encoding/binary"
)

var (
	byteOrder = binary.LittleEndian
)

type entryHeader struct {
	separator uint16
	dataSize  uint64
	crc       uint32
}

func encodeHeader(h *entryHeader, b []byte) {
	byteOrder.PutUint16(b[0:2], h.separator)
	byteOrder.PutUint64(b[2:10], h.dataSize)
	byteOrder.PutUint32(b[10:14], h.crc)
}

func decodeHeader(h *entryHeader, b []byte) {
	h.separator = byteOrder.Uint16(b[0:2])
	h.dataSize = byteOrder.Uint64(b[2:10])
	h.crc = byteOrder.Uint32(b[10:14])
}
