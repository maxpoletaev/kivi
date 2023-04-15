package protoio

import (
	"encoding/binary"
)

var (
	byteOrder = binary.LittleEndian
)

type entryHeader struct {
	dataSize uint64
	crc      uint32
}

func encodeHeader(h *entryHeader, b []byte) {
	byteOrder.PutUint64(b[0:8], h.dataSize)
	byteOrder.PutUint32(b[8:12], h.crc)
}

func decodeHeader(h *entryHeader, b []byte) {
	h.dataSize = byteOrder.Uint64(b[0:8])
	h.crc = byteOrder.Uint32(b[8:12])
}
