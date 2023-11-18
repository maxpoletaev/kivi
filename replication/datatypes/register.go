package datatypes

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/maxpoletaev/kivi/internal/binario"
)

type Register struct {
	timestamp int64
	value     []byte
	modified  bool
}

func NewRegister() *Register {
	return &Register{}
}

func (r *Register) Get() []byte {
	return r.value
}

func (r *Register) Put(value []byte) {
	r.timestamp = time.Now().UnixMilli()
	r.modified = true
	r.value = value
}

func (r *Register) Merge(other *Register) {
	if other.timestamp > r.timestamp {
		r.timestamp = other.timestamp
		r.value = other.value
	}
}

func (r *Register) Modified() bool {
	return r.modified
}

func (r *Register) FromBytes(data []byte) error {
	reader := binario.NewReader(bytes.NewReader(data), binary.LittleEndian)

	timestamp, err := reader.ReadUint64()
	if err != nil {
		return err
	}

	value, err := reader.ReadBytes()
	if err != nil {
		return err
	}

	r.timestamp = int64(timestamp)
	r.value = value

	return nil
}

func (r *Register) ToBytes() ([]byte, error) {
	var buf bytes.Buffer

	writer := binario.NewWriter(&buf, binary.LittleEndian)

	err := writer.WriteUint64(uint64(r.timestamp))
	if err != nil {
		return nil, err
	}

	err = writer.WriteBytes(r.value)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
