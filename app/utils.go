package main

import (
	"encoding/binary"
	"fmt"
)

// Read ByteBuffer
type ByteBuffer struct {
	data   []byte
	offset int
}

func NewByteBuffer(data []byte) *ByteBuffer {
	return &ByteBuffer{
		data:   data,
		offset: 0,
	}
}

func (b *ByteBuffer) ReadUint32() (uint32, error) {
	if b.offset+4 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow: trying to read 4 bytes at offset %d, buffer size %d", b.offset, len(b.data))
	}
	value := binary.BigEndian.Uint32(b.data[b.offset : b.offset+4])
	b.offset += 4
	return value, nil
}

func (b *ByteBuffer) ReadUint16() (uint16, error) {
	if b.offset+2 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow: trying to read 2 bytes at offset %d, buffer size %d", b.offset, len(b.data))
	}
	value := binary.BigEndian.Uint16(b.data[b.offset : b.offset+2])
	b.offset += 2
	return value, nil
}

func (b *ByteBuffer) ReadUint8() (uint8, error) {
	if b.offset+1 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow: trying to read 1 byte at offset %d, buffer size %d", b.offset, len(b.data))
	}
	value := b.data[b.offset]
	b.offset += 1
	return value, nil
}

func (b *ByteBuffer) ReadBytes(n int) ([]byte, error) {
	if b.offset+n > len(b.data) {
		return nil, fmt.Errorf("buffer overflow: trying to read %d bytes at offset %d, buffer size %d", n, b.offset, len(b.data))
	}
	data := make([]byte, n)
	copy(data, b.data[b.offset:b.offset+n])
	b.offset += n
	return data, nil
}

func (b *ByteBuffer) SkipBytes(n int) error {
	if b.offset+n > len(b.data) {
		return fmt.Errorf("buffer overflow: trying to skip %d bytes at offset %d, buffer size %d", n, b.offset, len(b.data))
	}
	b.offset += n
	return nil
}

func (b *ByteBuffer) Remaining() int {
	return len(b.data) - b.offset
}

func (b *ByteBuffer) ReadRemaining() ([]byte, error) {
	if b.offset >= len(b.data) {
		return []byte{}, nil
	}
	return b.ReadBytes(b.Remaining())
}

func (b *ByteBuffer) GetOffset() int {
	return b.offset
}

// Write ByteBuffer
type WriteByteBuffer struct {
	data []byte
}

func NewWriteByteBuffer(capacity *int) *WriteByteBuffer {
	if capacity == nil {
		return &WriteByteBuffer{
			data: make([]byte, 0),
		}
	}

	return &WriteByteBuffer{
		data: make([]byte, 0, *capacity),
	}
}

func (w *WriteByteBuffer) WriteUint32(value uint32) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, value)
	w.data = append(w.data, bytes...)
}

func (w *WriteByteBuffer) WriteUint16(value uint16) {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, value)
	w.data = append(w.data, bytes...)
}

func (w *WriteByteBuffer) WriteUint8(value uint8) {
	bytes := make([]byte, 1)
	bytes[0] = value
	w.data = append(w.data, bytes...)
}

func (w *WriteByteBuffer) WriteBytes(data []byte) {
	w.data = append(w.data, data...)
}

func (w *WriteByteBuffer) Bytes() []byte {
	return w.data
}

func (w *WriteByteBuffer) Len() int {
	return len(w.data)
}
