package main

import (
	"encoding/binary"
	"fmt"
)

// Read ByteBuffer
type KafkaReader struct {
	data   []byte
	offset int
}

func NewKafkaReader(data []byte) *KafkaReader {
	return &KafkaReader{
		data:   data,
		offset: 0,
	}
}

func (b *KafkaReader) Int8() (int8, error) {
	if b.offset+1 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow: trying to read 1 byte at offset %d, buffer size %d", b.offset, len(b.data))
	}
	value := b.data[b.offset]
	b.offset += 1
	return int8(value), nil
}

func (b *KafkaReader) Int16() (int16, error) {
	if b.offset+2 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow: trying to read 2 bytes at offset %d, buffer size %d", b.offset, len(b.data))
	}
	value := binary.BigEndian.Uint16(b.data[b.offset : b.offset+2])
	b.offset += 2
	return int16(value), nil
}


func (b *KafkaReader) Int32() (int32, error) {
	if b.offset+4 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow: trying to read 4 bytes at offset %d, buffer size %d", b.offset, len(b.data))
	}
	value := binary.BigEndian.Uint32(b.data[b.offset : b.offset+4])
	b.offset += 4
	return int32(value), nil
}

func (b *KafkaReader) Int64() (int64, error) {
	if b.offset+8 > len(b.data) {
		return 0, fmt.Errorf("buffer overflow: trying to read 8 bytes at offset %d, buffer size %d", b.offset, len(b.data))
	}
	value := int64(binary.BigEndian.Uint64(b.data[b.offset : b.offset+8]))
	b.offset += 8
	return value, nil
}

func (b *KafkaReader) Bytes(n int) ([]byte, error) {
	if b.offset+n > len(b.data) {
		return nil, fmt.Errorf("buffer overflow: trying to read %d bytes at offset %d, buffer size %d", n, b.offset, len(b.data))
	}
	data := make([]byte, n)
	copy(data, b.data[b.offset:b.offset+n])
	b.offset += n
	return data, nil
}

func (b *KafkaReader) CompactInt32Array() ([]int32, error) {
	length, err := b.VarUint()
	if err != nil {
		return nil, err
	}
	
	if length <= 1 {
		return []int32{}, nil
	}

	length--
	data, err := b.Bytes(int(length))
	if err != nil {
		return nil, err
	}
	int32s := make([]int32, len(data))
	for i, d := range data {
		int32s[i] = int32(d)
	}
	return int32s, nil
}

func (b *KafkaReader) SkipBytes(n int) error {
	if b.offset+n > len(b.data) {
		return fmt.Errorf("buffer overflow: trying to skip %d bytes at offset %d, buffer size %d", n, b.offset, len(b.data))
	}
	b.offset += n
	return nil
}

func (b *KafkaReader) Remaining() int {
	return len(b.data) - b.offset
}

func (b *KafkaReader) RemainingBytes() ([]byte, error) {
	if b.offset >= len(b.data) {
		return []byte{}, nil
	}
	return b.Bytes(b.Remaining())
}

func (b *KafkaReader) GetOffset() int {
	return b.offset
}

// VarInt reads a variable-size integer (varint) from the buffer
// VarInts are encoded using a variable number of bytes where the most significant bit
// of each byte indicates whether more bytes follow, with zig-zag encoding
func (b *KafkaReader) VarInt() (int64, error) {
	var result int64
	var shift uint
	
	for {
		if b.offset >= len(b.data) {
			return 0, fmt.Errorf("buffer overflow: trying to read varint at offset %d, buffer size %d", b.offset, len(b.data))
		}
		
		byte := b.data[b.offset]
		b.offset++
		
		result |= int64(byte&0x7F) << shift
		
		if (byte & 0x80) == 0 {
			break
		}
		
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint too large")
		}
	}
	
	// Apply zig-zag decoding: (n >> 1) ^ (-(n & 1))
	// For positive numbers, this is just n >> 1 (divide by 2)
	return (result >> 1) ^ (-(result & 1)), nil
}

// VarUint reads a variable-size unsigned integer (varuint) from the buffer
// VarUints are encoded using a variable number of bytes where the most significant bit
// of each byte indicates whether more bytes follow, WITHOUT zig-zag encoding
func (b *KafkaReader) VarUint() (uint64, error) {
	var result uint64
	var shift uint

	for {
		if b.offset >= len(b.data) {
			return 0, fmt.Errorf("buffer overflow: trying to read varuint at offset %d, buffer size %d", b.offset, len(b.data))
		}

		bt := b.data[b.offset]
		b.offset++

		result |= uint64(bt&0x7F) << shift

		if (bt & 0x80) == 0 {
			break
		}

		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varuint too large")
		}
	}

	// No zig-zag decoding for unsigned integers - return the result as-is
	return result, nil
}