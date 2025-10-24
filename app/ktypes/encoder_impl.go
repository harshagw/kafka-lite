package ktypes

import (
	"encoding/binary"
)

// Basic integer writing methods

func (e *KEncoder) writeInt8(val int8) {
	e.buf = append(e.buf, byte(val))
}

func (e *KEncoder) writeInt16(val int16) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(val))
	e.buf = append(e.buf, b...)
}

func (e *KEncoder) writeInt32(val int32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(val))
	e.buf = append(e.buf, b...)
}

func (e *KEncoder) writeInt64(val int64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(val))
	e.buf = append(e.buf, b...)
}

func (e *KEncoder) writeUint16(val uint16) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, val)
	e.buf = append(e.buf, b...)
}

func (e *KEncoder) writeUint32(val uint32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, val)
	e.buf = append(e.buf, b...)
}

func (e *KEncoder) writeBoolean(val bool) {
	if val {
		e.buf = append(e.buf, 1)
	} else {
		e.buf = append(e.buf, 0)
	}
}

func (e *KEncoder) writeFloat64(val float64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(val))
	e.buf = append(e.buf, b...)
}

// Variable-length integer writing methods

func (e *KEncoder) writeVarInt(val int32) {
	// Apply zig-zag encoding: (n << 1) ^ (n >> 31)
	encoded := uint32((val << 1) ^ (val >> 31))
	e.writeVarUint32(encoded)
}

func (e *KEncoder) writeVarLong(val int64) {
	// Apply zig-zag encoding: (n << 1) ^ (n >> 63)
	encoded := uint64((val << 1) ^ (val >> 63))
	e.writeVarUint64(encoded)
}

func (e *KEncoder) writeUnsignedVarInt(val uint32) {
	e.writeVarUint32(val)
}

func (e *KEncoder) writeUnsignedVarLong(val uint64) {
	e.writeVarUint64(val)
}

func (e *KEncoder) writeVarUint32(val uint32) {
	for val >= 0x80 {
		e.buf = append(e.buf, byte(val)|0x80)
		val >>= 7
	}
	e.buf = append(e.buf, byte(val))
}

func (e *KEncoder) writeVarUint64(val uint64) {
	for val >= 0x80 {
		e.buf = append(e.buf, byte(val)|0x80)
		val >>= 7
	}
	e.buf = append(e.buf, byte(val))
}

// String writing methods

func (e *KEncoder) writeString(val string) {
	length := int16(len(val))
	e.writeInt16(length)
	e.buf = append(e.buf, []byte(val)...)
}

func (e *KEncoder) writeNullableString(val *string) {
	if val == nil {
		e.writeInt16(-1)
	} else {
		e.writeString(*val)
	}
}

func (e *KEncoder) writeCompactString(val string) {
	// Compact string length is N + 1, where N is the actual length
	length := uint32(len(val) + 1)
	e.writeUnsignedVarInt(length)
	e.buf = append(e.buf, []byte(val)...)
}

func (e *KEncoder) writeCompactNullableString(val *string) {
	if val == nil {
		e.writeUnsignedVarInt(0)
	} else {
		e.writeCompactString(*val)
	}
}

// Bytes writing methods

func (e *KEncoder) writeBytes(val []byte) {
	length := int32(len(val))
	e.writeInt32(length)
	e.buf = append(e.buf, val...)
}

func (e *KEncoder) writeNullableBytes(val []byte) {
	if val == nil {
		e.writeInt32(-1)
	} else {
		e.writeBytes(val)
	}
}

func (e *KEncoder) writeCompactBytes(val []byte) {
	// Compact bytes length is N + 1, where N is the actual length
	length := uint32(len(val) + 1)
	e.writeUnsignedVarInt(length)
	e.buf = append(e.buf, val...)
}

func (e *KEncoder) writeCompactNullableBytes(val []byte) {
	if val == nil {
		e.writeUnsignedVarInt(0)
	} else {
		e.writeCompactBytes(val)
	}
}

func (e *KEncoder) writeRecords(val []byte) {
	e.writeNullableBytes(val)
}

func (e *KEncoder) writeCompactRecords(val []byte) {
	e.writeCompactNullableBytes(val)
}

func (e *KEncoder) writeUUID(val UUID) {
	e.buf = append(e.buf, val[:]...)
}

