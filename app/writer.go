package main

import (
	"encoding/binary"
)

type KafkaWriter struct {
	data []byte
}

// NewKafkaWriter creates a new Kafka writer
func NewKafkaWriter() *KafkaWriter {
	return &KafkaWriter{
		data: make([]byte, 0),
	}
}

func (w *KafkaWriter) Int8(value int8) {
	w.data = append(w.data, uint8(value))
}

func (w *KafkaWriter) Int16(value int16) {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, uint16(value))
	w.data = append(w.data, bytes...)
}

func (w *KafkaWriter) Uint16(value uint16) {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, value)
	w.data = append(w.data, bytes...)
}


func (w *KafkaWriter) Int32(value int32) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(value))
	w.data = append(w.data, bytes...)
}

func (w *KafkaWriter) Uint32(value uint32) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, value)
	w.data = append(w.data, bytes...)
}

func (w *KafkaWriter) Int64(value int64) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(value))
	w.data = append(w.data, bytes...)
}

func (w *KafkaWriter) Bytes(data []byte) {
	w.data = append(w.data, data...)
}

func (w *KafkaWriter) String(value string) {
	bytes := []byte(value)
	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, uint16(len(bytes)))
	w.data = append(w.data, lengthBytes...)
	w.data = append(w.data, bytes...)
}

func (w *KafkaWriter) StringArray(arr []string) {
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(arr)))
	w.data = append(w.data, lengthBytes...)
	for _, s := range arr {
		w.String(s)
	}
}

func (w *KafkaWriter) Int32Array(arr []int32) {
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(arr)))
	w.data = append(w.data, lengthBytes...)
	for _, val := range arr {
		w.Int32(val)
	}
}

func (w *KafkaWriter) Int16Array(arr []int16) {
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(arr)))
	w.data = append(w.data, lengthBytes...)
	for _, val := range arr {
		w.Int16(val)
	}
}

func (w *KafkaWriter) Uint32Array(arr []uint32) {
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(arr)))
	w.data = append(w.data, lengthBytes...)
	for _, val := range arr {
		w.Uint32(val)
	}
}

func (w *KafkaWriter) Build() []byte {
	return w.data
}

func (w *KafkaWriter) Length() int {
	return len(w.data)
}

// VarInt writes a variable-size integer (varint) to the buffer with zig zag encoding
func (w *KafkaWriter) VarInt(value int64) {
	encoded := uint64((value << 1) ^ (value >> 63))
	
	for encoded >= 0x80 {
		w.data = append(w.data, byte(encoded)|0x80)
		encoded >>= 7
	}
	w.data = append(w.data, byte(encoded))
}

// VarUint writes a variable-size unsigned integer (varuint) to the buffer
func (w *KafkaWriter) VarUint(value uint64) {
	for value >= 0x80 {
		w.data = append(w.data, byte(value)|0x80)
		value >>= 7
	}
	w.data = append(w.data, byte(value))
}

