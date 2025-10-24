package ktypes

import "fmt"

// Kafka-specific types that map to Kafka protocol types
// These types carry encoding/decoding information and can be used in structs
// with order tags to specify exactly which Kafka type to use

// Basic types
type Bool bool
type Int8 int8
type Int16 int16
type Int32 int32
type Int64 int64
type Uint16 uint16
type Uint32 uint32
type Float64 float64

// Variable-length integers
type VarInt int32
type VarLong int64
type UVarInt uint32
type UVarLong uint64

// UUID
type UUID [16]byte

// String returns the string representation of the UUID
func (u UUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// String types
type String string
type CompactString string
type NullableString string
type CompactNullableString string

// Bytes types
type Bytes []byte
type NullableBytes []byte
type CompactBytes []byte
type CompactNullableBytes []byte

// Records types
type Records []byte
type CompactRecords []byte

// Array types
type Array[T any] []T
type CompactArray[T any] []T
