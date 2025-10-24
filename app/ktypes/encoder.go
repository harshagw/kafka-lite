package ktypes

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// KEncoder represents an encoder specifically for Kafka types
type KEncoder struct {
	buf []byte
}

// NewKEncoder creates a new Kafka encoder
func NewKEncoder() *KEncoder {
	return &KEncoder{
		buf: make([]byte, 0),
	}
}

// Encode encodes the provided struct into a byte array using ktypes
func (e *KEncoder) Encode(v any) ([]byte, error) {
	if v == nil {
		return nil, errors.New("cannot encode nil value")
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		return nil, errors.New("encode target must be a pointer")
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return nil, errors.New("encode target must be a pointer to struct")
	}

	// Reset buffer
	e.buf = make([]byte, 0)

	if err := e.encodeStruct(rv); err != nil {
		return nil, err
	}

	return e.buf, nil
}

// encodeStruct encodes a struct by writing fields in order based on struct tags
func (e *KEncoder) encodeStruct(rv reflect.Value) error {
	rt := rv.Type()
	
	// Create a map to store field order -> field info
	fieldMap := make(map[int]reflect.StructField)
	maxOrder := 0

	// Parse struct tags to determine field order
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		orderTag := field.Tag.Get("order")
		if orderTag == "" {
			continue // Skip fields without order tag
		}

		order, err := strconv.Atoi(orderTag)
		if err != nil {
			return fmt.Errorf("invalid order tag for field %s: %v", field.Name, err)
		}

		fieldMap[order] = field
		if order > maxOrder {
			maxOrder = order
		}
	}

	// Encode fields in order
	for order := 1; order <= maxOrder; order++ {
		field, exists := fieldMap[order]
		if !exists {
			continue
		}

		fieldValue := rv.FieldByName(field.Name)
		if !fieldValue.CanInterface() {
			return fmt.Errorf("cannot access field %s", field.Name)
		}

		if err := e.encodeKType(fieldValue, field); err != nil {
			return fmt.Errorf("failed to encode field %s: %v", field.Name, err)
		}
	}

	return nil
}

// encodeKType encodes a single ktype field
func (e *KEncoder) encodeKType(fv reflect.Value, field reflect.StructField) error {
	// Get the actual type (not pointer)
	actualType := fv.Type()
	if actualType.Kind() == reflect.Ptr {
		actualType = actualType.Elem()
	}

	// Check if it's a ktype by looking at the package
	if actualType.PkgPath() != "github.com/codecrafters-io/kafka-starter-go/app/ktypes" {
		return fmt.Errorf("field %s is not a ktype (pkg: %s, type: %s)", field.Name, actualType.PkgPath(), actualType.Name())
	}

	// Check if it's a generic Array or CompactArray type
	if actualType.Kind() == reflect.Slice {
		// This is a slice type, check if it's Array[T] or CompactArray[T]
		typeName := actualType.Name()
		if strings.HasPrefix(typeName, "Array[") || strings.HasPrefix(typeName, "CompactArray[") {
			isCompact := strings.HasPrefix(typeName, "CompactArray[")
			return e.encodeGenericArray(fv, isCompact)
		}
	}

	switch actualType.Name() {
	// Basic types
	case "Bool":
		val := fv.Bool()
		e.writeBoolean(val)
		return nil

	case "Int8":
		val := int8(fv.Int())
		e.writeInt8(val)
		return nil

	case "Int16":
		val := int16(fv.Int())
		e.writeInt16(val)
		return nil

	case "Int32":
		val := int32(fv.Int())
		e.writeInt32(val)
		return nil

	case "Int64":
		val := fv.Int()
		e.writeInt64(val)
		return nil

	case "Uint16":
		val := uint16(fv.Uint())
		e.writeUint16(val)
		return nil

	case "Uint32":
		val := uint32(fv.Uint())
		e.writeUint32(val)
		return nil

	case "Float64":
		val := fv.Float()
		e.writeFloat64(val)
		return nil

	// Variable-length integers
	case "VarInt":
		val := int32(fv.Int())
		e.writeVarInt(val)
		return nil

	case "VarLong":
		val := fv.Int()
		e.writeVarLong(val)
		return nil

	case "UVarInt":
		val := uint32(fv.Uint())
		e.writeUnsignedVarInt(val)
		return nil

	case "UVarLong":
		val := fv.Uint()
		e.writeUnsignedVarLong(val)
		return nil

	// UUID
	case "UUID":
		val := fv.Interface().(UUID)
		e.writeUUID(val)
		return nil

	// String types
	case "String":
		val := fv.String()
		e.writeString(val)
		return nil

	case "CompactString":
		val := fv.String()
		e.writeCompactString(val)
		return nil

	case "NullableString":
		val := fv.String()
		if val == "" {
			e.writeNullableString(nil)
		} else {
			e.writeNullableString(&val)
		}
		return nil

	case "CompactNullableString":
		val := fv.String()
		if val == "" {
			e.writeCompactNullableString(nil)
		} else {
			e.writeCompactNullableString(&val)
		}
		return nil

	// Bytes types
	case "Bytes":
		val := fv.Bytes()
		e.writeBytes(val)
		return nil

	case "NullableBytes":
		val := fv.Bytes()
		e.writeNullableBytes(val)
		return nil

	case "CompactBytes":
		val := fv.Bytes()
		e.writeCompactBytes(val)
		return nil

	case "CompactNullableBytes":
		val := fv.Bytes()
		e.writeCompactNullableBytes(val)
		return nil

	// Records types
	case "Records":
		val := fv.Bytes()
		e.writeRecords(val)
		return nil

	case "CompactRecords":
		val := fv.Bytes()
		e.writeCompactRecords(val)
		return nil

	// Array types
	case "Array":
		return e.encodeGenericArray(fv, false) // false = regular array

	case "CompactArray":
		return e.encodeGenericArray(fv, true) // true = compact array

	default:
		// Check if it's a struct type (for nested structs)
		if actualType.Kind() == reflect.Struct {
			return e.encodeStruct(fv)
		}
		return fmt.Errorf("unsupported ktype: %s", actualType.Name())
	}
}

// GetBuffer returns the current buffer
func (e *KEncoder) GetBuffer() []byte {
	return e.buf
}

// Reset clears the buffer
func (e *KEncoder) Reset() {
	e.buf = make([]byte, 0)
}

// encodeGenericArray encodes a generic array (Array[T] or CompactArray[T])
func (e *KEncoder) encodeGenericArray(fv reflect.Value, isCompact bool) error {
	// Check if the slice is nil
	if fv.IsNil() {
		if isCompact {
			// For CompactArray, write 0 for null array
			e.writeUnsignedVarInt(0)
		} else {
			// For Array, write -1 for null array
			e.writeInt32(-1)
		}
		return nil
	}

	length := fv.Len()
	
	if isCompact {
		// For CompactArray, write length as UNSIGNED_VARINT (N + 1)
		e.writeUnsignedVarInt(uint32(length + 1))
	} else {
		// For Array, write length as INT32
		e.writeInt32(int32(length))
	}
	
	// Encode each element
	for i := 0; i < length; i++ {
		elem := fv.Index(i)
		if err := e.encodeArrayElement(elem); err != nil {
			return fmt.Errorf("failed to encode array element %d: %v", i, err)
		}
	}
	
	return nil
}

// encodeArrayElement encodes a single element in an array
func (e *KEncoder) encodeArrayElement(elem reflect.Value) error {
	elemType := elem.Type()
	
	// Check if it's a ktype
	if elemType.PkgPath() == "github.com/codecrafters-io/kafka-starter-go/app/ktypes" {
		// It's a ktype, use the ktype encoder
		return e.encodeKType(elem, reflect.StructField{})
	}
	
	// Handle basic Go types
	switch elemType.Kind() {
	case reflect.String:
		val := elem.String()
		e.writeString(val)
		return nil
		
	case reflect.Int32:
		val := int32(elem.Int())
		e.writeInt32(val)
		return nil
		
	case reflect.Int64:
		val := elem.Int()
		e.writeInt64(val)
		return nil
		
	case reflect.Slice:
		if elemType.Elem().Kind() == reflect.Uint8 {
			// []byte
			val := elem.Bytes()
			e.writeBytes(val)
			return nil
		}
		return fmt.Errorf("unsupported slice element type: %v", elemType.Elem())
		
	case reflect.Struct:
		// Check if this struct has ktype fields with order tags
		// If so, encode it as a struct with ordered fields
		if e.hasOrderedKTypeFields(elemType) {
			return e.encodeStructWithOrderedFields(elem)
		}
		// Otherwise, treat as a regular struct
		return e.encodeStruct(elem)
		
	default:
		return fmt.Errorf("unsupported array element type: %v", elemType)
	}
}

// hasOrderedKTypeFields checks if a struct type has fields with order tags
func (e *KEncoder) hasOrderedKTypeFields(rt reflect.Type) bool {
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		orderTag := field.Tag.Get("order")
		if orderTag != "" {
			return true
		}
	}
	return false
}

// encodeStructWithOrderedFields encodes a struct with ordered fields (like SupportedAPIsKType)
func (e *KEncoder) encodeStructWithOrderedFields(rv reflect.Value) error {
	rt := rv.Type()
	
	// Create a map to store field order -> field info
	fieldMap := make(map[int]reflect.StructField)
	maxOrder := 0

	// Parse struct tags to determine field order
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		orderTag := field.Tag.Get("order")
		if orderTag == "" {
			continue // Skip fields without order tag
		}

		order, err := strconv.Atoi(orderTag)
		if err != nil {
			return fmt.Errorf("invalid order tag for field %s: %v", field.Name, err)
		}

		fieldMap[order] = field
		if order > maxOrder {
			maxOrder = order
		}
	}

	// Encode fields in order
	for order := 1; order <= maxOrder; order++ {
		field, exists := fieldMap[order]
		if !exists {
			continue
		}

		fieldValue := rv.FieldByName(field.Name)
		if !fieldValue.CanInterface() {
			return fmt.Errorf("cannot access field %s", field.Name)
		}

		if err := e.encodeKType(fieldValue, field); err != nil {
			return fmt.Errorf("failed to encode field %s: %v", field.Name, err)
		}
	}

	return nil
}
