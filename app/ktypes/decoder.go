package ktypes

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// KDecoder represents a decoder specifically for Kafka types
type KDecoder struct {
	data []byte
	pos  int
}

// NewKDecoder creates a new Kafka decoder with the given byte data
func NewKDecoder(data []byte) *KDecoder {
	return &KDecoder{
		data: data,
		pos:  0,
	}
}

// Decode decodes the byte data into the provided struct using ktypes
func (d *KDecoder) Decode(v any) error {
	if v == nil {
		return errors.New("cannot decode into nil value")
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr {
		return errors.New("decode target must be a pointer")
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return errors.New("decode target must be a pointer to struct")
	}

	return d.decodeStruct(rv)
}

// decodeStruct decodes a struct by reading fields in order based on struct tags
func (d *KDecoder) decodeStruct(rv reflect.Value) error {
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

	// Decode fields in order
	for order := 1; order <= maxOrder; order++ {
		field, exists := fieldMap[order]
		if !exists {
			continue
		}

		fieldValue := rv.FieldByName(field.Name)
		if !fieldValue.CanSet() {
			return fmt.Errorf("cannot set field %s", field.Name)
		}

		if err := d.decodeKType(fieldValue, field); err != nil {
			return fmt.Errorf("failed to decode field %s: %v", field.Name, err)
		}
	}

	return nil
}

// decodeKType decodes a single ktype field
func (d *KDecoder) decodeKType(fv reflect.Value, field reflect.StructField) error {
	// Get the actual type (not pointer)
	actualType := fv.Type()
	if actualType.Kind() == reflect.Ptr {
		actualType = actualType.Elem()
	}

	// Check if it's a ktype by looking at the package
	if actualType.PkgPath() != "github.com/codecrafters-io/kafka-starter-go/app/ktypes" {
		return fmt.Errorf("field %s is not a ktype", field.Name)
	}

	// Check if it's a generic Array or CompactArray type
	if actualType.Kind() == reflect.Slice {
		// This is a slice type, check if it's Array[T] or CompactArray[T]
		typeName := actualType.Name()
		if strings.HasPrefix(typeName, "Array[") || strings.HasPrefix(typeName, "CompactArray[") {
			isCompact := strings.HasPrefix(typeName, "CompactArray[")
			return d.decodeGenericArray(fv, isCompact)
		}
	}

	switch actualType.Name() {
	// Basic types
	case "Bool":
		val, err := d.readBoolean()
		if err != nil {
			return err
		}
		fv.SetBool(val)
		return nil

	case "Int8":
		val, err := d.readInt8()
		if err != nil {
			return err
		}
		fv.SetInt(int64(val))
		return nil

	case "Int16":
		val, err := d.readInt16()
		if err != nil {
			return err
		}
		fv.SetInt(int64(val))
		return nil

	case "Int32":
		val, err := d.readInt32()
		if err != nil {
			return err
		}
		fv.SetInt(int64(val))
		return nil

	case "Int64":
		val, err := d.readInt64()
		if err != nil {
			return err
		}
		fv.SetInt(val)
		return nil

	case "Uint16":
		val, err := d.readUint16()
		if err != nil {
			return err
		}
		fv.SetUint(uint64(val))
		return nil

	case "Uint32":
		val, err := d.readUint32()
		if err != nil {
			return err
		}
		fv.SetUint(uint64(val))
		return nil

	case "Float64":
		val, err := d.readFloat64()
		if err != nil {
			return err
		}
		fv.SetFloat(val)
		return nil

	// Variable-length integers
	case "VarInt":
		val, err := d.readVarInt()
		if err != nil {
			return err
		}
		fv.SetInt(int64(val))
		return nil

	case "VarLong":
		val, err := d.readVarLong()
		if err != nil {
			return err
		}
		fv.SetInt(val)
		return nil

	case "UVarInt":
		val, err := d.readUnsignedVarInt()
		if err != nil {
			return err
		}
		fv.SetUint(uint64(val))
		return nil

	case "UVarLong":
		val, err := d.readUnsignedVarLong()
		if err != nil {
			return err
		}
		fv.SetUint(val)
		return nil

	// UUID
	case "UUID":
		val, err := d.readUUID()
		if err != nil {
			return err
		}
		fv.Set(reflect.ValueOf(val))
		return nil

	// String types
	case "String":
		val, err := d.readString()
		if err != nil {
			return err
		}
		fv.SetString(val)
		return nil

	case "CompactString":
		val, err := d.readCompactString()
		if err != nil {
			return err
		}
		fv.SetString(val)
		return nil

	case "NullableString":
		val, err := d.readNullableString()
		if err != nil {
			return err
		}
		if val == nil {
			fv.SetString("")
		} else {
			fv.SetString(*val)
		}
		return nil

	case "CompactNullableString":
		val, err := d.readCompactNullableString()
		if err != nil {
			return err
		}
		if val == nil {
			fv.SetString("")
		} else {
			fv.SetString(*val)
		}
		return nil

	// Bytes types
	case "Bytes":
		val, err := d.readBytes()
		if err != nil {
			return err
		}
		fv.SetBytes(val)
		return nil

	case "NullableBytes":
		val, err := d.readNullableBytes()
		if err != nil {
			return err
		}
		fv.SetBytes(val)
		return nil

	case "CompactBytes":
		val, err := d.readCompactBytes()
		if err != nil {
			return err
		}
		fv.SetBytes(val)
		return nil

	case "CompactNullableBytes":
		val, err := d.readCompactNullableBytes()
		if err != nil {
			return err
		}
		fv.SetBytes(val)
		return nil

	// Records types
	case "Records":
		val, err := d.readRecords()
		if err != nil {
			return err
		}
		fv.SetBytes(val)
		return nil

	case "CompactRecords":
		val, err := d.readCompactRecords()
		if err != nil {
			return err
		}
		fv.SetBytes(val)
		return nil

	// Array types (non-generic)
	case "Array":
		return d.decodeGenericArray(fv, false) // false = regular array

	case "CompactArray":
		return d.decodeGenericArray(fv, true) // true = compact array

	default:
		// Check if it's a struct type (for nested structs)
		if actualType.Kind() == reflect.Struct {
			return d.decodeStruct(fv)
		}
		return fmt.Errorf("unsupported ktype: %s", actualType.Name())
	}
}

// GetPosition returns the current read position
func (d *KDecoder) GetPosition() int {
	return d.pos
}

// SetPosition sets the read position
func (d *KDecoder) SetPosition(pos int) error {
	if pos < 0 || pos > len(d.data) {
		return errors.New("position out of bounds")
	}
	d.pos = pos
	return nil
}

// RemainingBytes returns the number of bytes remaining to be read
func (d *KDecoder) RemainingBytes() int {
	return len(d.data) - d.pos
}

// SkipBytes advances the position by the specified number of bytes
func (d *KDecoder) SkipBytes(n int) error {
	if d.pos+n > len(d.data) {
		return errors.New("cannot skip beyond end of data")
	}
	d.pos += n
	return nil
}

// HasMoreData returns true if there are more bytes to read
func (d *KDecoder) HasMoreData() bool {
	return d.pos < len(d.data)
}

// decodeGenericArray decodes a generic array (Array[T] or CompactArray[T])
func (d *KDecoder) decodeGenericArray(fv reflect.Value, isCompact bool) error {
	// Get the element type from the generic type
	elemType := fv.Type().Elem()
	
	var length int32
	var err error
	
	if isCompact {
		// For CompactArray, read length as UNSIGNED_VARINT
		lengthUint, err := d.readUnsignedVarInt()
		if err != nil {
			return err
		}
		if lengthUint == 0 {
			// Null array
			fv.Set(reflect.Zero(fv.Type()))
			return nil
		}
		length = int32(lengthUint - 1) // Length is N + 1, so actual length is N
	} else {
		// For Array, read length as INT32
		// Check if we have enough bytes to read the length
		if d.RemainingBytes() < 4 {
			// Not enough bytes for array length, assume empty array
			length = 0
		} else {
			length, err = d.readInt32()
			if err != nil {
				return fmt.Errorf("failed to read array length: %v", err)
			}
			if length == -1 {
				// Null array
				fv.Set(reflect.Zero(fv.Type()))
				return nil
			}
		}
	}
	
	if length < 0 {
		return fmt.Errorf("invalid array length: %d", length)
	}
	
	// Create slice with the specified length
	slice := reflect.MakeSlice(fv.Type(), int(length), int(length))
	
	// Decode each element
	for i := 0; i < int(length); i++ {
		elem := slice.Index(i)
		if err := d.decodeArrayElement(elem, elemType); err != nil {
			return fmt.Errorf("failed to decode array element %d: %v", i, err)
		}
	}
	
	fv.Set(slice)
	return nil
}

// decodeArrayElement decodes a single element in an array
func (d *KDecoder) decodeArrayElement(elem reflect.Value, elemType reflect.Type) error {
	// Check if it's a ktype
	if elemType.PkgPath() == "github.com/codecrafters-io/kafka-starter-go/app/ktypes" {
		// It's a ktype, use the ktype decoder
		return d.decodeKType(elem, reflect.StructField{})
	}
	
	// Handle basic Go types
	switch elemType.Kind() {
	case reflect.String:
		val, err := d.readString()
		if err != nil {
			return err
		}
		elem.SetString(val)
		return nil
		
	case reflect.Int32:
		val, err := d.readInt32()
		if err != nil {
			return err
		}
		elem.SetInt(int64(val))
		return nil
		
	case reflect.Int64:
		val, err := d.readInt64()
		if err != nil {
			return err
		}
		elem.SetInt(val)
		return nil
		
	case reflect.Slice:
		if elemType.Elem().Kind() == reflect.Uint8 {
			// []byte
			val, err := d.readBytes()
			if err != nil {
				return err
			}
			elem.SetBytes(val)
			return nil
		}
		return fmt.Errorf("unsupported slice element type: %v", elemType.Elem())
		
	case reflect.Struct:
		// Check if this struct has ktypes fields with order tags
		// If so, use the struct decoder
		rt := elemType
		hasOrderTags := false
		for i := 0; i < rt.NumField(); i++ {
			field := rt.Field(i)
			if field.Tag.Get("order") != "" {
				hasOrderTags = true
				break
			}
		}
		
		if hasOrderTags {
			// This struct has order tags, use the struct decoder
			return d.decodeStruct(elem)
		}
		
		// For structs without order tags, try to decode each field individually
		// This is a fallback for simple structs
		for i := 0; i < rt.NumField(); i++ {
			field := rt.Field(i)
			fieldValue := elem.Field(i)
			if !fieldValue.CanSet() {
				continue
			}
			
			// Try to decode based on field type
			if err := d.decodeArrayElement(fieldValue, field.Type); err != nil {
				return fmt.Errorf("failed to decode struct field %s: %v", field.Name, err)
			}
		}
		return nil
		
	default:
		return fmt.Errorf("unsupported array element type: %v", elemType)
	}
}
