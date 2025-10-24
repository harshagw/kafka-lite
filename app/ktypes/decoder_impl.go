package ktypes

import (
	"encoding/binary"
	"errors"
)

// Basic integer reading methods

func (d *KDecoder) readInt8() (int8, error) {
	if d.pos >= len(d.data) {
		return 0, errors.New("out of bounds: cannot read int8")
	}
	val := int8(d.data[d.pos])
	d.pos++
	return val, nil
}

func (d *KDecoder) readInt16() (int16, error) {
	if d.pos+2 > len(d.data) {
		return 0, errors.New("out of bounds: cannot read int16")
	}
	val := int16(binary.BigEndian.Uint16(d.data[d.pos : d.pos+2]))
	d.pos += 2
	return val, nil
}

func (d *KDecoder) readInt32() (int32, error) {
	if d.pos+4 > len(d.data) {
		return 0, errors.New("out of bounds: cannot read int32")
	}
	val := int32(binary.BigEndian.Uint32(d.data[d.pos : d.pos+4]))
	d.pos += 4
	return val, nil
}

func (d *KDecoder) readInt64() (int64, error) {
	if d.pos+8 > len(d.data) {
		return 0, errors.New("out of bounds: cannot read int64")
	}
	val := int64(binary.BigEndian.Uint64(d.data[d.pos : d.pos+8]))
	d.pos += 8
	return val, nil
}

func (d *KDecoder) readUint16() (uint16, error) {
	if d.pos+2 > len(d.data) {
		return 0, errors.New("out of bounds: cannot read uint16")
	}
	val := binary.BigEndian.Uint16(d.data[d.pos : d.pos+2])
	d.pos += 2
	return val, nil
}

func (d *KDecoder) readUint32() (uint32, error) {
	if d.pos+4 > len(d.data) {
		return 0, errors.New("out of bounds: cannot read uint32")
	}
	val := binary.BigEndian.Uint32(d.data[d.pos : d.pos+4])
	d.pos += 4
	return val, nil
}

func (d *KDecoder) readBoolean() (bool, error) {
	if d.pos >= len(d.data) {
		return false, errors.New("out of bounds: cannot read boolean")
	}
	val := d.data[d.pos] != 0
	d.pos++
	return val, nil
}

func (d *KDecoder) readFloat64() (float64, error) {
	if d.pos+8 > len(d.data) {
		return 0, errors.New("out of bounds: cannot read float64")
	}
	bits := binary.BigEndian.Uint64(d.data[d.pos : d.pos+8])
	val := float64(bits)
	d.pos += 8
	return val, nil
}

// Variable-length integer reading methods

func (d *KDecoder) readVarInt() (int32, error) {
	val, err := d.readVarInt64()
	if err != nil {
		return 0, err
	}
	// Apply zig-zag decoding: (n >> 1) ^ (-(n & 1))
	decoded := (val >> 1) ^ (-(val & 1))
	return int32(decoded), nil
}

func (d *KDecoder) readVarLong() (int64, error) {
	val, err := d.readVarInt64()
	if err != nil {
		return 0, err
	}
	// Apply zig-zag decoding: (n >> 1) ^ (-(n & 1))
	return (val >> 1) ^ (-(val & 1)), nil
}

func (d *KDecoder) readUnsignedVarInt() (uint32, error) {
	val, err := d.readVarUint64()
	if err != nil {
		return 0, err
	}
	return uint32(val), nil
}

func (d *KDecoder) readUnsignedVarLong() (uint64, error) {
	return d.readVarUint64()
}

func (d *KDecoder) readVarInt64() (int64, error) {
	var result int64
	var shift uint
	
	for {
		if d.pos >= len(d.data) {
			return 0, errors.New("out of bounds: cannot read varint")
		}
		
		byte := d.data[d.pos]
		d.pos++
		
		result |= int64(byte&0x7F) << shift
		
		if (byte & 0x80) == 0 {
			break
		}
		
		shift += 7
		if shift >= 64 {
			return 0, errors.New("varint too large")
		}
	}
	
	return result, nil
}

func (d *KDecoder) readVarUint64() (uint64, error) {
	var result uint64
	var shift uint
	
	for {
		if d.pos >= len(d.data) {
			return 0, errors.New("out of bounds: cannot read varuint")
		}
		
		bt := d.data[d.pos]
		d.pos++
		
		result |= uint64(bt&0x7F) << shift
		
		if (bt & 0x80) == 0 {
			break
		}
		
		shift += 7
		if shift >= 64 {
			return 0, errors.New("varuint too large")
		}
	}
	
	return result, nil
}

// String reading methods

func (d *KDecoder) readString() (string, error) {
	length, err := d.readInt16()
	if err != nil {
		return "", err
	}
	
	if length < 0 {
		return "", errors.New("invalid string length")
	}
	
	if d.pos+int(length) > len(d.data) {
		return "", errors.New("out of bounds: cannot read string")
	}
	
	val := string(d.data[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return val, nil
}

func (d *KDecoder) readNullableString() (*string, error) {
	length, err := d.readInt16()
	if err != nil {
		return nil, err
	}
	
	if length == -1 {
		return nil, nil
	}
	
	if length < 0 {
		return nil, errors.New("invalid string length")
	}
	
	if d.pos+int(length) > len(d.data) {
		return nil, errors.New("out of bounds: cannot read string")
	}
	
	val := string(d.data[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return &val, nil
}

func (d *KDecoder) readCompactString() (string, error) {
	length, err := d.readUnsignedVarInt()
	if err != nil {
		return "", err
	}
	
	if length == 0 {
		return "", errors.New("compact string length cannot be 0")
	}
	
	actualLength := length - 1
	
	if d.pos+int(actualLength) > len(d.data) {
		return "", errors.New("out of bounds: cannot read compact string")
	}
	
	val := string(d.data[d.pos : d.pos+int(actualLength)])
	d.pos += int(actualLength)
	return val, nil
}

func (d *KDecoder) readCompactNullableString() (*string, error) {
	length, err := d.readUnsignedVarInt()
	if err != nil {
		return nil, err
	}
	
	if length == 0 {
		return nil, nil
	}
	
	actualLength := length - 1
	
	if d.pos+int(actualLength) > len(d.data) {
		return nil, errors.New("out of bounds: cannot read compact nullable string")
	}
	
	val := string(d.data[d.pos : d.pos+int(actualLength)])
	d.pos += int(actualLength)
	return &val, nil
}

// Bytes reading methods

func (d *KDecoder) readBytes() ([]byte, error) {
	length, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	
	if length < 0 {
		return nil, errors.New("invalid bytes length")
	}
	
	if d.pos+int(length) > len(d.data) {
		return nil, errors.New("out of bounds: cannot read bytes")
	}
	
	val := make([]byte, length)
	copy(val, d.data[d.pos:d.pos+int(length)])
	d.pos += int(length)
	return val, nil
}

func (d *KDecoder) readNullableBytes() ([]byte, error) {
	length, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	
	if length == -1 {
		return nil, nil
	}
	
	if length < 0 {
		return nil, errors.New("invalid bytes length")
	}
	
	if d.pos+int(length) > len(d.data) {
		return nil, errors.New("out of bounds: cannot read bytes")
	}
	
	val := make([]byte, length)
	copy(val, d.data[d.pos:d.pos+int(length)])
	d.pos += int(length)
	return val, nil
}

func (d *KDecoder) readCompactBytes() ([]byte, error) {
	length, err := d.readUnsignedVarInt()
	if err != nil {
		return nil, err
	}
	
	if length == 0 {
		return nil, errors.New("compact bytes length cannot be 0")
	}
	
	actualLength := length - 1
	
	if d.pos+int(actualLength) > len(d.data) {
		return nil, errors.New("out of bounds: cannot read compact bytes")
	}
	
	val := make([]byte, actualLength)
	copy(val, d.data[d.pos:d.pos+int(actualLength)])
	d.pos += int(actualLength)
	return val, nil
}

func (d *KDecoder) readCompactNullableBytes() ([]byte, error) {
	length, err := d.readUnsignedVarInt()
	if err != nil {
		return nil, err
	}
	
	if length == 0 {
		return nil, nil
	}
	
	actualLength := length - 1
	
	if d.pos+int(actualLength) > len(d.data) {
		return nil, errors.New("out of bounds: cannot read compact nullable bytes")
	}
	
	val := make([]byte, actualLength)
	copy(val, d.data[d.pos:d.pos+int(actualLength)])
	d.pos += int(actualLength)
	return val, nil
}

func (d *KDecoder) readRecords() ([]byte, error) {
	return d.readNullableBytes()
}

func (d *KDecoder) readCompactRecords() ([]byte, error) {
	return d.readCompactNullableBytes()
}

func (d *KDecoder) readUUID() (UUID, error) {
	if d.pos+16 > len(d.data) {
		return UUID{}, errors.New("out of bounds: cannot read UUID")
	}
	
	var uuid UUID
	copy(uuid[:], d.data[d.pos:d.pos+16])
	d.pos += 16
	return uuid, nil
}

