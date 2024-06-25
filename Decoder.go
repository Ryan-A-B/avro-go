package avro

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"tps-git.topcon.com/cloud/avro/avroschema"
)

type Reader interface {
	io.Reader
	io.ByteReader
}

type Decoder struct {
	reader Reader
	decode decodeFunc
}

type decodeFunc func(reader Reader, v interface{}) error

func NewDecoder(reader Reader, schema avroschema.Schema) *Decoder {
	return &Decoder{
		reader: reader,
		decode: getDecodeFuncForSchema(schema),
	}
}

func getDecodeFuncForSchema(schema avroschema.Schema) decodeFunc {
	switch schema.GetType() {
	case avroschema.AvroTypeBoolean:
		return ReadBoolean
	case avroschema.AvroTypeInt:
		return ReadInt
	case avroschema.AvroTypeLong:
		return ReadLong
	case avroschema.AvroTypeFloat:
		panic("float not implemented")
	case avroschema.AvroTypeDouble:
		panic("double not implemented")
	case avroschema.AvroTypeBytes:
		return ReadBytes
	case avroschema.AvroTypeString:
		return ReadString
	case avroschema.AvroTypeRecord:
		avroRecord := schema.(*avroschema.Record)
		return getDecodeFuncForRecord(avroRecord)
	case avroschema.AvroTypeEnum:
		return getDecodeFuncForEnum(schema.(*avroschema.Enum))
	case avroschema.AvroTypeArray:
		avroArray := schema.(*avroschema.Array)
		return getDecodeFuncForArray(avroArray)
	case avroschema.AvroTypeMap:
		panic("map not implemented")
	case avroschema.AvroTypeFixed:
		panic("fixed not implemented")
	case avroschema.AvroTypeUnion:
		panic("union not implemented")
	default:
		panic("unknown type")
	}
}

func getDecodeFuncForRecord(schema *avroschema.Record) decodeFunc {
	type frame struct {
		name   string
		decode decodeFunc
	}
	decodeFuncSlice := make([]frame, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		decode := getDecodeFuncForSchema(field.Type)
		decodeFuncSlice = append(decodeFuncSlice, frame{
			name:   field.Name,
			decode: decode,
		})
	}
	return func(reader Reader, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Ptr {
			panic(fmt.Errorf("pointer expected, got %s", val.Kind()))
		}
		val = val.Elem()
		if val.Kind() != reflect.Struct {
			panic(fmt.Errorf("struct expected, got %s", val.Kind()))
		}
		values := make(map[string]interface{})
		typ := val.Type()
		for i := 0; i < typ.NumField(); i++ {
			tag := typ.Field(i).Tag.Get("avro")
			values[tag] = val.Field(i).Addr().Interface()
		}
		for _, frame := range decodeFuncSlice {
			err = frame.decode(reader, values[frame.name])
			if err != nil {
				return
			}
		}
		return
	}
}

func getDecodeFuncForEnum(schema *avroschema.Enum) decodeFunc {
	symbols := schema.Symbols
	return func(reader Reader, v interface{}) (err error) {
		value := v.(*string)
		index, err := binary.ReadVarint(reader)
		if err != nil {
			return
		}
		if index < 0 || int(index) >= len(symbols) {
			err = fmt.Errorf("invalid enum index %d", index)
			return
		}
		*value = symbols[index]
		return
	}
}

func getDecodeFuncForArray(schema *avroschema.Array) decodeFunc {
	switch schema.Items.GetType() {
	case avroschema.AvroTypeBoolean:
		return ReadBooleanArray
	case avroschema.AvroTypeInt:
		return ReadIntArray
	case avroschema.AvroTypeLong:
		return ReadLongArray
	case avroschema.AvroTypeFloat:
		panic("float not implemented")
	case avroschema.AvroTypeDouble:
		panic("double not implemented")
	case avroschema.AvroTypeBytes:
		return ReadBytesArray
	case avroschema.AvroTypeString:
		return ReadStringArray
	case avroschema.AvroTypeRecord:
		decode := getDecodeFuncForRecord(schema.Items.(*avroschema.Record))
		return getDecodeFuncForComplexArray(decode)
	case avroschema.AvroTypeEnum:
		decode := getDecodeFuncForEnum(schema.Items.(*avroschema.Enum))
		return getDecodeFuncForComplexArray(decode)
	case avroschema.AvroTypeArray:
		decode := getDecodeFuncForArray(schema.Items.(*avroschema.Array))
		return getDecodeFuncForComplexArray(decode)
	case avroschema.AvroTypeMap:
		panic("map not implemented")
	case avroschema.AvroTypeFixed:
		panic("fixed not implemented")
	case avroschema.AvroTypeUnion:
		panic("union not implemented")
	default:
		panic("unknown type")
	}
}

func getDecodeFuncForComplexArray(decode decodeFunc) decodeFunc {
	return func(reader Reader, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Ptr {
			panic(fmt.Errorf("pointer expected, got %s", val.Kind()))
		}
		val = val.Elem()
		if val.Kind() != reflect.Slice {
			panic(fmt.Errorf("slice expected, got %s", val.Kind()))
		}
		itemType := reflect.TypeOf(v).Elem().Elem()
		blockLength, err := binary.ReadVarint(reader)
		if err != nil {
			return
		}
		for blockLength != 0 {
			for i := int64(0); i < blockLength; i++ {
				item := reflect.New(itemType).Interface()
				err = decode(reader, item)
				if err != nil {
					return
				}
				val.Set(reflect.Append(val, reflect.ValueOf(item).Elem()))
			}
			blockLength, err = binary.ReadVarint(reader)
			if err != nil {
				return
			}
		}
		return
	}
}

func (decoder *Decoder) Decode(v interface{}) (err error) {
	return decoder.decode(decoder.reader, v)
}

func ReadBoolean(reader Reader, v interface{}) (err error) {
	value := v.(*bool)
	data := [1]byte{}
	_, err = reader.Read(data[:])
	if err != nil {
		return
	}
	*value = data[0] == 1
	return
}

func ReadInt(reader Reader, v interface{}) (err error) {
	value := v.(*int32)
	x, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	*value = int32(x)
	return
}

func ReadLong(reader Reader, v interface{}) (err error) {
	value := v.(*int64)
	x, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	*value = x
	return
}

func ReadBytes(reader Reader, v interface{}) (err error) {
	value := v.(*[]byte)
	length, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return
	}
	*value = data
	return
}

func ReadString(reader Reader, v interface{}) (err error) {
	value := v.(*string)
	var data []byte
	err = ReadBytes(reader, &data)
	if err != nil {
		return
	}
	*value = string(data)
	return
}

// func (decoder *Decoder) readEnum(v interface{}) (err error) {
// 	enum := decoder.schema.(*avroschema.Enum)
// 	value, ok := v.(*string)
// 	if !ok {
// 		err = ErrInvalidType
// 		return
// 	}
// 	index, err := binary.ReadVarint(decoder.reader)
// 	if err != nil {
// 		return
// 	}
// 	if index < 0 || int(index) >= len(enum.Symbols) {
// 		err = fmt.Errorf("invalid enum index %d", index)
// 		return
// 	}
// 	*value = enum.Symbols[index]
// 	return
// }

// func (decoder *Decoder) readMap(v interface{}) (err error) {
// 	avroMap := decoder.schema.(*avroschema.Map)
// 	switch avroMap.Values.GetType() {
// 	case avroschema.AvroTypeBoolean:
// 		return ReadBooleanMap(decoder.reader, v)
// 	case avroschema.AvroTypeInt:
// 		return ReadIntMap(decoder.reader, v)
// 	case avroschema.AvroTypeLong:
// 		return ReadLongMap(decoder.reader, v)
// 	case avroschema.AvroTypeFloat:
// 		return errors.New("float not implemented")
// 	case avroschema.AvroTypeDouble:
// 		return errors.New("double not implemented")
// 	case avroschema.AvroTypeBytes:
// 		return ReadBytesMap(decoder.reader, v)
// 	case avroschema.AvroTypeString:
// 		return ReadStringMap(decoder.reader, v)
// 	default:
// 		return fmt.Errorf("type %s not implemented", avroMap.Values.GetType())
// 	}
// }

// func (decoder *Decoder) readFixed(v interface{}) (err error) {
// 	fixed := decoder.schema.(*avroschema.Fixed)
// 	value, ok := v.(*[]byte)
// 	if !ok {
// 		err = ErrInvalidType
// 		return
// 	}
// 	if len(*value) != fixed.Size {
// 		err = fmt.Errorf("expected %d bytes, got %d", fixed.Size, len(*value))
// 		return
// 	}
// 	_, err = decoder.reader.Read(*value)
// 	if err != nil {
// 		return
// 	}
// 	return
// }

// func (decoder *Decoder) readUnion(v interface{}) (err error) {
// 	union := decoder.schema.(avroschema.Union)
// 	index, err := binary.ReadVarint(decoder.reader)
// 	if err != nil {
// 		return
// 	}
// 	if index < 0 || int(index) >= len(union) {
// 		err = fmt.Errorf("invalid union index %d", index)
// 		return
// 	}
// 	schema := union[index]
// 	val := reflect.ValueOf(v).Elem()
// 	switch schema.GetType() {
// 	case avroschema.AvroTypeNull:
// 		return reset(v)
// 	case avroschema.AvroTypeBoolean:
// 		var value bool
// 		err = ReadBoolean(decoder.reader, &value)
// 		if err != nil {
// 			return
// 		}
// 		val.Set(reflect.ValueOf(value))
// 		return
// 	case avroschema.AvroTypeInt:
// 		var value int32
// 		err = ReadInt(decoder.reader, &value)
// 		if err != nil {
// 			return
// 		}
// 		val.Set(reflect.ValueOf(value))
// 		return
// 	case avroschema.AvroTypeLong:
// 		var value int64
// 		err = ReadLong(decoder.reader, &value)
// 		if err != nil {
// 			return
// 		}
// 		val.Set(reflect.ValueOf(value))
// 		return
// 	case avroschema.AvroTypeFloat:
// 		return errors.New("float not implemented")
// 	case avroschema.AvroTypeDouble:
// 		return errors.New("double not implemented")
// 	case avroschema.AvroTypeBytes:
// 		var value []byte
// 		err = ReadBytes(decoder.reader, &value)
// 		if err != nil {
// 			return
// 		}
// 		val.Set(reflect.ValueOf(value))
// 		return
// 	case avroschema.AvroTypeString:
// 		var value string
// 		err = ReadString(decoder.reader, &value)
// 		if err != nil {
// 			return
// 		}
// 		val.Set(reflect.ValueOf(value))
// 		return
// 	default:
// 		return fmt.Errorf("type %s not implemented", schema.GetType())
// 	}
// }

// func reset(v interface{}) (err error) {
// 	val := reflect.ValueOf(v)
// 	if val.Kind() != reflect.Ptr {
// 		err = errors.New("pointer expected")
// 		return
// 	}
// 	val = val.Elem()
// 	val.Set(reflect.Zero(val.Type()))
// 	return
// }

func ReadBooleanArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]bool)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]bool, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value bool
			err = ReadBoolean(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadIntArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]int32)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]int32, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value int32
			err = ReadInt(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadLongArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]int64)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]int64, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value int64
			err = ReadLong(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadBytesArray(reader Reader, v interface{}) (err error) {
	value := v.(*[][]byte)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([][]byte, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value []byte
			err = ReadBytes(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadStringArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]string)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]string, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value string
			err = ReadString(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadBooleanMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]bool)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]bool, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value bool
			err = ReadBoolean(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadIntMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]int32)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]int32, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value int32
			err = ReadInt(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadLongMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]int64)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]int64, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value int64
			err = ReadLong(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadBytesMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string][]byte)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string][]byte, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value []byte
			err = ReadBytes(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadStringMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]string)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]string, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value string
			err = ReadString(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}
