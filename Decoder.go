package avro

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/Ryan-A-B/avro-go/avroschema"
	"github.com/Ryan-A-B/avro-go/internal"
)

type Decoder struct {
	reader internal.Reader
	decode internal.DecodeFunc
}

func NewDecoder(reader internal.Reader, schema avroschema.Schema) *Decoder {
	return &Decoder{
		reader: reader,
		decode: getDecodeFuncForSchema(schema),
	}
}

func (decoder *Decoder) Decode(v interface{}) (err error) {
	return decoder.decode(decoder.reader, v)
}

func getDecodeFuncForSchema(schema avroschema.Schema) internal.DecodeFunc {
	switch schema.GetType() {
	case avroschema.AvroTypeBoolean:
		return internal.ReadBoolean
	case avroschema.AvroTypeInt:
		return internal.ReadInt
	case avroschema.AvroTypeLong:
		return internal.ReadLong
	case avroschema.AvroTypeFloat:
		return internal.ReadFloat
	case avroschema.AvroTypeDouble:
		return internal.ReadDouble
	case avroschema.AvroTypeBytes:
		return internal.ReadBytes
	case avroschema.AvroTypeString:
		return internal.ReadString
	case avroschema.AvroTypeRecord:
		avroRecord := schema.(*avroschema.Record)
		return getDecodeFuncForRecord(avroRecord)
	case avroschema.AvroTypeEnum:
		avroEnum := schema.(*avroschema.Enum)
		return getDecodeFuncForEnum(avroEnum)
	case avroschema.AvroTypeArray:
		avroArray := schema.(*avroschema.Array)
		return getDecodeFuncForArray(avroArray)
	case avroschema.AvroTypeMap:
		avroMap := schema.(*avroschema.Map)
		return getDecodeFuncForMap(avroMap)
	case avroschema.AvroTypeFixed:
		avroFixed := schema.(*avroschema.Fixed)
		return getDecodeFuncForFixed(avroFixed)
	case avroschema.AvroTypeUnion:
		avroUnion := schema.(avroschema.Union)
		return getDecodeFuncForUnion(avroUnion)
	default:
		panic("unknown type")
	}
}

func getDecodeFuncForRecord(avroRecord *avroschema.Record) internal.DecodeFunc {
	type frame struct {
		name   string
		decode internal.DecodeFunc
	}
	decodeFuncSlice := make([]frame, 0, len(avroRecord.Fields))
	for _, field := range avroRecord.Fields {
		decode := getDecodeFuncForSchema(field.Type)
		decodeFuncSlice = append(decodeFuncSlice, frame{
			name:   field.Name,
			decode: decode,
		})
	}
	return func(reader internal.Reader, v interface{}) (err error) {
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
			value, ok := values[frame.name]
			if !ok {
				continue
			}
			err = frame.decode(reader, value)
			if err != nil {
				return
			}
		}
		return
	}
}

func getDecodeFuncForEnum(avroEnum *avroschema.Enum) internal.DecodeFunc {
	symbols := avroEnum.Symbols
	return func(reader internal.Reader, v interface{}) (err error) {
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

func getDecodeFuncForArray(avroArray *avroschema.Array) internal.DecodeFunc {
	switch avroArray.Items.GetType() {
	case avroschema.AvroTypeBoolean:
		return internal.ReadBooleanArray
	case avroschema.AvroTypeInt:
		return internal.ReadIntArray
	case avroschema.AvroTypeLong:
		return internal.ReadLongArray
	case avroschema.AvroTypeFloat:
		return internal.ReadFloatArray
	case avroschema.AvroTypeDouble:
		return internal.ReadDoubleArray
	case avroschema.AvroTypeBytes:
		return internal.ReadBytesArray
	case avroschema.AvroTypeString:
		return internal.ReadStringArray
	case avroschema.AvroTypeRecord:
		decode := getDecodeFuncForRecord(avroArray.Items.(*avroschema.Record))
		return getDecodeFuncForComplexArray(decode)
	case avroschema.AvroTypeEnum:
		decode := getDecodeFuncForEnum(avroArray.Items.(*avroschema.Enum))
		return getDecodeFuncForComplexArray(decode)
	case avroschema.AvroTypeArray:
		decode := getDecodeFuncForArray(avroArray.Items.(*avroschema.Array))
		return getDecodeFuncForComplexArray(decode)
	case avroschema.AvroTypeMap:
		decode := getDecodeFuncForMap(avroArray.Items.(*avroschema.Map))
		return getDecodeFuncForComplexArray(decode)
	case avroschema.AvroTypeFixed:
		decode := getDecodeFuncForFixed(avroArray.Items.(*avroschema.Fixed))
		return getDecodeFuncForComplexArray(decode)
	case avroschema.AvroTypeUnion:
		panic("union not implemented")
	default:
		panic("unknown type")
	}
}

func getDecodeFuncForComplexArray(decode internal.DecodeFunc) internal.DecodeFunc {
	return func(reader internal.Reader, v interface{}) (err error) {
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

func getDecodeFuncForMap(avroMap *avroschema.Map) internal.DecodeFunc {
	switch avroMap.Values.GetType() {
	case avroschema.AvroTypeBoolean:
		return internal.ReadBooleanMap
	case avroschema.AvroTypeInt:
		return internal.ReadIntMap
	case avroschema.AvroTypeLong:
		return internal.ReadLongMap
	case avroschema.AvroTypeFloat:
		return internal.ReadFloatMap
	case avroschema.AvroTypeDouble:
		return internal.ReadDoubleMap
	case avroschema.AvroTypeBytes:
		return internal.ReadBytesMap
	case avroschema.AvroTypeString:
		return internal.ReadStringMap
	case avroschema.AvroTypeRecord:
		decode := getDecodeFuncForRecord(avroMap.Values.(*avroschema.Record))
		return getDecodeFuncForComplexMap(decode)
	case avroschema.AvroTypeEnum:
		decode := getDecodeFuncForEnum(avroMap.Values.(*avroschema.Enum))
		return getDecodeFuncForComplexMap(decode)
	case avroschema.AvroTypeArray:
		decode := getDecodeFuncForArray(avroMap.Values.(*avroschema.Array))
		return getDecodeFuncForComplexMap(decode)
	case avroschema.AvroTypeMap:
		decode := getDecodeFuncForMap(avroMap.Values.(*avroschema.Map))
		return getDecodeFuncForComplexMap(decode)
	case avroschema.AvroTypeFixed:
		decode := getDecodeFuncForFixed(avroMap.Values.(*avroschema.Fixed))
		return getDecodeFuncForComplexMap(decode)
	case avroschema.AvroTypeUnion:
		panic("union not implemented")
	default:
		panic("unknown type")
	}
}

func getDecodeFuncForComplexMap(decode internal.DecodeFunc) internal.DecodeFunc {
	return func(reader internal.Reader, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Ptr {
			panic(fmt.Errorf("pointer expected, got %s", val.Kind()))
		}
		val = val.Elem()
		if val.Kind() != reflect.Map {
			panic(fmt.Errorf("map expected, got %s", val.Kind()))
		}
		itemType := reflect.TypeOf(v).Elem().Elem()
		blockLength, err := binary.ReadVarint(reader)
		if err != nil {
			return
		}
		for blockLength != 0 {
			for i := int64(0); i < blockLength; i++ {
				key := reflect.New(reflect.TypeOf("")).Interface()
				err = internal.ReadString(reader, key)
				if err != nil {
					return
				}
				item := reflect.New(itemType).Interface()
				err = decode(reader, item)
				if err != nil {
					return
				}
				val.SetMapIndex(reflect.ValueOf(key).Elem(), reflect.ValueOf(item).Elem())
			}
			blockLength, err = binary.ReadVarint(reader)
			if err != nil {
				return
			}
		}
		return
	}
}

func getDecodeFuncForFixed(avroFixed *avroschema.Fixed) internal.DecodeFunc {
	expectedLength := avroFixed.Size
	return func(reader internal.Reader, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Ptr {
			panic(fmt.Errorf("pointer expected, got %s", val.Kind()))
		}
		val = val.Elem()
		switch val.Kind() {
		case reflect.Slice:
			if val.Len() != expectedLength {
				panic(fmt.Errorf("expected %d bytes, got %d", expectedLength, val.Len()))
			}
			_, err = reader.Read(val.Bytes())
			return
		case reflect.Array:
			if val.Len() != expectedLength {
				panic(fmt.Errorf("expected %d bytes, got %d", expectedLength, val.Len()))
			}
			_, err = reader.Read(val.Slice(0, expectedLength).Bytes())
			return
		default:
			panic(fmt.Errorf("slice or array expected, got %s", val.Kind()))
		}
	}
}

func getDecodeFuncForUnion(avroUnion avroschema.Union) internal.DecodeFunc {
	if internal.IsOptional(avroUnion) {
		return getDecodeFuncForOptional(avroUnion)
	}
	decodeFuncs := make([]internal.DecodeFunc, 0, len(avroUnion))
	for _, schema := range avroUnion {
		var decode internal.DecodeFunc
		switch schema.GetType() {
		case avroschema.AvroTypeNull:
			decode = internal.ReadUnionNull
		case avroschema.AvroTypeBoolean:
			decode = internal.ReadUnionBoolean
		case avroschema.AvroTypeInt:
			decode = internal.ReadUnionInt
		case avroschema.AvroTypeLong:
			decode = internal.ReadUnionLong
		case avroschema.AvroTypeFloat:
			decode = internal.ReadUnionFloat
		case avroschema.AvroTypeDouble:
			decode = internal.ReadUnionDouble
		case avroschema.AvroTypeBytes:
			decode = internal.ReadUnionBytes
		case avroschema.AvroTypeString:
			decode = internal.ReadUnionString
		case avroschema.AvroTypeRecord:
			panic("record not implemented")
		case avroschema.AvroTypeEnum:
			avroEnum := schema.(*avroschema.Enum)
			decode = internal.MakeReadUnionValue(reflect.TypeOf(""), getDecodeFuncForEnum(avroEnum))
		case avroschema.AvroTypeArray:
			avroArray := schema.(*avroschema.Array)
			decode = getDecodeFuncForUnionArray(avroArray)
		case avroschema.AvroTypeMap:
			avroMap := schema.(*avroschema.Map)
			decode = getDecodeFuncForUnionMap(avroMap)
		case avroschema.AvroTypeFixed:
			avroFixed := schema.(*avroschema.Fixed)
			decode = getDecodeFuncForUnionFixed(avroFixed)
		case avroschema.AvroTypeUnion:
			panic("not allowed")
		default:
			panic("unknown type")
		}
		decodeFuncs = append(decodeFuncs, decode)
	}
	return func(reader internal.Reader, v interface{}) (err error) {
		index, err := binary.ReadVarint(reader)
		if err != nil {
			return
		}
		if index < 0 || int(index) >= len(decodeFuncs) {
			err = fmt.Errorf("invalid union index %d", index)
			return
		}
		decode := decodeFuncs[index]
		return decode(reader, v)
	}
}

func getDecodeFuncForOptional(avroUnion avroschema.Union) internal.DecodeFunc {
	var decode internal.DecodeFunc
	indexOfNull := int64(0)
	for i, schema := range avroUnion {
		switch schema.GetType() {
		case avroschema.AvroTypeNull:
			indexOfNull = int64(i)
		default:
			decode = getDecodeFuncForSchema(schema)
		}
	}
	return func(reader internal.Reader, v interface{}) (err error) {
		index, err := binary.ReadVarint(reader)
		if err != nil {
			return
		}
		if index == indexOfNull {
			internal.ReadUnionNull(reader, v)
			return
		}
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Ptr {
			panic(fmt.Errorf("pointer expected, got %s", val.Kind()))
		}
		val = val.Elem()
		if val.Kind() != reflect.Ptr {
			return decode(reader, v)
		}
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		return decode(reader, val.Interface())
	}
}

func getDecodeFuncForUnionArray(avroArray *avroschema.Array) internal.DecodeFunc {
	switch avroArray.Items.GetType() {
	case avroschema.AvroTypeBoolean:
		return internal.ReadUnionBooleanArray
	case avroschema.AvroTypeInt:
		return internal.ReadUnionIntArray
	case avroschema.AvroTypeLong:
		return internal.ReadUnionLongArray
	case avroschema.AvroTypeFloat:
		return internal.ReadUnionFloatArray
	case avroschema.AvroTypeDouble:
		return internal.ReadUnionDoubleArray
	case avroschema.AvroTypeBytes:
		return internal.ReadUnionBytesArray
	case avroschema.AvroTypeString:
		return internal.ReadUnionStringArray
	case avroschema.AvroTypeRecord:
		panic("record not implemented")
	case avroschema.AvroTypeEnum:
		panic("enum not implemented")
	case avroschema.AvroTypeArray:
		panic("array not implemented")
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

func getDecodeFuncForUnionMap(avroMap *avroschema.Map) internal.DecodeFunc {
	switch avroMap.Values.GetType() {
	case avroschema.AvroTypeBoolean:
		return internal.ReadUnionBooleanMap
	case avroschema.AvroTypeInt:
		return internal.ReadUnionIntMap
	case avroschema.AvroTypeLong:
		return internal.ReadUnionLongMap
	case avroschema.AvroTypeFloat:
		return internal.ReadUnionFloatMap
	case avroschema.AvroTypeDouble:
		return internal.ReadUnionDoubleMap
	case avroschema.AvroTypeBytes:
		return internal.ReadUnionBytesMap
	case avroschema.AvroTypeString:
		return internal.ReadUnionStringMap
	case avroschema.AvroTypeRecord:
		panic("record not implemented")
	case avroschema.AvroTypeEnum:
		panic("enum not implemented")
	case avroschema.AvroTypeArray:
		panic("array not implemented")
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

func getDecodeFuncForUnionFixed(avroFixed *avroschema.Fixed) internal.DecodeFunc {
	expectedLength := avroFixed.Size
	decode := getDecodeFuncForFixed(avroFixed)
	return func(reader internal.Reader, v interface{}) (err error) {
		value := make([]byte, expectedLength)
		err = decode(reader, &value)
		if err != nil {
			return
		}
		reflect.ValueOf(v).Elem().Set(reflect.ValueOf(value))
		return
	}
}
