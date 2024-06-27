package avro

import (
	"fmt"
	"io"
	"reflect"

	"tps-git.topcon.com/cloud/avro/avroschema"
)

type Encoder struct {
	writer io.Writer
	encode EncodeFunc
}

type EncodeFunc func(writer io.Writer, v interface{}) error

func NewEncoder(writer io.Writer, schema avroschema.Schema) *Encoder {
	return &Encoder{
		writer: writer,
		encode: getEncodeFuncForSchema(schema),
	}
}

func (encoder *Encoder) Encode(v interface{}) (err error) {
	return encoder.encode(encoder.writer, v)
}

func getEncodeFuncForSchema(schema avroschema.Schema) EncodeFunc {
	switch schema.GetType() {
	case avroschema.AvroTypeNull:
		return WriteNull
	case avroschema.AvroTypeBoolean:
		return WriteBoolean
	case avroschema.AvroTypeInt:
		return WriteInt
	case avroschema.AvroTypeLong:
		return WriteLong
	case avroschema.AvroTypeFloat:
		panic("float not implemented")
	case avroschema.AvroTypeDouble:
		panic("double not implemented")
	case avroschema.AvroTypeBytes:
		return WriteBytes
	case avroschema.AvroTypeString:
		return WriteString
	case avroschema.AvroTypeRecord:
		avroRecord := schema.(*avroschema.Record)
		return getEncodeFuncForRecord(avroRecord)
	case avroschema.AvroTypeEnum:
		avroEnum := schema.(*avroschema.Enum)
		return getEncodeFuncForEnum(avroEnum)
	case avroschema.AvroTypeArray:
		avroArray := schema.(*avroschema.Array)
		return getEncodeFuncForArray(avroArray)
	case avroschema.AvroTypeMap:
		avroMap := schema.(*avroschema.Map)
		return getEncodeFuncForMap(avroMap)
	case avroschema.AvroTypeFixed:
		avroFixed := schema.(*avroschema.Fixed)
		return getEncodeFuncForFixed(avroFixed)
	case avroschema.AvroTypeUnion:
		avroUnion := schema.(avroschema.Union)
		return getEncodeFuncForUnion(avroUnion)
	default:
		panic(fmt.Sprintf("type %s not implemented", schema.GetType()))
	}
}

func getEncodeFuncForRecord(avroRecord *avroschema.Record) EncodeFunc {
	type frame struct {
		name   string
		encode EncodeFunc
	}
	frames := make([]frame, 0, len(avroRecord.Fields))
	for _, field := range avroRecord.Fields {
		frames = append(frames, frame{
			name:   field.Name,
			encode: getEncodeFuncForSchema(field.Type),
		})
	}
	return func(writer io.Writer, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Struct {
			panic(fmt.Errorf("expected a struct, got %T", v))
		}
		values := make(map[string]interface{})
		typ := reflect.TypeOf(v)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := field.Tag.Get("avro")
			if tag == "" {
				continue
			}
			values[tag] = val.Field(i).Interface()
		}
		for _, frame := range frames {
			value, ok := values[frame.name]
			if !ok {
				return fmt.Errorf("field %s not found", frame.name)
			}
			err = frame.encode(writer, value)
			if err != nil {
				return
			}
		}
		return nil
	}
}

func getEncodeFuncForEnum(avroEnum *avroschema.Enum) EncodeFunc {
	index := make(map[string]int)
	for i, symbol := range avroEnum.Symbols {
		index[symbol] = i
	}
	return func(writer io.Writer, v interface{}) (err error) {
		value := v.(string)
		i, ok := index[value]
		if !ok {
			return fmt.Errorf("symbol %s not found", value)
		}
		return WriteInt(writer, int32(i))
	}
}

func getEncodeFuncForArray(avroArray *avroschema.Array) EncodeFunc {
	encode, ok := encodeArrayByType[avroArray.Items.GetType()]
	if ok {
		return encode
	}
	switch avroArray.Items.GetType() {
	case avroschema.AvroTypeEnum:
		avroEnum := avroArray.Items.(*avroschema.Enum)
		return getEncodeFuncForEnumArray(avroEnum)
	case avroschema.AvroTypeFixed:
		avroFixed := avroArray.Items.(*avroschema.Fixed)
		return getEncodeFuncForFixedArray(avroFixed)
	}
	encode = getEncodeFuncForSchema(avroArray.Items)
	return func(writer io.Writer, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Slice {
			panic(fmt.Errorf("expected a slice, got %T", v))
		}
		err = WriteLong(writer, int64(val.Len()))
		if err != nil {
			return
		}
		for i := 0; i < val.Len(); i++ {
			err = encode(writer, val.Index(i).Interface())
			if err != nil {
				return
			}
		}
		_, err = writer.Write([]byte{0})
		if err != nil {
			return
		}
		return
	}
}

func getEncodeFuncForEnumArray(avroEnum *avroschema.Enum) EncodeFunc {
	encode := getEncodeFuncForEnum(avroEnum)
	return func(writer io.Writer, v interface{}) (err error) {
		value := v.([]string)
		err = WriteLong(writer, int64(len(value)))
		if err != nil {
			return
		}
		for _, symbol := range value {
			err = encode(writer, symbol)
			if err != nil {
				return
			}
		}
		_, err = writer.Write([]byte{0})
		if err != nil {
			return
		}
		return
	}
}

func getEncodeFuncForFixedArray(avroFixed *avroschema.Fixed) EncodeFunc {
	encode := getEncodeFuncForFixed(avroFixed)
	return func(writer io.Writer, v interface{}) (err error) {
		value := v.([][]byte)
		err = WriteLong(writer, int64(len(value)))
		if err != nil {
			return
		}
		for _, bytes := range value {
			err = encode(writer, bytes)
			if err != nil {
				return
			}
		}
		_, err = writer.Write([]byte{0})
		if err != nil {
			return
		}
		return
	}
}

func getEncodeFuncForMap(avroMap *avroschema.Map) EncodeFunc {
	encode, ok := encodeMapByType[avroMap.Values.GetType()]
	if ok {
		return encode
	}
	encode = getEncodeFuncForSchema(avroMap.Values)
	return func(writer io.Writer, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Map {
			panic(fmt.Errorf("expected a map, got %T", v))
		}
		err = WriteLong(writer, int64(val.Len()))
		if err != nil {
			return
		}
		for _, keyVal := range val.MapKeys() {
			key := keyVal.String()
			err = WriteString(writer, key)
			if err != nil {
				return
			}
			err = encode(writer, val.MapIndex(keyVal).Interface())
			if err != nil {
				return
			}
		}
		_, err = writer.Write([]byte{0})
		if err != nil {
			return
		}
		return
	}
}

func getEncodeFuncForFixed(avroFixed *avroschema.Fixed) EncodeFunc {
	size := avroFixed.Size
	return func(writer io.Writer, v interface{}) (err error) {
		value := v.([]byte)
		if len(value) != size {
			return fmt.Errorf("expected %d bytes, got %d", size, len(value))
		}
		_, err = writer.Write(value)
		if err != nil {
			return
		}
		return
	}
}

func getEncodeFuncForUnion(avroUnion avroschema.Union) EncodeFunc {
	indices := make(map[reflect.Type]int)
	encodeFuncs := make([]EncodeFunc, len(avroUnion))
	for i, schema := range avroUnion {
		indices[getGoTypeForSchema(schema)] = i
		encodeFuncs[i] = getEncodeFuncForSchema(schema)
	}
	return func(writer io.Writer, v interface{}) (err error) {
		typ := reflect.TypeOf(v)
		index, ok := indices[typ]
		if !ok {
			panic(fmt.Errorf("type %s not supported", typ))
		}
		err = WriteInt(writer, int32(index))
		if err != nil {
			return
		}
		encode := encodeFuncs[index]
		return encode(writer, v)
	}
}

func getGoTypeForSchema(schema avroschema.Schema) reflect.Type {
	switch schema.GetType() {
	case avroschema.AvroTypeNull:
		return reflect.TypeOf(nil)
	case avroschema.AvroTypeBoolean:
		return reflect.TypeOf(false)
	case avroschema.AvroTypeInt:
		return reflect.TypeOf(int32(0))
	case avroschema.AvroTypeLong:
		return reflect.TypeOf(int64(0))
	case avroschema.AvroTypeFloat:
		return reflect.TypeOf(float32(0))
	case avroschema.AvroTypeDouble:
		return reflect.TypeOf(float64(0))
	case avroschema.AvroTypeBytes:
		return reflect.TypeOf([]byte{})
	case avroschema.AvroTypeString:
		return reflect.TypeOf("")
	case avroschema.AvroTypeRecord:
		panic("record not implemented")
	case avroschema.AvroTypeEnum:
		return reflect.TypeOf("")
	case avroschema.AvroTypeArray:
		panic("array not implemented")
	case avroschema.AvroTypeMap:
		panic("map not implemented")
	case avroschema.AvroTypeFixed:
		return reflect.TypeOf([]byte{})
	case avroschema.AvroTypeUnion:
		panic("union not implemented")
	default:
		panic(fmt.Sprintf("type %s not implemented", schema.GetType()))
	}
}
