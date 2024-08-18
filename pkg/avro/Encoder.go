package avro

import (
	"fmt"
	"io"
	"reflect"

	"github.com/Ryan-A-B/avro-go/internal"
	"github.com/Ryan-A-B/avro-go/pkg/avroschema"
)

type Encoder struct {
	writer io.Writer
	encode internal.EncodeFunc
}

func NewEncoder(writer io.Writer, schema avroschema.Schema) *Encoder {
	return &Encoder{
		writer: writer,
		encode: getEncodeFuncForSchema(schema),
	}
}

func (encoder *Encoder) Encode(v interface{}) (err error) {
	return encoder.encode(encoder.writer, v)
}

func getEncodeFuncForSchema(schema avroschema.Schema) internal.EncodeFunc {
	switch schema.GetType() {
	case avroschema.AvroTypeNull:
		return internal.WriteNull
	case avroschema.AvroTypeBoolean:
		return internal.WriteBoolean
	case avroschema.AvroTypeInt:
		return internal.WriteInt
	case avroschema.AvroTypeLong:
		return internal.WriteLong
	case avroschema.AvroTypeFloat:
		return internal.WriteFloat
	case avroschema.AvroTypeDouble:
		return internal.WriteDouble
	case avroschema.AvroTypeBytes:
		return internal.WriteBytes
	case avroschema.AvroTypeString:
		return internal.WriteString
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

func getEncodeFuncForRecord(avroRecord *avroschema.Record) internal.EncodeFunc {
	type frame struct {
		name   string
		encode internal.EncodeFunc
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

func getEncodeFuncForEnum(avroEnum *avroschema.Enum) internal.EncodeFunc {
	index := make(map[string]int)
	for i, symbol := range avroEnum.Symbols {
		index[symbol] = i
	}
	return func(writer io.Writer, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		value := val.String()
		i, ok := index[value]
		if !ok {
			return fmt.Errorf("symbol %s not found", value)
		}
		return internal.WriteInt(writer, int32(i))
	}
}

func getEncodeFuncForArray(avroArray *avroschema.Array) internal.EncodeFunc {
	encode, ok := internal.EncodeArrayByType[avroArray.Items.GetType()]
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
		err = internal.WriteLong(writer, int64(val.Len()))
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

func getEncodeFuncForEnumArray(avroEnum *avroschema.Enum) internal.EncodeFunc {
	encode := getEncodeFuncForEnum(avroEnum)
	return func(writer io.Writer, v interface{}) (err error) {
		value := v.([]string)
		err = internal.WriteLong(writer, int64(len(value)))
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

func getEncodeFuncForFixedArray(avroFixed *avroschema.Fixed) internal.EncodeFunc {
	encode := getEncodeFuncForFixed(avroFixed)
	return func(writer io.Writer, v interface{}) (err error) {
		value := v.([][]byte)
		err = internal.WriteLong(writer, int64(len(value)))
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

func getEncodeFuncForMap(avroMap *avroschema.Map) internal.EncodeFunc {
	encode, ok := internal.EncodeMapByType[avroMap.Values.GetType()]
	if ok {
		return encode
	}
	encode = getEncodeFuncForSchema(avroMap.Values)
	return func(writer io.Writer, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Map {
			panic(fmt.Errorf("expected a map, got %T", v))
		}
		err = internal.WriteLong(writer, int64(val.Len()))
		if err != nil {
			return
		}
		for _, keyVal := range val.MapKeys() {
			key := keyVal.String()
			err = internal.WriteString(writer, key)
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

func getEncodeFuncForFixed(avroFixed *avroschema.Fixed) internal.EncodeFunc {
	size := avroFixed.Size
	return func(writer io.Writer, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Len() != size {
			return fmt.Errorf("expected %d bytes, got %d", size, val.Len())
		}
		switch val.Kind() {
		case reflect.Array:
			var addressableArray reflect.Value = val
			if !val.CanAddr() {
				addressableArray = reflect.New(reflect.ArrayOf(val.Len(), val.Type().Elem())).Elem()
				addressableArray.Set(val)
			}
			_, err = writer.Write(addressableArray.Slice(0, size).Bytes())
			if err != nil {
				return
			}
			return
		case reflect.Slice:
			_, err = writer.Write(val.Bytes())
			if err != nil {
				return
			}
			return
		default:
			panic(fmt.Errorf("expected an array or slice, got %T", v))
		}
	}
}

func getEncodeFuncForUnion(avroUnion avroschema.Union) internal.EncodeFunc {
	if internal.IsOptional(avroUnion) {
		return getEncodeFuncForOptional(avroUnion)
	}
	indexByType := make(map[reflect.Type]int)
	encodeFuncs := make([]internal.EncodeFunc, len(avroUnion))
	for i, schema := range avroUnion {
		indexByType[getGoTypeForSchema(schema)] = i
		encodeFuncs[i] = getEncodeFuncForSchema(schema)
	}
	return func(writer io.Writer, v interface{}) (err error) {
		typ := reflect.TypeOf(v)
		index, ok := indexByType[typ]
		if !ok {
			panic(fmt.Errorf("type %s not supported", typ))
		}
		err = internal.WriteInt(writer, int32(index))
		if err != nil {
			return
		}
		encode := encodeFuncs[index]
		return encode(writer, v)
	}
}

func getEncodeFuncForOptional(avroUnion avroschema.Union) internal.EncodeFunc {
	var encode internal.EncodeFunc
	indexOfNull := int64(0)
	indexOfValue := int64(1)
	for i, schema := range avroUnion {
		if schema.GetType() == avroschema.AvroTypeNull {
			indexOfNull = int64(i)
		} else {
			indexOfValue = int64(i)
			encode = getEncodeFuncForSchema(schema)
		}
	}
	return func(writer io.Writer, v interface{}) (err error) {
		val := reflect.ValueOf(v)
		if val.Kind() != reflect.Ptr {
			return encode(writer, v)
		}
		if val.IsNil() {
			return internal.WriteLong(writer, indexOfNull)
		}
		err = internal.WriteLong(writer, indexOfValue)
		if err != nil {
			return
		}
		return encode(writer, val.Elem().Interface())
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
		panic("union of union not allowed")
	default:
		panic(fmt.Sprintf("type %s not implemented", schema.GetType()))
	}
}
