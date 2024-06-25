package avro

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"

	"tps-git.topcon.com/cloud/avro/avroschema"
)

type Encoder struct {
	writer io.Writer
	schema avroschema.Schema
}

func NewEncoder(writer io.Writer, schema avroschema.Schema) *Encoder {
	return &Encoder{
		writer: writer,
		schema: schema,
	}
}

func (encoder *Encoder) Encode(v interface{}) (err error) {
	switch encoder.schema.GetType() {
	case avroschema.AvroTypeNull:
		return
	case avroschema.AvroTypeBoolean:
		return WriteBoolean(encoder.writer, v.(bool))
	case avroschema.AvroTypeInt:
		return WriteInt(encoder.writer, v.(int32))
	case avroschema.AvroTypeLong:
		return WriteLong(encoder.writer, v.(int64))
	case avroschema.AvroTypeFloat:
		return errors.New("float not implemented")
	case avroschema.AvroTypeDouble:
		return errors.New("double not implemented")
	case avroschema.AvroTypeBytes:
		return WriteBytes(encoder.writer, v.([]byte))
	case avroschema.AvroTypeString:
		return WriteString(encoder.writer, v.(string))
	case avroschema.AvroTypeRecord:
		return encoder.writeRecord(v)
	case avroschema.AvroTypeEnum:
		return encoder.writeEnum(v.(string))
	case avroschema.AvroTypeArray:
		return encoder.writeArray(v)
	case avroschema.AvroTypeMap:
		return encoder.writeMap(v)
	case avroschema.AvroTypeFixed:
		return encoder.writeFixed(v.([]byte))
	case avroschema.AvroTypeUnion:
		return encoder.writeUnion(v)
	default:
		return fmt.Errorf("type %s not implemented", encoder.schema.GetType())
	}
}

func WriteBoolean(writer io.Writer, value bool) (err error) {
	encoded := byte(0)
	if value {
		encoded = 1
	}
	_, err = writer.Write([]byte{encoded})
	if err != nil {
		return
	}
	return
}

func WriteInt(writer io.Writer, value int32) (err error) {
	encodedValue := make([]byte, binary.MaxVarintLen32)
	n := binary.PutVarint(encodedValue, int64(value))
	_, err = writer.Write(encodedValue[:n])
	if err != nil {
		return
	}
	return
}

func WriteLong(writer io.Writer, value int64) (err error) {
	encodedValue := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(encodedValue, value)
	_, err = writer.Write(encodedValue[:n])
	if err != nil {
		return
	}
	return
}

func WriteBytes(writer io.Writer, value []byte) (err error) {
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	_, err = writer.Write(value)
	if err != nil {
		return
	}
	return
}

func WriteString(writer io.Writer, value string) (err error) {
	err = WriteBytes(writer, []byte(value))
	return
}

func (encoder *Encoder) writeRecord(v interface{}) (err error) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("expected a struct, got %T", v)
	}
	typ := reflect.TypeOf(v)
	record := encoder.schema.(*avroschema.Record)
	for _, recordField := range record.Fields {
		found := false
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := field.Tag.Get("avro")
			if tag == "" {
				continue
			}
			if tag != recordField.Name {
				continue
			}
			found = true
			switch recordField.Type.GetType() {
			case avroschema.AvroTypeBoolean:
				err = WriteBoolean(encoder.writer, val.Field(i).Bool())
			case avroschema.AvroTypeInt:
				err = WriteInt(encoder.writer, int32(val.Field(i).Int()))
			case avroschema.AvroTypeLong:
				err = WriteLong(encoder.writer, val.Field(i).Int())
			case avroschema.AvroTypeFloat:
				err = errors.New("float not implemented")
			case avroschema.AvroTypeDouble:
				err = errors.New("double not implemented")
			case avroschema.AvroTypeBytes:
				err = WriteBytes(encoder.writer, val.Field(i).Bytes())
			case avroschema.AvroTypeString:
				err = WriteString(encoder.writer, val.Field(i).String())
			case avroschema.AvroTypeRecord:
				err = encoder.writeRecord(val.Field(i).Interface())
			default:
				err = fmt.Errorf("type %s not implemented", recordField.Type.GetType())
			}
			if err != nil {
				return
			}
		}
		if !found {
			return fmt.Errorf("field %s not found", recordField.Name)
		}
	}
	return
}

func (encoder *Encoder) writeEnum(value string) (err error) {
	enum := encoder.schema.(*avroschema.Enum)
	for i, symbol := range enum.Symbols {
		if symbol != value {
			continue
		}
		err = WriteInt(encoder.writer, int32(i))
		if err != nil {
			return
		}
		return
	}
	err = fmt.Errorf("symbol %s not found", value)
	return
}

func (encoder *Encoder) writeArray(v interface{}) (err error) {
	avroArray := encoder.schema.(*avroschema.Array)
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("expected a slice, got %T", v)
	}
	err = WriteLong(encoder.writer, int64(val.Len()))
	if err != nil {
		return
	}
	for i := 0; i < val.Len(); i++ {
		switch avroArray.Items.GetType() {
		case avroschema.AvroTypeBoolean:
			err = WriteBoolean(encoder.writer, val.Index(i).Bool())
		case avroschema.AvroTypeInt:
			err = WriteInt(encoder.writer, int32(val.Index(i).Int()))
		case avroschema.AvroTypeLong:
			err = WriteLong(encoder.writer, val.Index(i).Int())
		case avroschema.AvroTypeFloat:
			err = errors.New("float not implemented")
		case avroschema.AvroTypeDouble:
			err = errors.New("double not implemented")
		case avroschema.AvroTypeBytes:
			err = WriteBytes(encoder.writer, val.Index(i).Bytes())
		case avroschema.AvroTypeString:
			err = WriteString(encoder.writer, val.Index(i).String())
		case avroschema.AvroTypeRecord:
			err = errors.New("record not implemented")
		case avroschema.AvroTypeEnum:
			err = errors.New("enum not implemented")
		default:
			err = fmt.Errorf("type %s not implemented", avroArray.Items.GetType())
		}
		if err != nil {
			return
		}
	}
	_, err = encoder.writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func (encoder *Encoder) writeMap(v interface{}) (err error) {
	avroMap := encoder.schema.(*avroschema.Map)
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Map {
		return fmt.Errorf("expected a map, got %T", v)
	}
	err = WriteLong(encoder.writer, int64(val.Len()))
	if err != nil {
		return
	}
	for _, keyVal := range val.MapKeys() {
		key := keyVal.String()
		err = WriteString(encoder.writer, key)
		if err != nil {
			return
		}
		switch avroMap.Values.GetType() {
		case avroschema.AvroTypeBoolean:
			err = WriteBoolean(encoder.writer, val.MapIndex(keyVal).Bool())
		case avroschema.AvroTypeInt:
			err = WriteInt(encoder.writer, int32(val.MapIndex(keyVal).Int()))
		case avroschema.AvroTypeLong:
			err = WriteLong(encoder.writer, val.MapIndex(keyVal).Int())
		case avroschema.AvroTypeFloat:
			err = errors.New("float not implemented")
		case avroschema.AvroTypeDouble:
			err = errors.New("double not implemented")
		case avroschema.AvroTypeBytes:
			err = WriteBytes(encoder.writer, val.MapIndex(keyVal).Bytes())
		case avroschema.AvroTypeString:
			err = WriteString(encoder.writer, val.MapIndex(keyVal).String())
		case avroschema.AvroTypeRecord:
			err = errors.New("record not implemented")
		case avroschema.AvroTypeEnum:
			err = errors.New("enum not implemented")
		default:
			err = fmt.Errorf("type %s not implemented", avroMap.Values.GetType())
		}
		if err != nil {
			return
		}
	}
	_, err = encoder.writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func (encoder *Encoder) writeFixed(value []byte) (err error) {
	fixed := encoder.schema.(*avroschema.Fixed)
	if len(value) != fixed.Size {
		return fmt.Errorf("expected %d bytes, got %d", fixed.Size, len(value))
	}
	_, err = encoder.writer.Write(value)
	if err != nil {
		return
	}
	return
}

func (encoder *Encoder) writeUnion(v interface{}) (err error) {
	union := encoder.schema.(avroschema.Union)
	for i, schema := range union {
		switch schema.GetType() {
		case avroschema.AvroTypeNull:
			if v == nil {
				return WriteInt(encoder.writer, int32(i))
			}
		case avroschema.AvroTypeBoolean:
			if value, ok := v.(bool); ok {
				err = WriteInt(encoder.writer, int32(i))
				if err != nil {
					return
				}
				return WriteBoolean(encoder.writer, value)
			}
		case avroschema.AvroTypeInt:
			if value, ok := v.(int32); ok {
				err = WriteInt(encoder.writer, int32(i))
				if err != nil {
					return
				}
				return WriteInt(encoder.writer, value)
			}
		case avroschema.AvroTypeLong:
			if value, ok := v.(int64); ok {
				err = WriteInt(encoder.writer, int32(i))
				if err != nil {
					return
				}
				return WriteLong(encoder.writer, value)
			}
		case avroschema.AvroTypeFloat:
			return errors.New("float not implemented")
		case avroschema.AvroTypeDouble:
			return errors.New("double not implemented")
		case avroschema.AvroTypeBytes:
			if value, ok := v.([]byte); ok {
				err = WriteInt(encoder.writer, int32(i))
				if err != nil {
					return
				}
				return WriteBytes(encoder.writer, value)
			}
		case avroschema.AvroTypeString:
			if value, ok := v.(string); ok {
				err = WriteInt(encoder.writer, int32(i))
				if err != nil {
					return
				}
				return WriteString(encoder.writer, value)
			}
		default:
			return fmt.Errorf("type %s not implemented", schema.GetType())
		}
	}
	return
}
