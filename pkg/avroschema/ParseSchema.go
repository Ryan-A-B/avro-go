package avroschema

import (
	"encoding/json"
	"errors"
	"io"
)

var ErrInvalidSchema = errors.New("invalid schema")

func ReadSchema(reader io.Reader) (schema Schema, err error) {
	var data json.RawMessage
	err = json.NewDecoder(reader).Decode(&data)
	if err != nil {
		return
	}
	schema, err = ParseSchema(data)
	if err != nil {
		return
	}
	return
}

func ParseSchema(data []byte) (schema Schema, err error) {
	switch data[0] {
	case '"':
		return parseAvroType(data)
	case '[':
		return parseUnion(data)
	case '{':
		return parseSchemaObject(data)
	default:
		err = ErrInvalidSchema
		return
	}
}

func parseAvroType(data []byte) (schema Schema, err error) {
	var avroType AvroType
	err = json.Unmarshal(data, &avroType)
	if err != nil {
		return
	}
	schema = avroType
	return
}

func parseUnion(data []byte) (schema Schema, err error) {
	var union Union
	err = json.Unmarshal(data, &union)
	if err != nil {
		return
	}
	schema = union
	return
}

func parseSchemaObject(data []byte) (schema Schema, err error) {
	var schemaBase SchemaBase
	err = json.Unmarshal(data, &schemaBase)
	if err != nil {
		return
	}
	parse, ok := schemaParsers[schemaBase.Type]
	if !ok {
		err = ErrInvalidSchema
		return
	}
	if parse == nil {
		schema = schemaBase
		return
	}
	return parse(data)
}

type parseSchemaFunc func(data []byte) (Schema, error)

var schemaParsers = map[AvroType]parseSchemaFunc{
	AvroTypeNull:    nil,
	AvroTypeBoolean: nil,
	AvroTypeInt:     nil,
	AvroTypeLong:    nil,
	AvroTypeFloat:   nil,
	AvroTypeDouble:  nil,
	AvroTypeBytes:   nil,
	AvroTypeString:  nil,
	AvroTypeRecord:  parseRecord,
	AvroTypeEnum:    parseEnum,
	AvroTypeArray:   parseArray,
	AvroTypeMap:     parseMap,
	AvroTypeFixed:   parseFixed,
}

func parseRecord(data []byte) (schema Schema, err error) {
	record := new(Record)
	err = json.Unmarshal(data, record)
	if err != nil {
		return
	}
	schema = record
	return
}

func parseEnum(data []byte) (schema Schema, err error) {
	enum := new(Enum)
	err = json.Unmarshal(data, enum)
	if err != nil {
		return
	}
	schema = enum
	return
}

func parseArray(data []byte) (schema Schema, err error) {
	array := new(Array)
	err = json.Unmarshal(data, array)
	if err != nil {
		return
	}
	schema = array
	return
}

func parseMap(data []byte) (schema Schema, err error) {
	avroMap := new(Map)
	err = json.Unmarshal(data, avroMap)
	if err != nil {
		return
	}
	schema = avroMap
	return
}

func parseFixed(data []byte) (schema Schema, err error) {
	fixed := new(Fixed)
	err = json.Unmarshal(data, fixed)
	if err != nil {
		return
	}
	schema = fixed
	return
}
