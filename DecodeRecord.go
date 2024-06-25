package avro

import (
	"fmt"
	"reflect"

	"tps-git.topcon.com/cloud/avro/avroschema"
)

func getGoTypeForSchema(schema avroschema.Schema) reflect.Type {
	avroType := schema.GetType()
	if fieldType, ok := goTypeByAvroType[avroType]; ok {
		return fieldType
	}
	switch avroType {
	case avroschema.AvroTypeRecord:
		return getGoTypeForRecord(schema.(*avroschema.Record))
	case avroschema.AvroTypeArray:
		return getGoTypeForArray(schema.(*avroschema.Array))
	case avroschema.AvroTypeMap:
		return getGoTypeForMap(schema.(*avroschema.Map))
	default:
		panic(fmt.Errorf("unsupported schema type %v", avroType))
	}
}

func getGoTypeForRecord(record *avroschema.Record) reflect.Type {
	var structFields []reflect.StructField
	for _, field := range record.Fields {
		structFields = append(structFields, reflect.StructField{
			Name: "F_" + field.Name, // go doesn't allow reflect.StructOf to have unexported fields
			Type: getGoTypeForSchema(field.Type),
			Tag:  reflect.StructTag(fmt.Sprintf(`avro:"%s"`, field.Name)),
		})
	}
	return reflect.StructOf(structFields)
}

func getGoTypeForArray(avroMap *avroschema.Array) reflect.Type {
	return reflect.SliceOf(getGoTypeForSchema(avroMap.Items))
}

func getGoTypeForMap(avroMap *avroschema.Map) reflect.Type {
	return reflect.MapOf(reflect.TypeOf(""), getGoTypeForSchema(avroMap.Values))
}

var goTypeByAvroType = map[avroschema.AvroType]reflect.Type{
	avroschema.AvroTypeNull:    reflect.TypeOf(nil),
	avroschema.AvroTypeBoolean: reflect.TypeOf(false),
	avroschema.AvroTypeInt:     reflect.TypeOf(int32(0)),
	avroschema.AvroTypeLong:    reflect.TypeOf(int64(0)),
	avroschema.AvroTypeFloat:   reflect.TypeOf(float32(0)),
	avroschema.AvroTypeDouble:  reflect.TypeOf(float64(0)),
	avroschema.AvroTypeBytes:   reflect.TypeOf([]byte(nil)),
	avroschema.AvroTypeString:  reflect.TypeOf(""),

	avroschema.AvroTypeEnum: reflect.TypeOf(""),
}
