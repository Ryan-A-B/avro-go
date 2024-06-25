package avroschema

type AvroType string

const (
	AvroTypeNull    AvroType = "null"
	AvroTypeBoolean AvroType = "boolean"
	AvroTypeInt     AvroType = "int"
	AvroTypeLong    AvroType = "long"
	AvroTypeFloat   AvroType = "float"
	AvroTypeDouble  AvroType = "double"
	AvroTypeBytes   AvroType = "bytes"
	AvroTypeString  AvroType = "string"
	AvroTypeRecord  AvroType = "record"
	AvroTypeEnum    AvroType = "enum"
	AvroTypeArray   AvroType = "array"
	AvroTypeMap     AvroType = "map"
	AvroTypeFixed   AvroType = "fixed"

	// AvroTypeUnion is a special type that is not part of the Avro specification.
	AvroTypeUnion AvroType = "union"
)

func (avroType AvroType) GetType() AvroType {
	return avroType
}
