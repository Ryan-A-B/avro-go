package avroschema

type Schema interface {
	GetType() AvroType
	// TODO Validate() error
}

type SchemaBase struct {
	Type AvroType `json:"type"`
}

func (schema SchemaBase) GetType() AvroType {
	return schema.Type
}
