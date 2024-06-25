package avroschema

type Fixed struct {
	SchemaBase
	NamedType
	Size int `json:"size"`
}
