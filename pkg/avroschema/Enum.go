package avroschema

type Enum struct {
	SchemaBase
	NamedType
	Symbols []string `json:"symbols"`
	Default string   `json:"default,omitempty"`
}
