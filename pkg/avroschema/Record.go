package avroschema

import "encoding/json"

type Record struct {
	SchemaBase
	NamedType
	Fields []*RecordField `json:"fields"`
}

type RecordField struct {
	Name    string      `json:"name"`
	Type    Schema      `json:"type"`
	Default interface{} `json:"default"`
}

func (field *RecordField) UnmarshalJSON(data []byte) (err error) {
	var base struct {
		Name string          `json:"name"`
		Type json.RawMessage `json:"type"`
	}
	err = json.Unmarshal(data, &base)
	if err != nil {
		return
	}
	fieldType, err := ParseSchema(base.Type)
	if err != nil {
		return
	}
	field.Name = base.Name
	field.Type = fieldType
	return
}
