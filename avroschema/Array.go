package avroschema

import (
	"encoding/json"
	"fmt"
)

type Array struct {
	SchemaBase
	Items   Schema        `json:"items"`
	Default []interface{} `json:"default,omitempty"`
}

func (array *Array) UnmarshalJSON(data []byte) (err error) {
	var base struct {
		SchemaBase
		Items json.RawMessage `json:"items"`
	}
	err = json.Unmarshal(data, &base)
	if err != nil {
		return
	}
	if base.SchemaBase.Type != "array" {
		return fmt.Errorf("expected type 'array', got %s", base.Type)
	}
	items, err := ParseSchema(base.Items)
	if err != nil {
		return
	}
	array.SchemaBase = base.SchemaBase
	array.Items = items
	return
}
