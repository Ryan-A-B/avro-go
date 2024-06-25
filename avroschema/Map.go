package avroschema

import (
	"encoding/json"
	"fmt"
)

type Map struct {
	SchemaBase
	Values  Schema                 `json:"values"`
	Default map[string]interface{} `json:"default,omitempty"`
}

func (avroMap *Map) UnmarshalJSON(data []byte) (err error) {
	var base struct {
		SchemaBase
		Values json.RawMessage `json:"values"`
	}
	err = json.Unmarshal(data, &base)
	if err != nil {
		return
	}
	if base.SchemaBase.Type != "map" {
		return fmt.Errorf("expected type 'map', got %s", base.Type)
	}
	values, err := ParseSchema(base.Values)
	if err != nil {
		return
	}
	avroMap.SchemaBase = base.SchemaBase
	avroMap.Values = values
	return
}
