package avroschema

import "encoding/json"

type Union []Schema

func (union Union) GetType() AvroType {
	return AvroTypeUnion
}

func (union *Union) UnmarshalJSON(data []byte) (err error) {
	var items []json.RawMessage
	err = json.Unmarshal(data, &items)
	if err != nil {
		return
	}
	schemas := make([]Schema, len(items))
	for i, item := range items {
		schemas[i], err = ParseSchema(item)
		if err != nil {
			return
		}
	}
	*union = schemas
	return
}
