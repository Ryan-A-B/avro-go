package internal

import "github.com/Ryan-A-B/avro-go/avroschema"

func IsOptional(avroUnion avroschema.Union) bool {
	if len(avroUnion) != 2 {
		return false
	}
	if avroUnion[0].GetType() == avroschema.AvroTypeNull {
		return true
	}
	if avroUnion[1].GetType() == avroschema.AvroTypeNull {
		return true
	}
	return false
}
