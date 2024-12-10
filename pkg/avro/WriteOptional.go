package avro

import (
	"reflect"
)

// Should be called Writer but already have a Writer for the writer union
type Marshaler interface {
	WriteAvro(w Writer) (int, error)
}

func writeOptionalFlag(writer Writer, present bool) error {
	var err error
	if present {
		err = writer.WriteByte(1)
	} else {
		err = writer.WriteByte(0)
	}
	return err
}

func WriteOptionalBoolean(writer Writer, value *bool) (int, error) {
	var err error
	err = writeOptionalFlag(writer, value != nil)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 1, nil
	}
	err = WriteBoolean(writer, *value)
	if err != nil {
		return 1, err
	}
	return 2, nil
}

func WriteOptionalDouble(writer Writer, value *float64) (int, error) {
	var err error
	var n int
	err = writeOptionalFlag(writer, value != nil)
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 1, nil
	}
	n, err = WriteDouble(writer, *value)
	return n + 1, err
}

func WriteOptional(writer Writer, value Marshaler) (int, error) {
	var err error
	var n int
	isNil := isNil(value)
	err = writeOptionalFlag(writer, !isNil)
	if err != nil {
		return 0, err
	}
	if isNil {
		return 1, nil
	}
	n, err = value.WriteAvro(writer)
	return n + 1, err
}

func isNil(value interface{}) bool {
	if value == nil {
		return true
	}
	return reflect.ValueOf(value).IsNil()
}
