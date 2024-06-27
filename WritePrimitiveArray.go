package avro

import (
	"io"

	"tps-git.topcon.com/cloud/avro/avroschema"
)

var encodeArrayByType = map[avroschema.AvroType]EncodeFunc{
	avroschema.AvroTypeBoolean: WriteBooleanArray,
	avroschema.AvroTypeInt:     WriteIntArray,
	avroschema.AvroTypeLong:    WriteLongArray,
	// float
	// double
	avroschema.AvroTypeBytes:  WriteBytesArray,
	avroschema.AvroTypeString: WriteStringArray,
}

func WriteBooleanArray(writer io.Writer, v interface{}) (err error) {
	value := v.([]bool)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, v := range value {
		err = WriteBoolean(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteIntArray(writer io.Writer, v interface{}) (err error) {
	value := v.([]int32)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, v := range value {
		err = WriteInt(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteLongArray(writer io.Writer, v interface{}) (err error) {
	value := v.([]int64)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, v := range value {
		err = WriteLong(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

// float
// double

func WriteBytesArray(writer io.Writer, v interface{}) (err error) {
	value := v.([][]byte)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, v := range value {
		err = WriteBytes(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteStringArray(writer io.Writer, v interface{}) (err error) {
	value := v.([]string)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, v := range value {
		err = WriteString(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}
