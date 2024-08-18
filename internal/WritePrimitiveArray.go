package internal

import (
	"io"

	"github.com/Ryan-A-B/avro-go/pkg/avroschema"
)

var EncodeArrayByType = map[avroschema.AvroType]EncodeFunc{
	avroschema.AvroTypeBoolean: WriteBooleanArray,
	avroschema.AvroTypeInt:     WriteIntArray,
	avroschema.AvroTypeLong:    WriteLongArray,
	avroschema.AvroTypeFloat:   WriteFloatArray,
	avroschema.AvroTypeDouble:  WriteDoubleArray,
	avroschema.AvroTypeBytes:   WriteBytesArray,
	avroschema.AvroTypeString:  WriteStringArray,
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

func WriteFloatArray(writer io.Writer, v interface{}) (err error) {
	value := v.([]float32)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, v := range value {
		err = WriteFloat(writer, v)
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

func WriteDoubleArray(writer io.Writer, v interface{}) (err error) {
	value := v.([]float64)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, v := range value {
		err = WriteDouble(writer, v)
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
