package avro

import (
	"encoding/binary"
	"io"
)

func WriteNull(writer io.Writer, v interface{}) (err error) {
	return
}

func WriteBoolean(writer io.Writer, v interface{}) (err error) {
	value := v.(bool)
	encoded := byte(0)
	if value {
		encoded = 1
	}
	_, err = writer.Write([]byte{encoded})
	if err != nil {
		return
	}
	return
}

func WriteInt(writer io.Writer, v interface{}) (err error) {
	value := v.(int32)
	encodedValue := make([]byte, binary.MaxVarintLen32)
	n := binary.PutVarint(encodedValue, int64(value))
	_, err = writer.Write(encodedValue[:n])
	if err != nil {
		return
	}
	return
}

func WriteLong(writer io.Writer, v interface{}) (err error) {
	value := v.(int64)
	encodedValue := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(encodedValue, value)
	_, err = writer.Write(encodedValue[:n])
	if err != nil {
		return
	}
	return
}

func WriteFloat(writer io.Writer, v interface{}) (err error) {
	value := v.(float32)
	return binary.Write(writer, binary.LittleEndian, value)
}

func WriteDouble(writer io.Writer, v interface{}) (err error) {
	value := v.(float64)
	return binary.Write(writer, binary.LittleEndian, value)
}

func WriteBytes(writer io.Writer, v interface{}) (err error) {
	value := v.([]byte)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	_, err = writer.Write(value)
	if err != nil {
		return
	}
	return
}

func WriteString(writer io.Writer, v interface{}) (err error) {
	value := v.(string)
	err = WriteBytes(writer, []byte(value))
	return
}
