package avro

import (
	"encoding/binary"
	"io"
)

func WriteBoolean(writer io.Writer, value bool) (err error) {
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

func WriteInt(writer io.Writer, value int32) (err error) {
	encodedValue := make([]byte, binary.MaxVarintLen32)
	n := binary.PutVarint(encodedValue, int64(value))
	_, err = writer.Write(encodedValue[:n])
	if err != nil {
		return
	}
	return
}

func WriteLong(writer io.Writer, value int64) (err error) {
	encodedValue := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(encodedValue, value)
	_, err = writer.Write(encodedValue[:n])
	if err != nil {
		return
	}
	return
}

func WriteBytes(writer io.Writer, value []byte) (err error) {
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

func WriteString(writer io.Writer, value string) (err error) {
	err = WriteBytes(writer, []byte(value))
	return
}
