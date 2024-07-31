package avro

import (
	"encoding/binary"
	"io"
)

func WriteBoolean(writer io.ByteWriter, value bool) (err error) {
	if value {
		return writer.WriteByte(1)
	}
	return writer.WriteByte(0)
}

func WriteInt(writer io.Writer, value int32) (n int, err error) {
	data := make([]byte, binary.MaxVarintLen32)
	n = binary.PutVarint(data, int64(value))
	_, err = writer.Write(data[:n])
	if err != nil {
		return
	}
	return
}

func WriteLong(writer io.Writer, value int64) (n int, err error) {
	data := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(data, value)
	_, err = writer.Write(data[:n])
	if err != nil {
		return
	}
	return
}

func WriteFloat(writer io.Writer, value float32) (n int, err error) {
	return 4, binary.Write(writer, binary.LittleEndian, value)
}

func WriteDouble(writer io.Writer, value float64) (n int, err error) {
	return 8, binary.Write(writer, binary.LittleEndian, value)
}

func WriteBytes(writer io.Writer, value []byte) (nWritten int, err error) {
	n, err := WriteLong(writer, int64(len(value)))
	nWritten += n
	if err != nil {
		return
	}
	n, err = writer.Write(value)
	nWritten += n
	if err != nil {
		return
	}
	return
}

func WriteString(writer io.Writer, value string) (n int, err error) {
	return WriteBytes(writer, []byte(value))
}
