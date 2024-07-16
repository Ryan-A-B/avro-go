package avro

import (
	"bytes"
	"encoding/binary"
	"io"
)

func ReadBoolean(reader io.ByteReader, value *bool) (err error) {
	data, err := reader.ReadByte()
	if err != nil {
		return
	}
	*value = data == 1
	return
}

func ReadInt(reader io.ByteReader, value *int32) (err error) {
	x, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	*value = int32(x)
	return
}

func ReadLong(reader io.ByteReader, value *int64) (err error) {
	*value, err = binary.ReadVarint(reader)
	if err != nil {
		return
	}
	return
}

func ReadFloat(reader io.Reader, value *float32) (err error) {
	err = binary.Read(reader, binary.LittleEndian, value)
	if err != nil {
		return
	}
	return
}

func ReadDouble(reader io.Reader, value *float64) (err error) {
	err = binary.Read(reader, binary.LittleEndian, value)
	if err != nil {
		return
	}
	return
}

func ReadBytes(reader Reader) (value []byte, err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	value = make([]byte, length)
	_, err = io.ReadFull(reader, value)
	if err != nil {
		return
	}
	return
}

func ReadString(reader Reader) (value string, err error) {
	data, err := ReadBytes(reader)
	if err != nil {
		return
	}
	value = string(data)
	return
}

func ReadBytesIntoBuffer(reader Reader, buffer *bytes.Buffer) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	buffer.Grow(int(length))
	_, err = io.CopyN(buffer, reader, length)
	if err != nil {
		return
	}
	return
}
