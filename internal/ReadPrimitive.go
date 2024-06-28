package internal

import (
	"encoding/binary"
	"io"
)

func ReadBoolean(reader Reader, v interface{}) (err error) {
	value := v.(*bool)
	data := [1]byte{}
	_, err = reader.Read(data[:])
	if err != nil {
		return
	}
	*value = data[0] == 1
	return
}

func ReadInt(reader Reader, v interface{}) (err error) {
	value := v.(*int32)
	x, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	*value = int32(x)
	return
}

func ReadLong(reader Reader, v interface{}) (err error) {
	value := v.(*int64)
	x, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	*value = x
	return
}

func ReadFloat(reader Reader, v interface{}) (err error) {
	value := v.(*float32)
	return binary.Read(reader, binary.LittleEndian, value)
}

func ReadDouble(reader Reader, v interface{}) (err error) {
	value := v.(*float64)
	return binary.Read(reader, binary.LittleEndian, value)
}

func ReadBytes(reader Reader, v interface{}) (err error) {
	value := v.(*[]byte)
	length, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return
	}
	*value = data
	return
}

func ReadString(reader Reader, v interface{}) (err error) {
	value := v.(*string)
	var data []byte
	err = ReadBytes(reader, &data)
	if err != nil {
		return
	}
	*value = string(data)
	return
}
