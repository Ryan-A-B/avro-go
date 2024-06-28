package internal

import "encoding/binary"

func ReadBooleanArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]bool)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]bool, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value bool
			err = ReadBoolean(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadIntArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]int32)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]int32, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value int32
			err = ReadInt(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadLongArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]int64)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]int64, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value int64
			err = ReadLong(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadFloatArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]float32)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]float32, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value float32
			err = ReadFloat(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadDoubleArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]float64)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]float64, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value float64
			err = ReadDouble(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadBytesArray(reader Reader, v interface{}) (err error) {
	value := v.(*[][]byte)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([][]byte, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value []byte
			err = ReadBytes(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadStringArray(reader Reader, v interface{}) (err error) {
	value := v.(*[]string)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make([]string, 0, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var value string
			err = ReadString(reader, &value)
			if err != nil {
				return
			}
			values = append(values, value)
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}
