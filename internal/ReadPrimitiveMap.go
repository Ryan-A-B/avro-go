package internal

import "encoding/binary"

func ReadBooleanMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]bool)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]bool, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value bool
			err = ReadBoolean(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadIntMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]int32)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]int32, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value int32
			err = ReadInt(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadLongMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]int64)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]int64, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value int64
			err = ReadLong(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadFloatMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]float32)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]float32, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value float32
			err = ReadFloat(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadDoubleMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]float64)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]float64, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value float64
			err = ReadDouble(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadBytesMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string][]byte)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string][]byte, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value []byte
			err = ReadBytes(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}

func ReadStringMap(reader Reader, v interface{}) (err error) {
	value := v.(*map[string]string)
	blockLength, err := binary.ReadVarint(reader)
	if err != nil {
		return
	}
	values := make(map[string]string, blockLength)
	for blockLength != 0 {
		for i := int64(0); i < blockLength; i++ {
			var key string
			err = ReadString(reader, &key)
			if err != nil {
				return
			}
			var value string
			err = ReadString(reader, &value)
			if err != nil {
				return
			}
			values[key] = value
		}
		blockLength, err = binary.ReadVarint(reader)
		if err != nil {
			return
		}
	}
	*value = values
	return
}
