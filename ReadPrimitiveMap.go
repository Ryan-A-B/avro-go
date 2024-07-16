package avro

func ReadBooleanMap(reader Reader, values *map[string]bool) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make(map[string]bool, length)
	for length > 0 {
		for i := int64(0); i < length; i++ {
			var key string
			var value bool
			key, err = ReadString(reader)
			if err != nil {
				return
			}
			err = ReadBoolean(reader, &value)
			if err != nil {
				return
			}
			(*values)[key] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return
		}
	}
	return
}

func ReadIntMap(reader Reader, values *map[string]int32) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make(map[string]int32, length)
	for length > 0 {
		for i := int64(0); i < length; i++ {
			var key string
			var value int32
			key, err = ReadString(reader)
			if err != nil {
				return
			}
			err = ReadInt(reader, &value)
			if err != nil {
				return
			}
			(*values)[key] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return
		}
	}
	return
}

func ReadLongMap(reader Reader, values *map[string]int64) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make(map[string]int64, length)
	for length > 0 {
		for i := int64(0); i < length; i++ {
			var key string
			var value int64
			key, err = ReadString(reader)
			if err != nil {
				return
			}
			err = ReadLong(reader, &value)
			if err != nil {
				return
			}
			(*values)[key] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return
		}
	}
	return
}

func ReadFloatMap(reader Reader, values *map[string]float32) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make(map[string]float32, length)
	for length > 0 {
		for i := int64(0); i < length; i++ {
			var key string
			var value float32
			key, err = ReadString(reader)
			if err != nil {
				return
			}
			err = ReadFloat(reader, &value)
			if err != nil {
				return
			}
			(*values)[key] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return
		}
	}
	return
}

func ReadDoubleMap(reader Reader, values *map[string]float64) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make(map[string]float64, length)
	for length > 0 {
		for i := int64(0); i < length; i++ {
			var key string
			var value float64
			key, err = ReadString(reader)
			if err != nil {
				return
			}
			err = ReadDouble(reader, &value)
			if err != nil {
				return
			}
			(*values)[key] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return
		}
	}
	return
}

func ReadBytesMap(reader Reader, values *map[string][]byte) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make(map[string][]byte, length)
	for length > 0 {
		for i := int64(0); i < length; i++ {
			var key string
			var value []byte
			key, err = ReadString(reader)
			if err != nil {
				return
			}
			value, err = ReadBytes(reader)
			if err != nil {
				return
			}
			(*values)[key] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return
		}
	}
	return
}

func ReadStringMap(reader Reader, values *map[string]string) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make(map[string]string, length)
	for length > 0 {
		for i := int64(0); i < length; i++ {
			var key string
			var value string
			key, err = ReadString(reader)
			if err != nil {
				return
			}
			value, err = ReadString(reader)
			if err != nil {
				return
			}
			(*values)[key] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return
		}
	}
	return
}
