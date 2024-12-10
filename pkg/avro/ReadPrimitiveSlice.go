package avro

func ReadBooleanSlice(reader Reader, values *[]bool) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make([]bool, length)
	for length > 0 {
		for i := 0; i < int(length); i++ {
			var value bool
			err = ReadBoolean(reader, &value)
			if err != nil {
				return
			}
			(*values)[i] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	return
}

func ReadIntSlice(reader Reader, values *[]int32) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make([]int32, length)
	for length > 0 {
		for i := 0; i < int(length); i++ {
			var value int32
			err = ReadInt(reader, &value)
			if err != nil {
				return
			}
			(*values)[i] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	return
}

func ReadLongSlice(reader Reader, values *[]int64) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make([]int64, length)
	for length > 0 {
		for i := 0; i < int(length); i++ {
			var value int64
			err = ReadLong(reader, &value)
			if err != nil {
				return
			}
			(*values)[i] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	return
}

func ReadFloatSlice(reader Reader, values *[]float32) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make([]float32, length)
	for length > 0 {
		for i := 0; i < int(length); i++ {
			var value float32
			err = ReadFloat(reader, &value)
			if err != nil {
				return
			}
			(*values)[i] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	return
}

func ReadDoubleSlice(reader Reader, values *[]float64) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make([]float64, length)
	for length > 0 {
		for i := 0; i < int(length); i++ {
			var value float64
			err = ReadDouble(reader, &value)
			if err != nil {
				return
			}
			(*values)[i] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	return
}

func ReadBytesSlice(reader Reader, values *[][]byte) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make([][]byte, length)
	for length > 0 {
		for i := 0; i < int(length); i++ {
			var value []byte
			value, err = ReadBytes(reader)
			if err != nil {
				return
			}
			(*values)[i] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	return
}

func ReadStringSlice(reader Reader, values *[]string) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	*values = make([]string, length)
	for length > 0 {
		for i := 0; i < int(length); i++ {
			var value string
			value, err = ReadString(reader)
			if err != nil {
				return
			}
			(*values)[i] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	return
}
