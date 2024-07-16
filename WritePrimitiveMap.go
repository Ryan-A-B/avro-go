package avro

import "io"

func WriteBooleanMap(writer Writer, value map[string]bool) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for key, value := range value {
		_, err = WriteString(writer, key)
		if err != nil {
			return
		}
		err = WriteBoolean(writer, value)
		if err != nil {
			return
		}
	}
	_, err = WriteLong(writer, 0)
	if err != nil {
		return
	}
	return
}

func WriteIntMap(writer io.Writer, value map[string]int32) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for key, value := range value {
		_, err = WriteString(writer, key)
		if err != nil {
			return
		}
		err = WriteInt(writer, value)
		if err != nil {
			return
		}
	}
	_, err = WriteLong(writer, 0)
	if err != nil {
		return
	}
	return
}

func WriteLongMap(writer io.Writer, value map[string]int64) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for key, value := range value {
		_, err = WriteString(writer, key)
		if err != nil {
			return
		}
		_, err = WriteLong(writer, value)
		if err != nil {
			return
		}
	}
	_, err = WriteLong(writer, 0)
	if err != nil {
		return
	}
	return
}

func WriteFloatMap(writer io.Writer, value map[string]float32) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for key, value := range value {
		_, err = WriteString(writer, key)
		if err != nil {
			return
		}
		err = WriteFloat(writer, value)
		if err != nil {
			return
		}
	}
	_, err = WriteLong(writer, 0)
	if err != nil {
		return
	}
	return
}

func WriteDoubleMap(writer io.Writer, value map[string]float64) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for key, value := range value {
		_, err = WriteString(writer, key)
		if err != nil {
			return
		}
		err = WriteDouble(writer, value)
		if err != nil {
			return
		}
	}
	_, err = WriteLong(writer, 0)
	if err != nil {
		return
	}
	return
}

func WriteBytesMap(writer io.Writer, value map[string][]byte) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for key, value := range value {
		_, err = WriteString(writer, key)
		if err != nil {
			return
		}
		_, err = WriteBytes(writer, value)
		if err != nil {
			return
		}
	}
	_, err = WriteLong(writer, 0)
	if err != nil {
		return
	}
	return
}

func WriteStringMap(writer io.Writer, value map[string]string) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for key, value := range value {
		_, err = WriteString(writer, key)
		if err != nil {
			return
		}
		_, err = WriteString(writer, value)
		if err != nil {
			return
		}
	}
	_, err = WriteLong(writer, 0)
	if err != nil {
		return
	}
	return
}
