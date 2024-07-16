package avro

import "io"

func WriteBooleanArray(writer Writer, value []bool) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, element := range value {
		err = WriteBoolean(writer, element)
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

func WriteIntArray(writer io.Writer, value []int32) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, element := range value {
		err = WriteInt(writer, element)
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

func WriteLongArray(writer io.Writer, value []int64) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, element := range value {
		_, err = WriteLong(writer, element)
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

func WriteFloatArray(writer io.Writer, value []float32) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, element := range value {
		err = WriteFloat(writer, element)
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

func WriteDoubleArray(writer io.Writer, value []float64) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, element := range value {
		err = WriteDouble(writer, element)
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

func WriteBytesArray(writer io.Writer, value [][]byte) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, element := range value {
		_, err = WriteBytes(writer, element)
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

func WriteStringArray(writer io.Writer, value []string) (err error) {
	_, err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for _, element := range value {
		_, err = WriteString(writer, element)
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
