package avro

import "io"

func WriteBooleanMap(writer Writer, value map[string]bool) (nTotal int, err error) {
	var n int
	length := int64(len(value))
	n, err = WriteLong(writer, length)
	nTotal += n
	if err != nil {
		return
	}
	if length == 0 {
		return
	}
	for key, value := range value {
		n, err = WriteString(writer, key)
		nTotal += n
		if err != nil {
			return
		}
		err = WriteBoolean(writer, value)
		nTotal += 1
		if err != nil {
			return
		}
	}
	n, err = WriteLong(writer, 0)
	nTotal += n
	if err != nil {
		return
	}
	return
}

func WriteIntMap(writer io.Writer, value map[string]int32) (nTotal int, err error) {
	var n int
	length := int64(len(value))
	n, err = WriteLong(writer, length)
	nTotal += n
	if err != nil {
		return
	}
	if length == 0 {
		return
	}
	for key, value := range value {
		n, err = WriteString(writer, key)
		nTotal += n
		if err != nil {
			return
		}
		n, err = WriteInt(writer, value)
		nTotal += n
		if err != nil {
			return
		}
	}
	n, err = WriteLong(writer, 0)
	nTotal += n
	if err != nil {
		return
	}
	return
}

func WriteLongMap(writer io.Writer, value map[string]int64) (nTotal int, err error) {
	var n int
	length := int64(len(value))
	n, err = WriteLong(writer, length)
	nTotal += n
	if err != nil {
		return
	}
	if length == 0 {
		return
	}
	for key, value := range value {
		n, err = WriteString(writer, key)
		nTotal += n
		if err != nil {
			return
		}
		n, err = WriteLong(writer, value)
		nTotal += n
		if err != nil {
			return
		}
	}
	n, err = WriteLong(writer, 0)
	nTotal += n
	if err != nil {
		return
	}
	return
}

func WriteFloatMap(writer io.Writer, value map[string]float32) (nTotal int, err error) {
	var n int
	length := int64(len(value))
	n, err = WriteLong(writer, length)
	nTotal += n
	if err != nil {
		return
	}
	if length == 0 {
		return
	}
	for key, value := range value {
		n, err = WriteString(writer, key)
		nTotal += n
		if err != nil {
			return
		}
		n, err = WriteFloat(writer, value)
		nTotal += n
		if err != nil {
			return
		}
	}
	n, err = WriteLong(writer, 0)
	nTotal += n
	if err != nil {
		return
	}
	return
}

func WriteDoubleMap(writer io.Writer, value map[string]float64) (nTotal int, err error) {
	var n int
	length := int64(len(value))
	n, err = WriteLong(writer, length)
	nTotal += n
	if err != nil {
		return
	}
	if length == 0 {
		return
	}
	for key, value := range value {
		n, err = WriteString(writer, key)
		nTotal += n
		if err != nil {
			return
		}
		n, err = WriteDouble(writer, value)
		nTotal += n
		if err != nil {
			return
		}
	}
	n, err = WriteLong(writer, 0)
	nTotal += n
	if err != nil {
		return
	}
	return
}

func WriteBytesMap(writer io.Writer, value map[string][]byte) (nTotal int, err error) {
	var n int
	length := int64(len(value))
	n, err = WriteLong(writer, length)
	nTotal += n
	if err != nil {
		return
	}
	if length == 0 {
		return
	}
	for key, value := range value {
		n, err = WriteString(writer, key)
		nTotal += n
		if err != nil {
			return
		}
		n, err = WriteBytes(writer, value)
		nTotal += n
		if err != nil {
			return
		}
	}
	n, err = WriteLong(writer, 0)
	nTotal += n
	if err != nil {
		return
	}
	return
}

func WriteStringMap(writer io.Writer, value map[string]string) (nTotal int, err error) {
	var n int
	length := int64(len(value))
	n, err = WriteLong(writer, length)
	nTotal += n
	if err != nil {
		return
	}
	if length == 0 {
		return
	}
	for key, value := range value {
		n, err = WriteString(writer, key)
		nTotal += n
		if err != nil {
			return
		}
		n, err = WriteString(writer, value)
		nTotal += n
		if err != nil {
			return
		}
	}
	n, err = WriteLong(writer, 0)
	nTotal += n
	if err != nil {
		return
	}
	return
}
