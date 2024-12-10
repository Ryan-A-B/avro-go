package avro

func ReadDoubleArray(reader Reader, values []float64) (err error) {
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return
	}
	totalLength := 0
	for length > 0 {
		totalLength += int(length)
		if totalLength > len(values) {
			panic("array length exceeds slice length")
		}
		for i := 0; i < int(length); i++ {
			var value float64
			err = ReadDouble(reader, &value)
			if err != nil {
				return
			}
			values[i] = value
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	if totalLength != len(values) {
		panic("array length does not match slice length")
	}
	return
}
