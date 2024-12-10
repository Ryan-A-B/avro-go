package avro

type WriteItemFunc func(i int) (int, error)

func WriteArray(writer Writer, length int, writeItem WriteItemFunc) (int, error) {
	var nTotal int
	var err error
	var n int
	n, err = WriteLong(writer, int64(length))
	nTotal += n
	if err != nil {
		return nTotal, err
	}
	for i := 0; i < length; i++ {
		n, err = writeItem(i)
		nTotal += n
		if err != nil {
			return nTotal, err
		}
	}
	n, err = WriteLong(writer, 0)
	nTotal += n
	return nTotal, err
}
