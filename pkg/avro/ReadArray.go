package avro

type ReadItemFunc func(int) error

func ReadArray(reader Reader, readItem ReadItemFunc) error {
	var err error
	var length int64
	err = ReadLong(reader, &length)
	if err != nil {
		return err
	}
	for length > 0 {
		for i := 0; i < int(length); i++ {
			err = readItem(i)
			if err != nil {
				return err
			}
		}
		err = ReadLong(reader, &length)
		if err != nil {
			return err
		}
	}
	return nil
}
