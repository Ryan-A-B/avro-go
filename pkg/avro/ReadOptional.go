package avro

func readOptionalFlag(reader Reader) (bool, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return false, err
	}
	return b == 1, nil
}

func ReadOptionalBoolean(reader Reader, value **bool) error {
	var err error
	var present bool
	present, err = readOptionalFlag(reader)
	if err != nil {
		return err
	}
	if !present {
		value = nil
		return nil
	}
	*value = new(bool)
	return ReadBoolean(reader, *value)
}

func ReadOptionalDouble(reader Reader, value **float64) error {
	var err error
	var present bool
	present, err = readOptionalFlag(reader)
	if err != nil {
		return err
	}
	if !present {
		value = nil
		return nil
	}
	*value = new(float64)
	return ReadDouble(reader, *value)
}

func ReadOptional(reader Reader, read func() error) error {
	var err error
	var present bool
	present, err = readOptionalFlag(reader)
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	return read()
}
