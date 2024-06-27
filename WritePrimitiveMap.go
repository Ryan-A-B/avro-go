package avro

import (
	"io"

	"tps-git.topcon.com/cloud/avro/avroschema"
)

var encodeMapByType = map[avroschema.AvroType]EncodeFunc{
	avroschema.AvroTypeBoolean: WriteBooleanMap,
	avroschema.AvroTypeInt:     WriteIntMap,
	avroschema.AvroTypeLong:    WriteLongMap,
	avroschema.AvroTypeFloat:   WriteFloatMap,
	avroschema.AvroTypeDouble:  WriteDoubleMap,
	avroschema.AvroTypeBytes:   WriteBytesMap,
	avroschema.AvroTypeString:  WriteStringMap,
}

func WriteBooleanMap(writer io.Writer, v interface{}) (err error) {
	value := v.(map[string]bool)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for k, v := range value {
		err = WriteString(writer, k)
		if err != nil {
			return
		}
		err = WriteBoolean(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteIntMap(writer io.Writer, v interface{}) (err error) {
	value := v.(map[string]int32)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for k, v := range value {
		err = WriteString(writer, k)
		if err != nil {
			return
		}
		err = WriteInt(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteLongMap(writer io.Writer, v interface{}) (err error) {
	value := v.(map[string]int64)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for k, v := range value {
		err = WriteString(writer, k)
		if err != nil {
			return
		}
		err = WriteLong(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteFloatMap(writer io.Writer, v interface{}) (err error) {
	value := v.(map[string]float32)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for k, v := range value {
		err = WriteString(writer, k)
		if err != nil {
			return
		}
		err = WriteFloat(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteDoubleMap(writer io.Writer, v interface{}) (err error) {
	value := v.(map[string]float64)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for k, v := range value {
		err = WriteString(writer, k)
		if err != nil {
			return
		}
		err = WriteDouble(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteBytesMap(writer io.Writer, v interface{}) (err error) {
	value := v.(map[string][]byte)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for k, v := range value {
		err = WriteString(writer, k)
		if err != nil {
			return
		}
		err = WriteBytes(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}

func WriteStringMap(writer io.Writer, v interface{}) (err error) {
	value := v.(map[string]string)
	err = WriteLong(writer, int64(len(value)))
	if err != nil {
		return
	}
	for k, v := range value {
		err = WriteString(writer, k)
		if err != nil {
			return
		}
		err = WriteString(writer, v)
		if err != nil {
			return
		}
	}
	_, err = writer.Write([]byte{0})
	if err != nil {
		return
	}
	return
}
