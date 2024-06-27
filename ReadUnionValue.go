package avro

import (
	"errors"
	"reflect"
)

func ReadUnionNull(reader Reader, v interface{}) (err error) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		err = errors.New("pointer expected")
		return
	}
	val = val.Elem()
	val.Set(reflect.Zero(val.Type()))
	return
}

var (
	ReadUnionBoolean = makeReadUnionValue(reflect.TypeOf(false), ReadBoolean)
	ReadUnionInt     = makeReadUnionValue(reflect.TypeOf(int32(0)), ReadInt)
	ReadUnionLong    = makeReadUnionValue(reflect.TypeOf(int64(0)), ReadLong)
	ReadUnionFloat   = makeReadUnionValue(reflect.TypeOf(float32(0)), ReadFloat)
	ReadUnionDouble  = makeReadUnionValue(reflect.TypeOf(float64(0)), ReadDouble)
	ReadUnionBytes   = makeReadUnionValue(reflect.TypeOf([]byte(nil)), ReadBytes)
	ReadUnionString  = makeReadUnionValue(reflect.TypeOf(""), ReadString)
)

var (
	ReadUnionBooleanArray = makeReadUnionValue(reflect.TypeOf([]bool(nil)), ReadBooleanArray)
	ReadUnionIntArray     = makeReadUnionValue(reflect.TypeOf([]int32(nil)), ReadIntArray)
	ReadUnionLongArray    = makeReadUnionValue(reflect.TypeOf([]int64(nil)), ReadLongArray)
	ReadUnionFloatArray   = makeReadUnionValue(reflect.TypeOf([]float32(nil)), ReadFloatArray)
	ReadUnionDoubleArray  = makeReadUnionValue(reflect.TypeOf([]float64(nil)), ReadDoubleArray)
	ReadUnionBytesArray   = makeReadUnionValue(reflect.TypeOf([][]byte(nil)), ReadBytesArray)
	ReadUnionStringArray  = makeReadUnionValue(reflect.TypeOf([]string(nil)), ReadStringArray)
)

var (
	ReadUnionBooleanMap = makeReadUnionValue(reflect.TypeOf(map[string]bool(nil)), ReadBooleanMap)
	ReadUnionIntMap     = makeReadUnionValue(reflect.TypeOf(map[string]int32(nil)), ReadIntMap)
	ReadUnionLongMap    = makeReadUnionValue(reflect.TypeOf(map[string]int64(nil)), ReadLongMap)
	ReadUnionFloatMap   = makeReadUnionValue(reflect.TypeOf(map[string]float32(nil)), ReadFloatMap)
	ReadUnionDoubleMap  = makeReadUnionValue(reflect.TypeOf(map[string]float64(nil)), ReadDoubleMap)
	ReadUnionBytesMap   = makeReadUnionValue(reflect.TypeOf(map[string][]byte(nil)), ReadBytesMap)
	ReadUnionStringMap  = makeReadUnionValue(reflect.TypeOf(map[string]string(nil)), ReadStringMap)
)

func makeReadUnionValue(t reflect.Type, decode decodeFunc) decodeFunc {
	return func(reader Reader, v interface{}) (err error) {
		value := reflect.New(t).Interface()
		err = decode(reader, value)
		if err != nil {
			return
		}
		reflect.ValueOf(v).Elem().Set(reflect.ValueOf(value).Elem())
		return
	}
}
