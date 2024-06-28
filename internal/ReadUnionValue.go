package internal

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
	ReadUnionBoolean = MakeReadUnionValue(reflect.TypeOf(false), ReadBoolean)
	ReadUnionInt     = MakeReadUnionValue(reflect.TypeOf(int32(0)), ReadInt)
	ReadUnionLong    = MakeReadUnionValue(reflect.TypeOf(int64(0)), ReadLong)
	ReadUnionFloat   = MakeReadUnionValue(reflect.TypeOf(float32(0)), ReadFloat)
	ReadUnionDouble  = MakeReadUnionValue(reflect.TypeOf(float64(0)), ReadDouble)
	ReadUnionBytes   = MakeReadUnionValue(reflect.TypeOf([]byte(nil)), ReadBytes)
	ReadUnionString  = MakeReadUnionValue(reflect.TypeOf(""), ReadString)
)

var (
	ReadUnionBooleanArray = MakeReadUnionValue(reflect.TypeOf([]bool(nil)), ReadBooleanArray)
	ReadUnionIntArray     = MakeReadUnionValue(reflect.TypeOf([]int32(nil)), ReadIntArray)
	ReadUnionLongArray    = MakeReadUnionValue(reflect.TypeOf([]int64(nil)), ReadLongArray)
	ReadUnionFloatArray   = MakeReadUnionValue(reflect.TypeOf([]float32(nil)), ReadFloatArray)
	ReadUnionDoubleArray  = MakeReadUnionValue(reflect.TypeOf([]float64(nil)), ReadDoubleArray)
	ReadUnionBytesArray   = MakeReadUnionValue(reflect.TypeOf([][]byte(nil)), ReadBytesArray)
	ReadUnionStringArray  = MakeReadUnionValue(reflect.TypeOf([]string(nil)), ReadStringArray)
)

var (
	ReadUnionBooleanMap = MakeReadUnionValue(reflect.TypeOf(map[string]bool(nil)), ReadBooleanMap)
	ReadUnionIntMap     = MakeReadUnionValue(reflect.TypeOf(map[string]int32(nil)), ReadIntMap)
	ReadUnionLongMap    = MakeReadUnionValue(reflect.TypeOf(map[string]int64(nil)), ReadLongMap)
	ReadUnionFloatMap   = MakeReadUnionValue(reflect.TypeOf(map[string]float32(nil)), ReadFloatMap)
	ReadUnionDoubleMap  = MakeReadUnionValue(reflect.TypeOf(map[string]float64(nil)), ReadDoubleMap)
	ReadUnionBytesMap   = MakeReadUnionValue(reflect.TypeOf(map[string][]byte(nil)), ReadBytesMap)
	ReadUnionStringMap  = MakeReadUnionValue(reflect.TypeOf(map[string]string(nil)), ReadStringMap)
)

func MakeReadUnionValue(t reflect.Type, decode DecodeFunc) DecodeFunc {
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
