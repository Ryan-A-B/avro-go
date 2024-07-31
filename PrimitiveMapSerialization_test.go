package avro_test

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/Ryan-A-B/avro-go"
	. "github.com/smartystreets/goconvey/convey"
)

func TestMapSerialization(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	Convey("TestMapSerialization", t, func() {
		var buffer bytes.Buffer
		var err error
		Convey("Boolean", func() {
			expectedValue := map[string]bool{
				"x": rand.Intn(2) == 1,
				"y": rand.Intn(2) == 1,
				"z": rand.Intn(2) == 1,
				"a": rand.Intn(2) == 1,
			}
			_, err = avro.WriteBooleanMap(&buffer, expectedValue)
			So(err, ShouldBeNil)
			var actualValue map[string]bool
			err = avro.ReadBooleanMap(&buffer, &actualValue)
			So(err, ShouldBeNil)
			So(actualValue, ShouldResemble, expectedValue)
			So(buffer.Len(), ShouldEqual, 0)
		})
		Convey("Int", func() {
			expectedValue := map[string]int32{
				"x": rand.Int31(),
				"y": rand.Int31(),
				"z": rand.Int31(),
				"a": rand.Int31(),
			}
			_, err = avro.WriteIntMap(&buffer, expectedValue)
			So(err, ShouldBeNil)
			var actualValue map[string]int32
			err = avro.ReadIntMap(&buffer, &actualValue)
			So(err, ShouldBeNil)
			So(actualValue, ShouldResemble, expectedValue)
			So(buffer.Len(), ShouldEqual, 0)
		})
		Convey("Long", func() {
			expectedValue := map[string]int64{
				"x": rand.Int63(),
				"y": rand.Int63(),
				"z": rand.Int63(),
				"a": rand.Int63(),
			}
			_, err = avro.WriteLongMap(&buffer, expectedValue)
			So(err, ShouldBeNil)
			var actualValue map[string]int64
			err = avro.ReadLongMap(&buffer, &actualValue)
			So(err, ShouldBeNil)
			So(actualValue, ShouldResemble, expectedValue)
			So(buffer.Len(), ShouldEqual, 0)
		})
		Convey("Float", func() {
			expectedValue := map[string]float32{
				"x": rand.Float32(),
				"y": rand.Float32(),
				"z": rand.Float32(),
				"a": rand.Float32(),
			}
			_, err = avro.WriteFloatMap(&buffer, expectedValue)
			So(err, ShouldBeNil)
			var actualValue map[string]float32
			err = avro.ReadFloatMap(&buffer, &actualValue)
			So(err, ShouldBeNil)
			So(actualValue, ShouldResemble, expectedValue)
			So(buffer.Len(), ShouldEqual, 0)
		})
		Convey("Double", func() {
			expectedValue := map[string]float64{
				"x": rand.Float64(),
				"y": rand.Float64(),
				"z": rand.Float64(),
				"a": rand.Float64(),
			}
			_, err = avro.WriteDoubleMap(&buffer, expectedValue)
			So(err, ShouldBeNil)
			var actualValue map[string]float64
			err = avro.ReadDoubleMap(&buffer, &actualValue)
			So(err, ShouldBeNil)
			So(actualValue, ShouldResemble, expectedValue)
			So(buffer.Len(), ShouldEqual, 0)
		})
		Convey("Bytes", func() {
			expectedValue := map[string][]byte{
				"x": []byte("x"),
				"y": []byte("y"),
				"z": []byte("z"),
				"a": []byte("a"),
			}
			_, err = avro.WriteBytesMap(&buffer, expectedValue)
			So(err, ShouldBeNil)
			var actualValue map[string][]byte
			err = avro.ReadBytesMap(&buffer, &actualValue)
			So(err, ShouldBeNil)
			So(actualValue, ShouldResemble, expectedValue)
			So(buffer.Len(), ShouldEqual, 0)
		})
		Convey("String", func() {
			Convey("nil", func() {
				_, err = avro.WriteStringMap(&buffer, nil)
				So(err, ShouldBeNil)
				var actualValue map[string]string
				err = avro.ReadStringMap(&buffer, &actualValue)
				So(err, ShouldBeNil)
				So(actualValue, ShouldHaveLength, 0)
				So(buffer.Len(), ShouldEqual, 0)
			})
			Convey("empty", func() {
				expectedValue := map[string]string{}
				_, err = avro.WriteStringMap(&buffer, expectedValue)
				So(err, ShouldBeNil)
				var actualValue map[string]string
				err = avro.ReadStringMap(&buffer, &actualValue)
				So(err, ShouldBeNil)
				So(actualValue, ShouldResemble, expectedValue)
				So(buffer.Len(), ShouldEqual, 0)
			})
			Convey("with data", func() {
				expectedValue := map[string]string{
					"x": "x",
					"y": "y",
					"z": "z",
					"a": "a",
				}
				_, err = avro.WriteStringMap(&buffer, expectedValue)
				So(err, ShouldBeNil)
				var actualValue map[string]string
				err = avro.ReadStringMap(&buffer, &actualValue)
				So(err, ShouldBeNil)
				So(actualValue, ShouldResemble, expectedValue)
				So(buffer.Len(), ShouldEqual, 0)
			})
		})
	})
}
