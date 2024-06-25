package avro_test

import (
	"bytes"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"tps-git.topcon.com/cloud/avro"
	"tps-git.topcon.com/cloud/avro/avroschema"
)

func TestEncode(t *testing.T) {
	Convey("TestEncode", t, func() {
		Convey("primitive types", func() {
			Convey("boolean", func() {
				schema := avroschema.AvroTypeBoolean
				var buffer bytes.Buffer
				encoder := avro.NewEncoder(&buffer, schema)
				Convey("false", func() {
					err := encoder.Encode(false)
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0x00})
				})
				Convey("true", func() {
					err := encoder.Encode(true)
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0x01})
				})
			})
			Convey("int", func() {
				schema := avroschema.AvroTypeInt
				var buffer bytes.Buffer
				encoder := avro.NewEncoder(&buffer, schema)
				err := encoder.Encode(int32(42))
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{0x54})
			})
			Convey("long", func() {
				schema := avroschema.AvroTypeLong
				var buffer bytes.Buffer
				encoder := avro.NewEncoder(&buffer, schema)
				err := encoder.Encode(int64(42))
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{0x54})
			})
			// float
			// double
			Convey("bytes", func() {
				schema := avroschema.AvroTypeBytes
				var buffer bytes.Buffer
				encoder := avro.NewEncoder(&buffer, schema)
				err := encoder.Encode([]byte{0x66, 0x6f, 0x6f})
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{0x06, 0x66, 0x6f, 0x6f})
			})
			Convey("string", func() {
				schema := avroschema.AvroTypeString
				var buffer bytes.Buffer
				encoder := avro.NewEncoder(&buffer, schema)
				err := encoder.Encode("foo")
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{0x06, 0x66, 0x6f, 0x6f})
			})
		})
		Convey("complex types", func() {
			Convey("simple", func() {
				Convey("record", func() {
					schema := &avroschema.Record{
						SchemaBase: avroschema.SchemaBase{
							Type: avroschema.AvroTypeRecord,
						},
						NamedType: avroschema.NamedType{
							Name: "SimpleRecord",
						},
						Fields: []*avroschema.RecordField{
							{
								Name: "name",
								Type: avroschema.AvroTypeString,
							},
							{
								Name: "age",
								Type: avroschema.AvroTypeInt,
							},
						},
					}
					var buffer bytes.Buffer
					encoder := avro.NewEncoder(&buffer, schema)
					value := struct {
						Name string `avro:"name"`
						Age  int32  `avro:"age"`
					}{
						Name: "foo",
						Age:  42,
					}
					err := encoder.Encode(value)
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0x06, 0x66, 0x6f, 0x6f, 0x54})
				})
				Convey("enum", func() {
					schema := &avroschema.Enum{
						SchemaBase: avroschema.SchemaBase{
							Type: avroschema.AvroTypeEnum,
						},
						NamedType: avroschema.NamedType{
							Name: "SimpleEnum",
						},
						Symbols: []string{"A", "B", "C"},
					}
					var buffer bytes.Buffer
					encoder := avro.NewEncoder(&buffer, schema)
					err := encoder.Encode("B")
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0x02})
				})
				Convey("array", func() {
					schema := &avroschema.Array{
						SchemaBase: avroschema.SchemaBase{
							Type: avroschema.AvroTypeArray,
						},
						Items: avroschema.AvroTypeInt,
					}
					var buffer bytes.Buffer
					encoder := avro.NewEncoder(&buffer, schema)
					err := encoder.Encode([]int32{42, 43})
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0x04, 0x54, 0x56, 0x00})
				})
				Convey("map", func() {
					schema := &avroschema.Map{
						SchemaBase: avroschema.SchemaBase{
							Type: avroschema.AvroTypeMap,
						},
						Values: avroschema.AvroTypeInt,
					}
					var buffer bytes.Buffer
					encoder := avro.NewEncoder(&buffer, schema)
					err := encoder.Encode(map[string]int32{"foo": 42, "bar": 43})
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00})
				})
				Convey("fixed", func() {
					schema := &avroschema.Fixed{
						SchemaBase: avroschema.SchemaBase{
							Type: avroschema.AvroTypeFixed,
						},
						NamedType: avroschema.NamedType{
							Name: "SimpleFixed",
						},
						Size: 3,
					}
					var buffer bytes.Buffer
					encoder := avro.NewEncoder(&buffer, schema)
					err := encoder.Encode([]byte{0x66, 0x6f, 0x6f})
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0x66, 0x6f, 0x6f})
				})
				Convey("union", func() {
					schema := avroschema.Union{
						avroschema.AvroTypeNull,
						avroschema.AvroTypeInt,
						avroschema.AvroTypeString,
					}
					var buffer bytes.Buffer
					encoder := avro.NewEncoder(&buffer, schema)
					Convey("null", func() {
						err := encoder.Encode(nil)
						So(err, ShouldBeNil)
						So(buffer.Bytes(), ShouldResemble, []byte{0x00})
					})
					Convey("int", func() {
						err := encoder.Encode(int32(42))
						So(err, ShouldBeNil)
						So(buffer.Bytes(), ShouldResemble, []byte{0x02, 0x54})
					})
					Convey("string", func() {
						err := encoder.Encode("foo")
						So(err, ShouldBeNil)
						So(buffer.Bytes(), ShouldResemble, []byte{0x04, 0x06, 0x66, 0x6f, 0x6f})
					})
				})
			})
		})
	})
}
