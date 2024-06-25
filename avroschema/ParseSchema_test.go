package avroschema_test

import (
	"io/ioutil"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"tps-git.topcon.com/cloud/avro/avroschema"
)

func TestParseSchema(t *testing.T) {
	Convey("TestParseSchema", t, func() {
		Convey("primitive types", func() {
			Convey("string", func() {
				data, err := ioutil.ReadFile("testdata/schemas/primitive_types/string.json")
				So(err, ShouldBeNil)
				schema, err := avroschema.ParseSchema(data)
				So(err, ShouldBeNil)
				So(schema.GetType(), ShouldEqual, avroschema.AvroTypeString)
			})
			Convey("int", func() {
				data, err := ioutil.ReadFile("testdata/schemas/primitive_types/int.json")
				So(err, ShouldBeNil)
				schema, err := avroschema.ParseSchema(data)
				So(err, ShouldBeNil)
				So(schema.GetType(), ShouldEqual, avroschema.AvroTypeInt)
			})
			Convey("long", func() {
				data, err := ioutil.ReadFile("testdata/schemas/primitive_types/long.json")
				So(err, ShouldBeNil)
				schema, err := avroschema.ParseSchema(data)
				So(err, ShouldBeNil)
				So(schema.GetType(), ShouldEqual, avroschema.AvroTypeLong)
			})
			Convey("float", func() {
				data, err := ioutil.ReadFile("testdata/schemas/primitive_types/float.json")
				So(err, ShouldBeNil)
				schema, err := avroschema.ParseSchema(data)
				So(err, ShouldBeNil)
				So(schema.GetType(), ShouldEqual, avroschema.AvroTypeFloat)
			})
		})
		Convey("complex types", func() {
			Convey("simple", func() {
				Convey("record", func() {
					data, err := ioutil.ReadFile("testdata/schemas/complex_types/simple/record.json")
					So(err, ShouldBeNil)
					schema, err := avroschema.ParseSchema(data)
					So(err, ShouldBeNil)
					So(schema.GetType(), ShouldEqual, avroschema.AvroTypeRecord)
					record := schema.(*avroschema.Record)
					So(record.Name, ShouldEqual, "SimpleRecord")
					So(record.Fields, ShouldHaveLength, 1)
					field := record.Fields[0]
					So(field.Name, ShouldEqual, "name")
					So(field.Type.GetType(), ShouldEqual, avroschema.AvroTypeString)
				})
				Convey("enum", func() {
					data, err := ioutil.ReadFile("testdata/schemas/complex_types/simple/enum.json")
					So(err, ShouldBeNil)
					schema, err := avroschema.ParseSchema(data)
					So(err, ShouldBeNil)
					So(schema.GetType(), ShouldEqual, avroschema.AvroTypeEnum)
					enum := schema.(*avroschema.Enum)
					So(enum.Name, ShouldEqual, "SimpleEnum")
					So(enum.Symbols, ShouldResemble, []string{"A", "B", "C"})
				})
				Convey("array", func() {
					data, err := ioutil.ReadFile("testdata/schemas/complex_types/simple/array.json")
					So(err, ShouldBeNil)
					schema, err := avroschema.ParseSchema(data)
					So(err, ShouldBeNil)
					So(schema.GetType(), ShouldEqual, avroschema.AvroTypeArray)
					avroArray := schema.(*avroschema.Array)
					So(avroArray.Items.GetType(), ShouldEqual, avroschema.AvroTypeString)
				})
				Convey("map", func() {
					data, err := ioutil.ReadFile("testdata/schemas/complex_types/simple/map.json")
					So(err, ShouldBeNil)
					schema, err := avroschema.ParseSchema(data)
					So(err, ShouldBeNil)
					So(schema.GetType(), ShouldEqual, avroschema.AvroTypeMap)
					avroMap := schema.(*avroschema.Map)
					So(avroMap.Values.GetType(), ShouldEqual, avroschema.AvroTypeString)
				})
				Convey("fixed", func() {
					data, err := ioutil.ReadFile("testdata/schemas/complex_types/simple/fixed.json")
					So(err, ShouldBeNil)
					schema, err := avroschema.ParseSchema(data)
					So(err, ShouldBeNil)
					So(schema.GetType(), ShouldEqual, avroschema.AvroTypeFixed)
					fixed := schema.(*avroschema.Fixed)
					So(fixed.Name, ShouldEqual, "SimpleFixed")
					So(fixed.Size, ShouldEqual, 16)
				})
				Convey("union", func() {
					data, err := ioutil.ReadFile("testdata/schemas/complex_types/simple/union.json")
					So(err, ShouldBeNil)
					schema, err := avroschema.ParseSchema(data)
					So(err, ShouldBeNil)
					So(schema.GetType(), ShouldEqual, avroschema.AvroTypeUnion)
					union := schema.(avroschema.Union)
					So(union, ShouldHaveLength, 2)
					So(union[0].GetType(), ShouldEqual, avroschema.AvroTypeNull)
					So(union[1].GetType(), ShouldEqual, avroschema.AvroTypeString)
				})
			})
			Convey("complex", func() {
				Convey("record", func() {
					data, err := ioutil.ReadFile("testdata/schemas/complex_types/complex/record.json")
					So(err, ShouldBeNil)
					schema, err := avroschema.ParseSchema(data)
					So(err, ShouldBeNil)
					So(schema.GetType(), ShouldEqual, avroschema.AvroTypeRecord)
					record := schema.(*avroschema.Record)
					So(record.Name, ShouldEqual, "ComplexRecord")
					So(record.Fields, ShouldHaveLength, 3)

					field := record.Fields[0]
					So(field.Name, ShouldEqual, "required_float")
					So(field.Type.GetType(), ShouldEqual, avroschema.AvroTypeFloat)

					field = record.Fields[1]
					So(field.Name, ShouldEqual, "optional_string")
					So(field.Type.GetType(), ShouldEqual, avroschema.AvroTypeUnion)
					union := field.Type.(avroschema.Union)
					So(union, ShouldHaveLength, 2)
					So(union[0].GetType(), ShouldEqual, avroschema.AvroTypeNull)
					So(union[1].GetType(), ShouldEqual, avroschema.AvroTypeString)

					// TODO is this allowed? should the enum be created as a named type and then referenced in the field type?
					field = record.Fields[2]
					So(field.Name, ShouldEqual, "enum")
					So(field.Type.GetType(), ShouldEqual, avroschema.AvroTypeEnum)
					enum := field.Type.(*avroschema.Enum)
					So(enum.Name, ShouldEqual, "ABC")
					So(enum.Symbols, ShouldResemble, []string{"A", "B", "C"})
				})
				Convey("union", func() {
					data, err := ioutil.ReadFile("testdata/schemas/complex_types/complex/union.json")
					So(err, ShouldBeNil)
					schema, err := avroschema.ParseSchema(data)
					So(err, ShouldBeNil)
					So(schema.GetType(), ShouldEqual, avroschema.AvroTypeUnion)
					union := schema.(avroschema.Union)
					So(union, ShouldHaveLength, 6)
					So(union[0].GetType(), ShouldEqual, avroschema.AvroTypeNull)

					So(union[1].GetType(), ShouldEqual, avroschema.AvroTypeRecord)
					record := union[1].(*avroschema.Record)
					So(record.Name, ShouldEqual, "record")
					So(record.Fields, ShouldHaveLength, 1)
					field := record.Fields[0]
					So(field.Name, ShouldEqual, "name")
					So(field.Type.GetType(), ShouldEqual, avroschema.AvroTypeString)

					So(union[2].GetType(), ShouldEqual, avroschema.AvroTypeEnum)
					enum := union[2].(*avroschema.Enum)
					So(enum.Name, ShouldEqual, "ABC")
					So(enum.Symbols, ShouldResemble, []string{"A", "B", "C"})

					So(union[3].GetType(), ShouldEqual, avroschema.AvroTypeArray)
					avroArray := union[3].(*avroschema.Array)
					So(avroArray.Items.GetType(), ShouldEqual, avroschema.AvroTypeString)

					So(union[4].GetType(), ShouldEqual, avroschema.AvroTypeMap)
					avroMap := union[4].(*avroschema.Map)
					So(avroMap.Values.GetType(), ShouldEqual, avroschema.AvroTypeString)

					So(union[5].GetType(), ShouldEqual, avroschema.AvroTypeFixed)
					fixed := union[5].(*avroschema.Fixed)
					So(fixed.Name, ShouldEqual, "chunk")
					So(fixed.Size, ShouldEqual, 16)
				})
			})
		})
	})
}
