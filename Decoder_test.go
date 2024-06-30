package avro_test

import (
	"bytes"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/Ryan-A-B/avro-go"
	"github.com/Ryan-A-B/avro-go/avroschema"
)

func TestDecoder(t *testing.T) {
	Convey("TestDecoder", t, func() {
		Convey("primitive types", func() {
			Convey("boolean", func() {
				Convey("false", func() {
					data := []byte{0x00}
					var value bool
					err := avro.NewDecoder(bytes.NewReader(data), avroschema.AvroTypeBoolean).Decode(&value)
					So(err, ShouldBeNil)
					So(value, ShouldBeFalse)
				})
				Convey("true", func() {
					data := []byte{0x01}
					var value bool
					err := avro.NewDecoder(bytes.NewReader(data), avroschema.AvroTypeBoolean).Decode(&value)
					So(err, ShouldBeNil)
					So(value, ShouldBeTrue)
				})
			})
			Convey("int", func() {
				data := []byte{0x54}
				var value int32
				err := avro.NewDecoder(bytes.NewReader(data), avroschema.AvroTypeInt).Decode(&value)
				So(err, ShouldBeNil)
				So(value, ShouldEqual, 42)
			})
			Convey("long", func() {
				data := []byte{0x54}
				var value int64
				err := avro.NewDecoder(bytes.NewReader(data), avroschema.AvroTypeLong).Decode(&value)
				So(err, ShouldBeNil)
				So(value, ShouldEqual, 42)
			})
			// float
			// double
			Convey("bytes", func() {
				data := []byte{0x06, 0x66, 0x6f, 0x6f}
				var value []byte
				err := avro.NewDecoder(bytes.NewReader(data), avroschema.AvroTypeBytes).Decode(&value)
				So(err, ShouldBeNil)
				So(value, ShouldResemble, []byte{0x66, 0x6f, 0x6f})
			})
			Convey("string", func() {
				data := []byte{0x06, 0x66, 0x6f, 0x6f}
				var value string
				err := avro.NewDecoder(bytes.NewReader(data), avroschema.AvroTypeString).Decode(&value)
				So(err, ShouldBeNil)
				So(value, ShouldEqual, "foo")
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
					data := []byte{0x06, 0x66, 0x6f, 0x6f, 0x54}
					var value struct {
						Name string `avro:"name"`
						Age  int32  `avro:"age"`
					}
					err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
					So(err, ShouldBeNil)
					So(value.Name, ShouldEqual, "foo")
					So(value.Age, ShouldEqual, 42)
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
					Convey("A", func() {
						data := []byte{0x00}
						var value string
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldEqual, "A")
					})
					Convey("B", func() {
						data := []byte{0x02}
						var value string
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldEqual, "B")
					})
					Convey("C", func() {
						data := []byte{0x04}
						var value string
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldEqual, "C")
					})
				})
				Convey("array", func() {
					Convey("boolean", func() {
						schema := &avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: avroschema.AvroTypeBoolean,
						}
						data := []byte{0x06, 0x01, 0x00, 0x01, 0x00}
						var value []bool
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, []bool{true, false, true})
					})
					Convey("int", func() {
						schema := &avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: avroschema.AvroTypeInt,
						}
						data := []byte{0x04, 0x54, 0x56, 0x00}
						var value []int32
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, []int32{42, 43})
					})
					Convey("string", func() {
						schema := &avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: avroschema.AvroTypeString,
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72, 0x00}
						var value []string
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, []string{"foo", "bar"})
					})
				})
				Convey("map", func() {
					Convey("boolean", func() {
						schema := &avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: avroschema.AvroTypeBoolean,
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x01, 0x06, 0x62, 0x61, 0x72, 0x00, 0x00}
						var value map[string]bool
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, map[string]bool{"foo": true, "bar": false})
					})
					Convey("int", func() {
						schema := &avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: avroschema.AvroTypeInt,
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00}
						var value map[string]int32
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, map[string]int32{"foo": 42, "bar": 43})
					})
					Convey("string", func() {
						schema := &avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: avroschema.AvroTypeString,
						}
						data := []byte{0x02, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72, 0x00}
						var value map[string]string
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, map[string]string{"foo": "bar"})
					})
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
					data := []byte{0x66, 0x6f, 0x6f}
					value := make([]byte, 3)
					err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
					So(err, ShouldBeNil)
					So(value, ShouldResemble, []byte{0x66, 0x6f, 0x6f})
				})
				Convey("union", func() {
					schema := avroschema.Union{
						avroschema.AvroTypeNull,
						avroschema.AvroTypeInt,
						avroschema.AvroTypeString,
					}
					Convey("null", func() {
						data := []byte{0x00}
						var value interface{}
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldBeNil)
					})
					Convey("int", func() {
						data := []byte{0x02, 0x54}
						var value interface{}
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldHaveSameTypeAs, int32(42))
						So(value, ShouldEqual, int32(42))
					})
					Convey("string", func() {
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f}
						var value interface{}
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldHaveSameTypeAs, "foo")
						So(value, ShouldEqual, "foo")
					})
				})
			})
			Convey("complex", func() {
				Convey("record", func() {
					Convey("with nested complex types", func() {
						record := avroschema.Record{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeRecord,
							},
							NamedType: avroschema.NamedType{
								Name: "ComplexRecord",
							},
							Fields: []*avroschema.RecordField{
								{
									Name: "nested_record",
									Type: &avroschema.Record{
										SchemaBase: avroschema.SchemaBase{
											Type: avroschema.AvroTypeRecord,
										},
										NamedType: avroschema.NamedType{
											Name: "NestedRecord",
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
									},
								},
								{
									Name: "nested_enum",
									Type: &avroschema.Enum{
										SchemaBase: avroschema.SchemaBase{
											Type: avroschema.AvroTypeEnum,
										},
										NamedType: avroschema.NamedType{
											Name: "NestedEnum",
										},
										Symbols: []string{"A", "B", "C"},
									},
								},
								{
									Name: "nested_array",
									Type: &avroschema.Array{
										SchemaBase: avroschema.SchemaBase{
											Type: avroschema.AvroTypeArray,
										},
										Items: avroschema.AvroTypeInt,
									},
								},
								{
									Name: "nested_map",
									Type: &avroschema.Map{
										SchemaBase: avroschema.SchemaBase{
											Type: avroschema.AvroTypeMap,
										},
										Values: avroschema.AvroTypeInt,
									},
								},
								{
									Name: "nested_fixed",
									Type: &avroschema.Fixed{
										SchemaBase: avroschema.SchemaBase{
											Type: avroschema.AvroTypeFixed,
										},
										NamedType: avroschema.NamedType{
											Name: "NestedFixed",
										},
										Size: 3,
									},
								},
							},
						}
						data := []byte{0x06, 0x66, 0x6f, 0x6f, 0x54, 0x00, 0x04, 0x54, 0x56, 0x00, 0x02, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x00, 0x66, 0x6f, 0x6f}
						var value struct {
							NestedRecord struct {
								Name string `avro:"name"`
								Age  int32  `avro:"age"`
							} `avro:"nested_record"`
							NestedEnum  string           `avro:"nested_enum"`
							NestedArray []int32          `avro:"nested_array"`
							NestedMap   map[string]int32 `avro:"nested_map"`
							NestedFixed [3]byte          `avro:"nested_fixed"`
						}
						err := avro.NewDecoder(bytes.NewReader(data), &record).Decode(&value)
						So(err, ShouldBeNil)
						So(value.NestedRecord.Name, ShouldEqual, "foo")
						So(value.NestedRecord.Age, ShouldEqual, 42)
						So(value.NestedEnum, ShouldEqual, "A")
						So(value.NestedArray, ShouldResemble, []int32{42, 43})
						So(value.NestedMap, ShouldResemble, map[string]int32{"foo": 42})
						So(value.NestedFixed, ShouldResemble, [3]byte{0x66, 0x6f, 0x6f})
					})
					Convey("without all fields", func() {
						type Person struct {
							Name string `avro:"name"`
						}
						schema := &avroschema.Record{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeRecord,
							},
							NamedType: avroschema.NamedType{
								Name: "Person",
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
						data := []byte{0x06, 0x66, 0x6f, 0x6f, 0x54}
						var value Person
						err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, Person{"foo"})
					})
				})
				Convey("array", func() {
					Convey("nested record", func() {
						array := avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: &avroschema.Record{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeRecord,
								},
								NamedType: avroschema.NamedType{
									Name: "NestedRecord",
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
							},
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00}
						type Person struct {
							Name string `avro:"name"`
							Age  int32  `avro:"age"`
						}
						var people []Person
						err := avro.NewDecoder(bytes.NewReader(data), &array).Decode(&people)
						So(err, ShouldBeNil)
						So(people, ShouldResemble, []Person{{"foo", 42}, {"bar", 43}})
					})
					Convey("nested enum", func() {
						array := avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: &avroschema.Enum{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeEnum,
								},
								NamedType: avroschema.NamedType{
									Name: "SimpleEnum",
								},
								Symbols: []string{"A", "B", "C"},
							},
						}
						data := []byte{0x08, 0x02, 0x04, 0x00, 0x02, 0x00}
						var value []string
						err := avro.NewDecoder(bytes.NewReader(data), &array).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, []string{"B", "C", "A", "B"})
					})
					Convey("nested array", func() {
						array := avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: &avroschema.Array{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeArray,
								},
								Items: avroschema.AvroTypeInt,
							},
						}
						data := []byte{0x04, 0x04, 0x54, 0x56, 0x00, 0x04, 0x54, 0x56, 0x00, 0x00}
						var value [][]int32
						err := avro.NewDecoder(bytes.NewReader(data), &array).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, [][]int32{{42, 43}, {42, 43}})
					})
					Convey("nested map", func() {
						array := avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: &avroschema.Map{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeMap,
								},
								Values: avroschema.AvroTypeInt,
							},
						}
						data := []byte{0x04, 0x04, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00, 0x02, 0x06, 0x62, 0x61, 0x72, 0x54, 0x00, 0x00}
						var value []map[string]int32
						err := avro.NewDecoder(bytes.NewReader(data), &array).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, []map[string]int32{{"foo": 42, "bar": 43}, {"bar": 42}})
					})
					Convey("nested fixed", func() {
						array := avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: &avroschema.Fixed{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeFixed,
								},
								NamedType: avroschema.NamedType{
									Name: "NestedFixed",
								},
								Size: 3,
							},
						}
						data := []byte{0x04, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72, 0x00}
						var value [][3]byte
						err := avro.NewDecoder(bytes.NewReader(data), &array).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, [][3]byte{{0x66, 0x6f, 0x6f}, {0x62, 0x61, 0x72}})
					})
				})
				Convey("map", func() {
					Convey("nested record", func() {
						mapSchema := avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: &avroschema.Record{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeRecord,
								},
								NamedType: avroschema.NamedType{
									Name: "NestedRecord",
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
							},
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00}
						type Person struct {
							Name string `avro:"name"`
							Age  int32  `avro:"age"`
						}
						people := make(map[string]Person)
						err := avro.NewDecoder(bytes.NewReader(data), &mapSchema).Decode(&people)
						So(err, ShouldBeNil)
						So(people, ShouldResemble, map[string]Person{"foo": {"foo", 42}, "bar": {"bar", 43}})
					})
					Convey("nested enum", func() {
						mapSchema := avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: &avroschema.Enum{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeEnum,
								},
								NamedType: avroschema.NamedType{
									Name: "SimpleEnum",
								},
								Symbols: []string{"A", "B", "C"},
							},
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x02, 0x06, 0x62, 0x61, 0x72, 0x04, 0x00}
						value := make(map[string]string)
						err := avro.NewDecoder(bytes.NewReader(data), &mapSchema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, map[string]string{"foo": "B", "bar": "C"})
					})
					Convey("nested array", func() {
						mapSchema := avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: &avroschema.Array{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeArray,
								},
								Items: avroschema.AvroTypeInt,
							},
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0x54, 0x56, 0x00, 0x06, 0x62, 0x61, 0x72, 0x04, 0x54, 0x56, 0x00, 0x00}
						value := make(map[string][]int32)
						err := avro.NewDecoder(bytes.NewReader(data), &mapSchema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, map[string][]int32{"foo": {42, 43}, "bar": {42, 43}})
					})
					Convey("nested map", func() {
						mapSchema := avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: &avroschema.Map{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeMap,
								},
								Values: avroschema.AvroTypeInt,
							},
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00, 0x06, 0x62, 0x61, 0x72, 0x02, 0x06, 0x62, 0x61, 0x72, 0x54, 0x00, 0x00}
						value := make(map[string]map[string]int32)
						err := avro.NewDecoder(bytes.NewReader(data), &mapSchema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, map[string]map[string]int32{"foo": {"foo": 42, "bar": 43}, "bar": {"bar": 42}})
					})
					Convey("nested fixed", func() {
						mapSchema := avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: &avroschema.Fixed{
								SchemaBase: avroschema.SchemaBase{
									Type: avroschema.AvroTypeFixed,
								},
								NamedType: avroschema.NamedType{
									Name: "NestedFixed",
								},
								Size: 3,
							},
						}
						data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72, 0x62, 0x61, 0x72, 0x00}
						value := make(map[string][3]byte)
						err := avro.NewDecoder(bytes.NewReader(data), &mapSchema).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldResemble, map[string][3]byte{"foo": {0x66, 0x6f, 0x6f}, "bar": {0x62, 0x61, 0x72}})
					})
				})
				Convey("union", func() {
					union := avroschema.Union{
						avroschema.AvroTypeNull,
						&avroschema.Enum{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeEnum,
							},
							NamedType: avroschema.NamedType{
								Name: "NestedEnum",
							},
							Symbols: []string{"A", "B", "C"},
						},
						&avroschema.Array{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeArray,
							},
							Items: avroschema.AvroTypeInt,
						},
						&avroschema.Map{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeMap,
							},
							Values: avroschema.AvroTypeInt,
						},
						&avroschema.Fixed{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeFixed,
							},
							NamedType: avroschema.NamedType{
								Name: "NestedFixed",
							},
							Size: 3,
						},
					}
					Convey("null", func() {
						data := []byte{0x00}
						var value interface{}
						err := avro.NewDecoder(bytes.NewReader(data), union).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldBeNil)
					})
					Convey("enum", func() {
						data := []byte{0x02, 0x04}
						var value interface{}
						err := avro.NewDecoder(bytes.NewReader(data), union).Decode(&value)
						So(err, ShouldBeNil)
						So(value, ShouldHaveSameTypeAs, "C")
						So(value, ShouldEqual, "C")
					})
					Convey("array", func() {
						Convey("int", func() {
							data := []byte{0x04, 0x04, 0x54, 0x56, 0x00}
							var value interface{}
							err := avro.NewDecoder(bytes.NewReader(data), union).Decode(&value)
							So(err, ShouldBeNil)
							So(value, ShouldHaveSameTypeAs, []int32{42, 43})
							So(value, ShouldResemble, []int32{42, 43})
						})
					})
					Convey("map", func() {
						Convey("int", func() {
							data := []byte{0x06, 0x04, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00}
							var value interface{}
							err := avro.NewDecoder(bytes.NewReader(data), union).Decode(&value)
							So(err, ShouldBeNil)
							So(value, ShouldHaveSameTypeAs, map[string]int32{"foo": 42, "bar": 43})
							So(value, ShouldResemble, map[string]int32{"foo": 42, "bar": 43})
						})
					})
					Convey("fixed", func() {
						data := []byte{0x08, 0x66, 0x6f, 0x6f}
						var value interface{}
						err := avro.NewDecoder(bytes.NewReader(data), union).Decode(&value)
						So(err, ShouldBeNil)
						expectedValue := []byte{0x66, 0x6f, 0x6f}
						So(value, ShouldHaveSameTypeAs, expectedValue)
						So(value, ShouldResemble, expectedValue)
					})
				})
				Convey("Optional", func() {
					Convey("int", func() {
						schema := avroschema.Union{
							avroschema.AvroTypeNull,
							avroschema.AvroTypeInt,
						}
						Convey("null", func() {
							data := []byte{0x00}
							value := new(int32)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value, ShouldResemble, new(int32))
						})
						Convey("int", func() {
							data := []byte{0x02, 0x54}
							value := new(int32)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value, ShouldNotBeNil)
							So(*value, ShouldEqual, 42)
						})
					})
					Convey("record", func() {
						schema := avroschema.Union{
							avroschema.AvroTypeNull,
							&avroschema.Record{
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
							},
						}
						type Person struct {
							Name string `avro:"name"`
							Age  int32  `avro:"age"`
						}
						Convey("null", func() {
							data := []byte{0x00}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value, ShouldResemble, new(Person))
						})
						Convey("record", func() {
							data := []byte{0x02, 0x06, 0x66, 0x6f, 0x6f, 0x54}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value.Name, ShouldEqual, "foo")
							So(value.Age, ShouldEqual, 42)
						})
					})
					Convey("int inside record", func() {
						type Person struct {
							Name string `avro:"name"`
							Age  *int32 `avro:"age"`
						}
						schema := &avroschema.Record{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeRecord,
							},
							NamedType: avroschema.NamedType{
								Name: "Person",
							},
							Fields: []*avroschema.RecordField{
								{
									Name: "name",
									Type: avroschema.AvroTypeString,
								},
								{
									Name: "age",
									Type: avroschema.Union{
										avroschema.AvroTypeNull,
										avroschema.AvroTypeInt,
									},
								},
							},
						}
						Convey("null", func() {
							data := []byte{0x06, 0x66, 0x6f, 0x6f, 0x00}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value, ShouldResemble, &Person{
								Name: "foo",
							})
						})
						Convey("int", func() {
							data := []byte{0x06, 0x66, 0x6f, 0x6f, 0x02, 0x54}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value.Name, ShouldEqual, "foo")
							So(*value.Age, ShouldEqual, 42)
						})
					})
					Convey("record inside record", func() {
						type Info struct {
							Name string `avro:"name"`
							Age  int32  `avro:"age"`
						}
						type Person struct {
							ID   string `avro:"id"`
							Info *Info  `avro:"info"`
						}
						schema := &avroschema.Record{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeRecord,
							},
							NamedType: avroschema.NamedType{
								Name: "Person",
							},
							Fields: []*avroschema.RecordField{
								{
									Name: "id",
									Type: avroschema.AvroTypeString,
								},
								{
									Name: "info",
									Type: avroschema.Union{
										avroschema.AvroTypeNull,
										&avroschema.Record{
											SchemaBase: avroschema.SchemaBase{
												Type: avroschema.AvroTypeRecord,
											},
											NamedType: avroschema.NamedType{
												Name: "Info",
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
										},
									},
								},
							},
						}
						Convey("null", func() {
							data := []byte{0x04, 0x69, 0x64, 0x00}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value, ShouldResemble, &Person{
								ID: "id",
							})
						})
						Convey("record", func() {
							data := []byte{0x04, 0x69, 0x64, 0x02, 0x06, 0x66, 0x6f, 0x6f, 0x54}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value.ID, ShouldEqual, "id")
							So(value.Info.Name, ShouldEqual, "foo")
							So(value.Info.Age, ShouldEqual, 42)
						})
					})
					Convey("array inside record", func() {
						type Person struct {
							ID    string    `avro:"id"`
							Names *[]string `avro:"names"`
						}
						schema := &avroschema.Record{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeRecord,
							},
							NamedType: avroschema.NamedType{
								Name: "Person",
							},
							Fields: []*avroschema.RecordField{
								{
									Name: "id",
									Type: avroschema.AvroTypeString,
								},
								{
									Name: "names",
									Type: avroschema.Union{
										avroschema.AvroTypeNull,
										&avroschema.Array{
											SchemaBase: avroschema.SchemaBase{
												Type: avroschema.AvroTypeArray,
											},
											Items: avroschema.AvroTypeString,
										},
									},
								},
							},
						}
						Convey("null", func() {
							data := []byte{0x04, 0x69, 0x64, 0x00}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value, ShouldResemble, &Person{
								ID: "id",
							})
						})
						Convey("array", func() {
							data := []byte{0x04, 0x69, 0x64, 0x02, 0x04, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72, 0x00}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value.ID, ShouldEqual, "id")
							So(*value.Names, ShouldResemble, []string{"foo", "bar"})
						})
					})
					Convey("map inside record", func() {
						type Person struct {
							ID     string            `avro:"id"`
							Values *map[string]int32 `avro:"values"`
						}
						schema := &avroschema.Record{
							SchemaBase: avroschema.SchemaBase{
								Type: avroschema.AvroTypeRecord,
							},
							NamedType: avroschema.NamedType{
								Name: "Person",
							},
							Fields: []*avroschema.RecordField{
								{
									Name: "id",
									Type: avroschema.AvroTypeString,
								},
								{
									Name: "values",
									Type: avroschema.Union{
										avroschema.AvroTypeNull,
										&avroschema.Map{
											SchemaBase: avroschema.SchemaBase{
												Type: avroschema.AvroTypeMap,
											},
											Values: avroschema.AvroTypeInt,
										},
									},
								},
							},
						}
						Convey("null", func() {
							data := []byte{0x04, 0x69, 0x64, 0x00}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value, ShouldResemble, &Person{
								ID: "id",
							})
						})
						Convey("map", func() {
							data := []byte{
								0x04, 0x69, 0x64,
								0x02,
								0x04,
								0x06, 0x66, 0x6f, 0x6f, 0x54,
								0x06, 0x62, 0x61, 0x72, 0x56,
								0x00}
							value := new(Person)
							err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(value)
							So(err, ShouldBeNil)
							So(value.ID, ShouldEqual, "id")
							So(*value.Values, ShouldResemble, map[string]int32{"foo": 42, "bar": 43})
						})
					})
				})
			})
		})
	})
}

func BenchmarkDecodeInt(b *testing.B) {
	schema := avroschema.AvroTypeInt
	var buffer bytes.Buffer
	decoder := avro.NewDecoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var value int32
		buffer.Write([]byte{0x54})
		decoder.Decode(&value)
	}
}

func BenchmarkDecodeString(b *testing.B) {
	schema := avroschema.AvroTypeString
	var buffer bytes.Buffer
	decoder := avro.NewDecoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var value string
		buffer.Write([]byte{0x06, 0x66, 0x6f, 0x6f})
		decoder.Decode(&value)
	}
}

func BenchmarkDecodeArray(b *testing.B) {
	schema := &avroschema.Array{
		SchemaBase: avroschema.SchemaBase{
			Type: avroschema.AvroTypeArray,
		},
		Items: avroschema.AvroTypeInt,
	}
	var buffer bytes.Buffer
	decoder := avro.NewDecoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var value []int32
		buffer.Write([]byte{0x04, 0x54, 0x56, 0x00})
		decoder.Decode(&value)
	}
}

func BenchmarkDecodeRecord(b *testing.B) {
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
	decoder := avro.NewDecoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var value struct {
			Name string `avro:"name"`
			Age  int32  `avro:"age"`
		}
		buffer.Write([]byte{0x06, 0x66, 0x6f, 0x6f, 0x54})
		decoder.Decode(&value)
	}
}

func BenchmarkDecodeUnion(b *testing.B) {
	schema := avroschema.Union{
		avroschema.AvroTypeNull,
		avroschema.AvroTypeInt,
		avroschema.AvroTypeString,
	}
	var buffer bytes.Buffer
	decoder := avro.NewDecoder(&buffer, schema)
	payloads := [][]byte{
		{0x00},
		{0x02, 0x54},
		{0x04, 0x06, 0x66, 0x6f, 0x6f},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var value interface{}
		buffer.Write(payloads[i%3])
		decoder.Decode(&value)
	}
}
