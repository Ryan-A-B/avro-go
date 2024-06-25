package avro_test

import (
	"bytes"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"tps-git.topcon.com/cloud/avro"
	"tps-git.topcon.com/cloud/avro/avroschema"
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
				// 		Convey("map", func() {
				// 			Convey("boolean", func() {
				// 				schema := &avroschema.Map{
				// 					SchemaBase: avroschema.SchemaBase{
				// 						Type: avroschema.AvroTypeMap,
				// 					},
				// 					Values: avroschema.AvroTypeBoolean,
				// 				}
				// 				data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x01, 0x06, 0x62, 0x61, 0x72, 0x00, 0x00}
				// 				var value map[string]bool
				// 				err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
				// 				So(err, ShouldBeNil)
				// 				So(value, ShouldResemble, map[string]bool{"foo": true, "bar": false})
				// 			})
				// 			Convey("int", func() {
				// 				schema := &avroschema.Map{
				// 					SchemaBase: avroschema.SchemaBase{
				// 						Type: avroschema.AvroTypeMap,
				// 					},
				// 					Values: avroschema.AvroTypeInt,
				// 				}
				// 				data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00}
				// 				var value map[string]int32
				// 				err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
				// 				So(err, ShouldBeNil)
				// 				So(value, ShouldResemble, map[string]int32{"foo": 42, "bar": 43})
				// 			})
				// 			Convey("string", func() {
				// 				schema := &avroschema.Map{
				// 					SchemaBase: avroschema.SchemaBase{
				// 						Type: avroschema.AvroTypeMap,
				// 					},
				// 					Values: avroschema.AvroTypeString,
				// 				}
				// 				data := []byte{0x02, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72, 0x00}
				// 				var value map[string]string
				// 				err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
				// 				So(err, ShouldBeNil)
				// 				So(value, ShouldResemble, map[string]string{"foo": "bar"})
				// 			})
				// 		})
				// 		Convey("fixed", func() {
				// 			schema := &avroschema.Fixed{
				// 				SchemaBase: avroschema.SchemaBase{
				// 					Type: avroschema.AvroTypeFixed,
				// 				},
				// 				NamedType: avroschema.NamedType{
				// 					Name: "SimpleFixed",
				// 				},
				// 				Size: 3,
				// 			}
				// 			data := []byte{0x66, 0x6f, 0x6f}
				// 			value := make([]byte, 3)
				// 			err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
				// 			So(err, ShouldBeNil)
				// 			So(value, ShouldResemble, []byte{0x66, 0x6f, 0x6f})
				// 		})
				// 		Convey("union", func() {
				// 			schema := avroschema.Union{
				// 				avroschema.AvroTypeNull,
				// 				avroschema.AvroTypeInt,
				// 				avroschema.AvroTypeString,
				// 			}
				// 			Convey("null", func() {
				// 				data := []byte{0x00}
				// 				var value interface{}
				// 				err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
				// 				So(err, ShouldBeNil)
				// 				So(value, ShouldBeNil)
				// 			})
				// 			Convey("int", func() {
				// 				data := []byte{0x02, 0x54}
				// 				var value interface{}
				// 				err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
				// 				So(err, ShouldBeNil)
				// 				So(value, ShouldHaveSameTypeAs, int32(42))
				// 				So(value, ShouldEqual, int32(42))
				// 			})
				// 			Convey("string", func() {
				// 				data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f}
				// 				var value interface{}
				// 				err := avro.NewDecoder(bytes.NewReader(data), schema).Decode(&value)
				// 				So(err, ShouldBeNil)
				// 				So(value, ShouldHaveSameTypeAs, "foo")
				// 				So(value, ShouldEqual, "foo")
				// 			})
				// 		})
			})
			Convey("complex", func() {
				Convey("record", func() {
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
								Name: "nested_array",
								Type: &avroschema.Array{
									SchemaBase: avroschema.SchemaBase{
										Type: avroschema.AvroTypeArray,
									},
									Items: avroschema.AvroTypeInt,
								},
							},
						},
					}
					data := []byte{0x06, 0x66, 0x6f, 0x6f, 0x54, 0x04, 0x54, 0x56, 0x00}
					var value struct {
						NestedRecord struct {
							Name string `avro:"name"`
							Age  int32  `avro:"age"`
						} `avro:"nested_record"`
						NestedArray []int32 `avro:"nested_array"`
					}
					err := avro.NewDecoder(bytes.NewReader(data), &record).Decode(&value)
					So(err, ShouldBeNil)
					So(value.NestedRecord.Name, ShouldEqual, "foo")
					So(value.NestedRecord.Age, ShouldEqual, 42)
					So(value.NestedArray, ShouldResemble, []int32{42, 43})
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

// func BenchmarkDecodeUnion(b *testing.B) {
// 	schema := avroschema.Union{
// 		avroschema.AvroTypeNull,
// 		avroschema.AvroTypeInt,
// 		avroschema.AvroTypeString,
// 	}
// 	var buffer bytes.Buffer
// 	decoder := avro.NewDecoder(&buffer, schema)
// 	payloads := [][]byte{
// 		{0x00},
// 		{0x02, 0x54},
// 		{0x04, 0x06, 0x66, 0x6f, 0x6f},
// 	}
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		var value interface{}
// 		buffer.Write(payloads[i%3])
// 		decoder.Decode(&value)
// 	}
// }
