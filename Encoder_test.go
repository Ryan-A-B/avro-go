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
				Convey("value", func() {
					schema := avroschema.AvroTypeInt
					var buffer bytes.Buffer
					encoder := avro.NewEncoder(&buffer, schema)
					err := encoder.Encode(int32(42))
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0x54})
				})
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
					expectedData := [2][]byte{
						{0x04, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x06, 0x62, 0x61, 0x72, 0x56, 0x00},
						{0x04, 0x06, 0x62, 0x61, 0x72, 0x56, 0x06, 0x66, 0x6f, 0x6f, 0x54, 0x00},
					}
					So(expectedData, ShouldContain, buffer.Bytes())
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

func BenchmarkEncodeBoolean(b *testing.B) {
	schema := avroschema.AvroTypeBoolean
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := i%2 == 0
		encoder.Encode(value)
	}
}

func BenchmarkEncodeInt(b *testing.B) {
	schema := avroschema.AvroTypeInt
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(int32(i))
	}
}

func BenchmarkEncodeLong(b *testing.B) {
	schema := avroschema.AvroTypeLong
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(int64(i))
	}
}

func BenchmarkEncodeBytes(b *testing.B) {
	schema := avroschema.AvroTypeBytes
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode([]byte{0x66, 0x6f, 0x6f})
	}
}

func BenchmarkEncodeString(b *testing.B) {
	schema := avroschema.AvroTypeString
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode("foo")
	}
}

func BenchmarkEncodeRecord(b *testing.B) {
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
	type SimpleRecord struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}
	value := SimpleRecord{
		Name: "foo",
		Age:  42,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(value)
	}
}

func BenchmarkEncodeEnum(b *testing.B) {
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
	options := []string{"A", "B", "C"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(options[i%3])
	}
}

func BenchmarkEncodeArray(b *testing.B) {
	schema := &avroschema.Array{
		SchemaBase: avroschema.SchemaBase{
			Type: avroschema.AvroTypeArray,
		},
		Items: avroschema.AvroTypeInt,
	}
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	value := []int32{42, 43}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(value)
	}
}

func BenchmarkEncodeMap(b *testing.B) {
	schema := &avroschema.Map{
		SchemaBase: avroschema.SchemaBase{
			Type: avroschema.AvroTypeMap,
		},
		Values: avroschema.AvroTypeInt,
	}
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	value := map[string]int32{"foo": 42, "bar": 43}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(value)
	}
}

func BenchmarkEncodeFixed(b *testing.B) {
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
	value := []byte{0x66, 0x6f, 0x6f}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(value)
	}
}

func BenchmarkEncodeUnion(b *testing.B) {
	schema := avroschema.Union{
		avroschema.AvroTypeNull,
		avroschema.AvroTypeInt,
		avroschema.AvroTypeString,
	}
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	options := []interface{}{nil, int32(42), "foo"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(options[i%3])
	}
}

func BenchmarkEncodeRecordArray(b *testing.B) {
	schema := &avroschema.Array{
		SchemaBase: avroschema.SchemaBase{
			Type: avroschema.AvroTypeArray,
		},
		Items: &avroschema.Record{
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
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	type SimpleRecord struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}
	value := []SimpleRecord{
		{
			Name: "foo",
			Age:  42,
		},
		{
			Name: "bar",
			Age:  43,
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(value)
	}
}

func BenchmarkEncodeEnumArray(b *testing.B) {
	schema := &avroschema.Array{
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
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	options := [][]string{
		{"A", "B", "C"},
		{"C", "A", "B"},
		{"B", "C", "A"},
		{"A", "C", "B"},
		{"C", "B", "A"},
		{"B", "A", "C"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(options[i%6])
	}
}

func BenchmarkEncodeFixedArray(b *testing.B) {
	schema := &avroschema.Array{
		SchemaBase: avroschema.SchemaBase{
			Type: avroschema.AvroTypeArray,
		},
		Items: &avroschema.Fixed{
			SchemaBase: avroschema.SchemaBase{
				Type: avroschema.AvroTypeFixed,
			},
			NamedType: avroschema.NamedType{
				Name: "SimpleFixed",
			},
			Size: 3,
		},
	}
	var buffer bytes.Buffer
	encoder := avro.NewEncoder(&buffer, schema)
	value := [][]byte{
		{0x66, 0x6f, 0x6f},
		{0x62, 0x61, 0x72},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(value)
	}
}
