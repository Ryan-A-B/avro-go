package avro_test

import (
	"bufio"
	"io"
	"math/rand"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/Ryan-A-B/avro-go/pkg/avro"
	"github.com/Ryan-A-B/avro-go/pkg/avroschema"
)

func TestCreateObjectContainerFile(t *testing.T) {
	nBlocks := 16
	nObjectsPerBlock := 1024
	Convey("TestCreateObjectContainerFile", t, func() {
		var name string
		var codec avro.CompressionCodec
		Convey("null", func() {
			name = "testdata/people-null.avro"
			codec = avro.CompressionCodecNull
		})
		Convey("deflate", func() {
			name = "testdata/people-deflate.avro"
			codec = avro.CompressionCodecDeflate
		})
		Convey("snappy", func() {
			name = "testdata/people-snappy.avro"
			codec = avro.CompressionCodecSnappy
		})
		file, err := os.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		defer writer.Flush()
		schema, err := avroschema.ParseSchema([]byte(`{
			"type": "record",
			"name": "Person",
			"fields": [
				{"name": "name", "type": "string"},
				{"name": "age", "type": "int"}
			]
		}`))
		if err != nil {
			t.Fatal(err)
		}
		type Person struct {
			Name string `avro:"name"`
			Age  int32  `avro:"age"`
		}
		header := avro.NewObjectContainerHeader(avro.NewObjectContainerHeaderInput{
			Schema:           schema,
			CompressionCodec: codec,
		})
		err = header.WriteAvro(writer)
		if err != nil {
			t.Fatal(err)
		}
		var block avro.ObjectBlock
		encoder := avro.NewEncoder(&block, schema)
		for i := 0; i < nBlocks; i++ {
			block.Reset()
			var wc io.WriteCloser
			wc, err = avro.NewCodecWriter(&block, codec)
			if err != nil {
				t.Fatal(err)
			}
			for j := 0; j < nObjectsPerBlock; j++ {
				err = encoder.Encode(Person{
					Name: generateRandomString(64),
					Age:  rand.Int31(),
				})
				if err != nil {
					t.Fatal(err)
				}
				block.Length++
			}
			err = wc.Close()
			if err != nil {
				t.Fatal(err)
			}
			_, err = avro.WriteObjectBlock(writer, &block, header.Sync)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = charset[rand.Intn(len(charset))]
	}
	return string(data)
}
