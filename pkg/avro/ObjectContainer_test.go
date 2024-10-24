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

type Person struct {
	Name string
	Age  int32
}

func (person *Person) WriteAvro(writer avro.Writer) (int, error) {
	var n int
	var nTotal int
	var err error
	n, err = avro.WriteString(writer, person.Name)
	nTotal += n
	if err != nil {
		return nTotal, err
	}
	n, err = avro.WriteInt(writer, person.Age)
	nTotal += n
	if err != nil {
		return nTotal, err
	}
	return nTotal, nil
}

func (person *Person) ReadAvro(reader avro.Reader) error {
	var err error
	person.Name, err = avro.ReadString(reader)
	if err != nil {
		return err
	}
	return avro.ReadInt(reader, &person.Age)
}

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
		bufferedFileWriter := bufio.NewWriter(file)
		defer bufferedFileWriter.Flush()
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
		header := avro.NewObjectContainerHeader(avro.NewObjectContainerHeaderInput{
			Schema:           schema,
			CompressionCodec: codec,
		})
		err = header.WriteAvro(bufferedFileWriter)
		if err != nil {
			t.Fatal(err)
		}
		var people []Person
		nPeople := 16
		for i := 0; i < nPeople; i++ {
			person := Person{
				Name: generateRandomString(64),
				Age:  rand.Int31(),
			}
			people = append(people, person)
		}
		var block avro.ObjectBlock
		bufferedCodecWriter := bufio.NewWriter(nil)
		for i := 0; i < nBlocks; i++ {
			block.Reset()
			var wc io.WriteCloser
			wc, err = avro.NewCodecWriter(&block, codec)
			if err != nil {
				t.Fatal(err)
			}
			bufferedCodecWriter.Reset(wc)
			for j := 0; j < nObjectsPerBlock; j++ {
				person := people[rand.Intn(nPeople)]
				_, err = person.WriteAvro(bufferedCodecWriter)
				if err != nil {
					t.Fatal(err)
				}
				block.Length++
			}
			err = bufferedCodecWriter.Flush()
			if err != nil {
				t.Fatal(err)
			}
			err = wc.Close()
			if err != nil {
				t.Fatal(err)
			}
			_, err = avro.WriteObjectBlock(bufferedFileWriter, &block, header.Sync)
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
