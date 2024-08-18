package avro_test

import (
	"bufio"
	"io"
	"os"
	"testing"

	"github.com/Ryan-A-B/avro-go/pkg/avro"
	. "github.com/smartystreets/goconvey/convey"
)

func TestObjectBlockIterator(t *testing.T) {
	Convey("TestObjectBlockIterator", t, func() {
		var name string
		Convey("null", func() {
			name = "testdata/people-null.avro"
		})
		Convey("deflate", func() {
			name = "testdata/people-deflate.avro"
		})
		Convey("snappy", func() {
			name = "testdata/people-snappy.avro"
		})
		file, err := os.Open(name)
		So(err, ShouldBeNil)
		defer file.Close()
		reader := bufio.NewReader(file)
		var header avro.ObjectContainerHeader
		err = header.ReadAvro(reader)
		So(err, ShouldBeNil)
		blockIterator := avro.NewObjectBlockIterator(avro.NewObjectBlockIteratorInput{
			Reader:       reader,
			ExpectedSync: header.Sync,
		})
		blockCount := 0
		var block avro.ObjectBlock
		for blockIterator.Next(&block) {
			So(block.Length, ShouldEqual, 1024)
			blockCount++
		}
		So(blockIterator.Err(), ShouldBeNil)
		So(blockCount, ShouldEqual, 16)
	})
}

func benchmarkObjectBlockIterator(b *testing.B, name string) {
	file, err := os.Open(name)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file.Seek(0, io.SeekStart)
		reader := bufio.NewReader(file)
		var header avro.ObjectContainerHeader
		err = header.ReadAvro(reader)
		if err != nil {
			b.Fatal(err)
		}
		blockIterator := avro.NewObjectBlockIterator(avro.NewObjectBlockIteratorInput{
			Reader:       reader,
			ExpectedSync: header.Sync,
		})
		var block avro.ObjectBlock
		for blockIterator.Next(&block) {
		}
		if err := blockIterator.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkObjectBlockIteratorNull(b *testing.B) {
	benchmarkObjectBlockIterator(b, "testdata/people-null.avro")
}

func BenchmarkObjectBlockIteratorDeflate(b *testing.B) {
	benchmarkObjectBlockIterator(b, "testdata/people-deflate.avro")
}

func BenchmarkObjectBlockIteratorSnappy(b *testing.B) {
	benchmarkObjectBlockIterator(b, "testdata/people-snappy.avro")
}
