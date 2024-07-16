package avro

import (
	"bytes"
	"compress/zlib"
	"io"

	"github.com/golang/snappy"
)

var _ io.Writer = (*ObjectBlockBuilder)(nil)

type ObjectBlockBuilder struct {
	length int64
	buffer *bytes.Buffer
	writer io.WriteCloser
}

type NewObjectBlockBuilderInput struct {
	CompressionCodec CompressionCodec
}

func NewObjectBlockBuilder(input NewObjectBlockBuilderInput) *ObjectBlockBuilder {
	buffer := new(bytes.Buffer)
	var writer io.WriteCloser
	switch input.CompressionCodec {
	case CompressionCodecNull:
		writer = &NopCloser{buffer}
	case CompressionCodecDeflate:
		writer = zlib.NewWriter(buffer)
	case CompressionCodecSnappy:
		writer = snappy.NewBufferedWriter(buffer)
	default:
		panic("unknown compression codec")
	}
	return &ObjectBlockBuilder{
		buffer: buffer,
		writer: writer,
	}
}

func (objectBlockBuilder *ObjectBlockBuilder) Write(data []byte) (n int, err error) {
	// TODO can't do this - encode calls write multiple times
	objectBlockBuilder.length++
	return objectBlockBuilder.writer.Write(data)
}

func (objectBlockBuilder *ObjectBlockBuilder) Build() (block *ObjectBlock, err error) {
	err = objectBlockBuilder.writer.Close()
	if err != nil {
		return
	}
	block = &ObjectBlock{
		Length: objectBlockBuilder.length,
		data:   *objectBlockBuilder.buffer,
	}
	return
}

type NopCloser struct {
	io.Writer
}

func (nopCloser *NopCloser) Close() (err error) {
	return
}
