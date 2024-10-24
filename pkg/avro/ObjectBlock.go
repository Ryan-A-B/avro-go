package avro

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"

	"github.com/golang/snappy"
)

var _ io.Reader = (*ObjectBlock)(nil)
var _ io.ByteReader = (*ObjectBlock)(nil)
var _ io.Writer = (*ObjectBlock)(nil)
var _ io.ByteWriter = (*ObjectBlock)(nil)

type ObjectBlock struct {
	Length int64
	data   bytes.Buffer
}

func NewObjectBlockSize(size int) *ObjectBlock {
	objectBlock := new(ObjectBlock)
	objectBlock.data.Grow(size)
	return objectBlock
}

func (objectBlock *ObjectBlock) Read(p []byte) (n int, err error) {
	return objectBlock.data.Read(p)
}

func (objectBlock *ObjectBlock) ReadByte() (c byte, err error) {
	return objectBlock.data.ReadByte()
}

func (objectBlock *ObjectBlock) Write(p []byte) (n int, err error) {
	return objectBlock.data.Write(p)
}

func (objectBlock *ObjectBlock) WriteByte(c byte) (err error) {
	return objectBlock.data.WriteByte(c)
}

func (objectBlock *ObjectBlock) Size() int {
	return objectBlock.data.Len()
}

func (objectBlock *ObjectBlock) Reset() {
	objectBlock.Length = 0
	objectBlock.data.Reset()
}

func ReadObjectBlock(reader Reader, block *ObjectBlock, expectedSync [16]byte) (ok bool, err error) {
	err = ReadLong(reader, &block.Length)
	if err != nil {
		if err == io.EOF {
			ok = false
			err = nil
		}
		return
	}
	block.data.Reset()
	err = ReadBytesIntoBuffer(reader, &block.data)
	if err != nil {
		return
	}
	var sync [16]byte
	_, err = io.ReadFull(reader, sync[:])
	if err != nil {
		return
	}
	if !bytes.Equal(sync[:], expectedSync[:]) {
		err = errors.New("invalid sync")
		return
	}
	ok = true
	return
}

func WriteObjectBlock(writer io.Writer, block *ObjectBlock, sync [16]byte) (nWritten int, err error) {
	n, err := WriteLong(writer, block.Length)
	nWritten += n
	if err != nil {
		return
	}
	n, err = WriteBytes(writer, block.data.Bytes())
	nWritten += n
	if err != nil {
		return
	}
	n, err = writer.Write(sync[:])
	nWritten += n
	if err != nil {
		return
	}
	return
}

func NewCodecWriter(writer io.Writer, codec CompressionCodec) (io.WriteCloser, error) {
	switch codec {
	case CompressionCodecNull:
		return &NoOpCloser{writer}, nil
	case CompressionCodecDeflate:
		return zlib.NewWriter(writer), nil
	case CompressionCodecSnappy:
		return snappy.NewBufferedWriter(writer), nil
	default:
		return nil, errors.New("unknown compression codec")
	}
}
