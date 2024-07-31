package avro

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"

	"github.com/Ryan-A-B/avro-go/avroschema"
)

var expectedMagic = [4]byte{0x4f, 0x62, 0x6a, 0x01}

type ObjectContainerHeader struct {
	Meta map[string][]byte `avro:"meta"`
	Sync [16]byte          `avro:"sync"`
}

type NewObjectContainerHeaderInput struct {
	Schema           avroschema.Schema
	CompressionCodec CompressionCodec
}

type CompressionCodec string

const (
	CompressionCodecNull    CompressionCodec = "null"
	CompressionCodecDeflate CompressionCodec = "deflate"
	CompressionCodecSnappy  CompressionCodec = "snappy"
)

func NewObjectContainerHeader(input NewObjectContainerHeaderInput) *ObjectContainerHeader {
	meta := make(map[string][]byte)
	schema, err := json.Marshal(input.Schema)
	if err != nil {
		panic(err)
	}
	meta["avro.schema"] = schema
	meta["avro.codec"] = []byte(input.CompressionCodec)
	return &ObjectContainerHeader{
		Meta: meta,
		Sync: GenerateSync(),
	}
}

func GenerateSync() (sync [16]byte) {
	_, err := rand.Read(sync[:])
	if err != nil {
		panic(err)
	}
	return
}

func (header *ObjectContainerHeader) ReadAvro(reader Reader) (err error) {
	var magic [4]byte
	_, err = io.ReadFull(reader, magic[:])
	if err != nil {
		return
	}
	if !bytes.Equal(magic[:], expectedMagic[:]) {
		err = errors.New("invalid magic bytes")
		return
	}
	err = ReadBytesMap(reader, &header.Meta)
	if err != nil {
		return
	}
	if _, ok := header.Meta["avro.schema"]; !ok {
		err = errors.New("missing avro.schema")
		return
	}
	_, err = io.ReadFull(reader, header.Sync[:])
	if err != nil {
		return
	}
	return
}

func (header *ObjectContainerHeader) WriteAvro(writer io.Writer) (err error) {
	_, err = writer.Write(expectedMagic[:])
	if err != nil {
		return
	}
	_, err = WriteBytesMap(writer, header.Meta)
	if err != nil {
		return
	}
	_, err = writer.Write(header.Sync[:])
	if err != nil {
		return
	}
	return
}
