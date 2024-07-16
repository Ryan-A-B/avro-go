package avro

import "io"

type Reader interface {
	io.Reader
	io.ByteReader
}

type Writer interface {
	io.Writer
	io.ByteWriter
}
