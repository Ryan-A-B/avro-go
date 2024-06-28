package internal

import "io"

type DecodeFunc func(reader Reader, v interface{}) error

type Reader interface {
	io.Reader
	io.ByteReader
}
