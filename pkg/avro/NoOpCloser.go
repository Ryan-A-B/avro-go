package avro

import "io"

type NoOpCloser struct {
	io.Writer
}

func (nopCloser *NoOpCloser) Close() (err error) {
	return
}
