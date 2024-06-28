package internal

import "io"

type EncodeFunc func(writer io.Writer, v interface{}) error
