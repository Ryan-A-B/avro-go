package avro_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/Ryan-A-B/avro-go/pkg/avro"
)

func BenchmarkReadBoolean(b *testing.B) {
	var buffer bytes.Buffer
	options := [2]byte{0, 1}
	var value bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.WriteByte(options[i%2])
		avro.ReadBoolean(&buffer, &value)
	}
}

func BenchmarkReadInt(b *testing.B) {
	var buffer bytes.Buffer
	nOptions := 5
	options := make([][]byte, nOptions)
	for i := 0; i < nOptions; i++ {
		buffer.Reset()
		avro.WriteInt(&buffer, rand.Int31())
		options[i] = buffer.Bytes()
	}
	var value int32
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Reset()
		buffer.Write(options[i%nOptions])
		avro.ReadInt(&buffer, &value)
	}
}

func BenchmarkReadLong(b *testing.B) {
	var buffer bytes.Buffer
	nOptions := 5
	options := make([][]byte, nOptions)
	for i := 0; i < nOptions; i++ {
		buffer.Reset()
		avro.WriteLong(&buffer, rand.Int63())
		options[i] = buffer.Bytes()
	}
	var value int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Reset()
		buffer.Write(options[i%nOptions])
		avro.ReadLong(&buffer, &value)
	}
}

func BenchmarkReadFloat(b *testing.B) {
	var buffer bytes.Buffer
	nOptions := 5
	options := make([][]byte, nOptions)
	for i := 0; i < nOptions; i++ {
		buffer.Reset()
		avro.WriteFloat(&buffer, rand.Float32())
		options[i] = buffer.Bytes()
	}
	var value float32
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Reset()
		buffer.Write(options[i%nOptions])
		avro.ReadFloat(&buffer, &value)
	}
}

func BenchmarkReadDouble(b *testing.B) {
	var buffer bytes.Buffer
	nOptions := 5
	options := make([][]byte, nOptions)
	for i := 0; i < nOptions; i++ {
		buffer.Reset()
		avro.WriteDouble(&buffer, rand.Float64())
		options[i] = buffer.Bytes()
	}
	var value float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Reset()
		buffer.Write(options[i%nOptions])
		avro.ReadDouble(&buffer, &value)
	}
}

func BenchmarkReadBytes(b *testing.B) {
	var buffer bytes.Buffer
	nOptions := 5
	options := make([][]byte, nOptions)
	for i := 0; i < nOptions; i++ {
		buffer.Reset()
		avro.WriteBytes(&buffer, []byte{byte(i)})
		options[i] = buffer.Bytes()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Reset()
		buffer.Write(options[i%nOptions])
		avro.ReadBytes(&buffer)
	}
}

func BenchmarkReadString(b *testing.B) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var buffer bytes.Buffer
	nOptions := 5
	options := make([][]byte, nOptions)
	for i := 0; i < nOptions; i++ {
		buffer.Reset()
		length := rand.Intn(128)
		data := make([]byte, length)
		for j := 0; j < length; j++ {
			data[j] = charset[rand.Intn(len(charset))]
		}
		avro.WriteString(&buffer, string(data))
		options[i] = buffer.Bytes()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Reset()
		buffer.Write(options[i%nOptions])
		avro.ReadString(&buffer)
	}
}
