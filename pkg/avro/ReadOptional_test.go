package avro_test

import (
	"bytes"
	"testing"

	"github.com/Ryan-A-B/avro-go/pkg/avro"
	. "github.com/smartystreets/goconvey/convey"
)

func TestReadOptional(t *testing.T) {
	Convey("TestReadOptional", t, func() {
		Convey("ReadOptionalBoolean", func() {
			Convey("present", func() {
				Convey("true", func() {
					reader := bytes.NewReader([]byte{1, 1})
					var value *bool
					err := avro.ReadOptionalBoolean(reader, &value)
					So(err, ShouldBeNil)
					So(*value, ShouldEqual, true)
				})
				Convey("false", func() {
					reader := bytes.NewReader([]byte{1, 0})
					var value *bool
					err := avro.ReadOptionalBoolean(reader, &value)
					So(err, ShouldBeNil)
					So(*value, ShouldEqual, false)
				})
			})
			Convey("not present", func() {
				reader := bytes.NewReader([]byte{0})
				var value *bool
				err := avro.ReadOptionalBoolean(reader, &value)
				So(err, ShouldBeNil)
				So(value, ShouldBeNil)
			})
		})
		Convey("ReadOptional", func() {
			Convey("present", func() {
				reader := bytes.NewReader([]byte{1, 16, 74, 111, 104, 110, 32, 68, 111, 101, 84})
				var value *Person
				err := avro.ReadOptional(reader, func() error {
					value = new(Person)
					return value.ReadAvro(reader)
				})
				So(err, ShouldBeNil)
				So(*value, ShouldResemble, Person{
					Name: "John Doe",
					Age:  42,
				})
			})
			Convey("not present", func() {
				reader := bytes.NewReader([]byte{0})
				var value *Person
				err := avro.ReadOptional(reader, func() error {
					value = new(Person)
					return value.ReadAvro(reader)
				})
				So(err, ShouldBeNil)
				So(value, ShouldBeNil)
			})
		})
	})
}
