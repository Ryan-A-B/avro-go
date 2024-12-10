package avro_test

import (
	"bytes"
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/Ryan-A-B/avro-go/pkg/avro"
)

func TestWriteOptional(t *testing.T) {
	Convey("TestWriteOptional", t, func() {
		var buffer bytes.Buffer
		Convey("WriteOptionalBoolean", func() {
			Convey("present", func() {
				value := true
				n, err := avro.WriteOptionalBoolean(&buffer, &value)
				So(n, ShouldEqual, 2)
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{1, 1})
			})
			Convey("not present", func() {
				n, err := avro.WriteOptionalBoolean(&buffer, nil)
				So(n, ShouldEqual, 1)
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{0})
			})
		})
		Convey("WriteOptionalDouble", func() {
			Convey("present", func() {
				value := 1.0
				n, err := avro.WriteOptionalDouble(&buffer, &value)
				So(n, ShouldEqual, 9)
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{1, 0, 0, 0, 0, 0, 0, 240, 63})

			})
			Convey("not present", func() {
				n, err := avro.WriteOptionalDouble(&buffer, nil)
				So(n, ShouldEqual, 1)
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{0})
			})
		})
		Convey("WriteOptional", func() {
			Convey("present", func() {
				value := Person{
					Name: "John Doe",
					Age:  42,
				}
				n, err := avro.WriteOptional(&buffer, &value)
				So(n, ShouldEqual, 11)
				So(err, ShouldBeNil)
				So(buffer.Bytes(), ShouldResemble, []byte{1, 16, 74, 111, 104, 110, 32, 68, 111, 101, 84})
			})
			Convey("not present", func() {
				Convey("nil", func() {
					n, err := avro.WriteOptional(&buffer, nil)
					fmt.Println("buffer", buffer.Bytes())
					So(n, ShouldEqual, 1)
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0})
				})
				Convey("value", func() {
					var value *Person
					n, err := avro.WriteOptional(&buffer, value)
					fmt.Println("buffer", buffer.Bytes())
					So(n, ShouldEqual, 1)
					So(err, ShouldBeNil)
					So(buffer.Bytes(), ShouldResemble, []byte{0})
				})
			})
		})
	})
}
