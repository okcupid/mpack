package mpack

import (
	"encoding/binary"
	"github.com/okcupid/jsonw"
	"io"
	"log"
	"errors"
	"fmt"
)

type PackReader struct {
	reader io.Reader
	framed bool
	offset int
	first  bool
}

func (pr *PackReader) expectFraming() bool {
	return pr.framed && pr.first
}

func NewPackReader(r io.Reader, f bool) *PackReader {
	result := new(PackReader)
	result.reader = r
	result.framed = f
	result.offset = 0
	result.first = true
	return result
}

func (pr *PackReader) incOffset(i int) {
	pr.offset += i
}

func (pr *PackReader) ReadBinary(result interface{}) error {
	e := binary.Read(pr.reader, binary.BigEndian, result)
	if e == nil {
		pr.incOffset(binary.Size(result))
	}
	return e
}

func (pr *PackReader) ReadUint8() (tmp uint8, e error)   { e = pr.ReadBinary(&tmp); return }
func (pr *PackReader) ReadUint32() (tmp uint32, e error) { e = pr.ReadBinary(&tmp); return }
func (pr *PackReader) ReadUint16() (tmp uint16, e error) { e = pr.ReadBinary(&tmp); return }
func (pr *PackReader) ReadUint64() (tmp uint64, e error) { e = pr.ReadBinary(&tmp); return }
func (pr *PackReader) ReadInt16() (tmp int16, e error)   { e = pr.ReadBinary(&tmp); return }
func (pr *PackReader) ReadInt32() (tmp int32, e error)   { e = pr.ReadBinary(&tmp); return }
func (pr *PackReader) ReadInt64() (tmp uint64, e error)  { e = pr.ReadBinary(&tmp); return }

func (pr *PackReader) ReadFloat32() (tmp float32, e error) { e = pr.ReadBinary(&tmp); return }
func (pr *PackReader) ReadFloat64() (tmp float64, e error) { e = pr.ReadBinary(&tmp); return }

func (pr *PackReader) unpackRaw(length uint32) (res string, e error) {

	e = nil
	var n int

	if length > 0 {
		bytes := make([]byte, length)
		n, e = pr.reader.Read(bytes)
		if e == nil {
			pr.incOffset(n)
			res = string(bytes)
		}
	}
	return
}

func (pr *PackReader) unpackArray(length uint32) (res []interface{}, e error) {

	res = nil
	e = nil

	res = make([]interface{}, length)
	for i := uint32(0); e == nil && i < length; i++ {
		var elt interface{}
		elt, _, e = pr.unpack()
		if e == nil {
			res[i] = elt
		}
	}
	return
}

func (pr *PackReader) unpackMap(length uint32) (res map[string]interface{}, err error) {

	res = make(map[string]interface{})
	err = nil

	for i := uint32(0); err == nil && i < length; i++ {
		var key, val interface{}

		key, _, err = pr.unpack()
		if err == nil {
			val, _, err = pr.unpack()
		}

		if err != nil {
			/* noop */
		} else if s, ok := key.(string); ok {
			res[s] = val
		} else {
			msg := fmt.Sprintf ("non-string keys for maps not allowed (%s)", key)
			err = errors.New(msg)
		}
	}

	return res, err
}

func (pr *PackReader) unpack() (interface{}, int, error) {

	frame := 0
	frame_len := 0
	var b uint8
	var err error = nil

	var iRes interface{} = nil

	var framed bool = pr.expectFraming()

	// Threaded implementation won't need to worry, so we can
	// just throw the framing away....
	if framed {
		pr.first = false
		var tmp interface{}
		tmp, frame_len, err = pr.unpack()
		if err == nil {
			w := jsonw.NewWrapper(tmp)
			var i int64
			i, err = w.GetInt()
			if err == nil {
				frame = int(i)
				// Reset the offset so that we start counting from the
				// inside of the frame
				pr.offset = 0
			}
		}
	}

	if err == nil {
		b, err = pr.ReadUint8()
	}

	doswitch := false

	if err == nil {

		if b < positive_fix_max {
			iRes = b
		} else if b >= negative_fix_min && b <= negative_fix_max {
			iRes = (b & negative_fix_mask) - negative_fix_offset

		} else if b >= type_fix_raw && b <= type_fix_raw_max {
			iRes, err = pr.unpackRaw(uint32(b & fix_raw_count_mask))

		} else if b >= type_fix_array_min && b <= type_fix_array_max {
			iRes, err = pr.unpackArray(uint32(b & fix_array_count_mask))

		} else if b >= type_fix_map_min && b <= type_fix_map_max {
			iRes, err = pr.unpackMap(uint32(b & fix_map_count_mask))

		} else {
			doswitch = true
		}
	}

	if doswitch && err == nil {

		switch b {
		case type_nil:
		case type_false:
			iRes = false
		case type_true:
			iRes = true
		case type_uint8:
			iRes, err = pr.ReadUint8()
		case type_uint16:
			iRes, err = pr.ReadUint16()
		case type_uint32:
			iRes, err = pr.ReadUint32()
		case type_uint64:
			iRes, err = pr.ReadUint64()
		case type_int16:
			iRes, err = pr.ReadInt16()
		case type_int32:
			iRes, err = pr.ReadInt32()
		case type_int64:
			iRes, err = pr.ReadInt64()
		case type_float:
			iRes, err = pr.ReadFloat32()
		case type_double:
			iRes, err = pr.ReadFloat64()
		case type_raw16:
			var length uint16
			length, err = pr.ReadUint16()
			if err == nil {
				iRes, err = pr.unpackRaw(uint32(length))
			}
		case type_raw32:
			var length uint32
			length, err = pr.ReadUint32()
			if err == nil {
				iRes, err = pr.unpackRaw(length)
			}
		case type_array16:
			var length uint16
			length, err := pr.ReadUint16()
			if err == nil {
				iRes, err = pr.unpackArray(uint32(length))
			}
		case type_array32:
			var length uint32
			length, err = pr.ReadUint32()
			if err == nil {
				iRes, err = pr.unpackArray(length)
			}
		case type_map16:
			var length uint16
			length, err = pr.ReadUint16()
			if err == nil {
				iRes, err = pr.unpackMap(uint32(length))
			}
		case type_map32:
			var length uint32
			length, err = pr.ReadUint32()
			if err == nil {
				iRes, err = pr.unpackMap(length)
			}
		default:
			log.Printf("unhandled type prefix: %x\n", b)
		}
	}

	numRead := pr.offset

	if framed && err == nil && numRead != frame {
		log.Printf("bad frame value: %d v %d", numRead, frame)
	}

	return iRes, numRead + frame_len, err
}
