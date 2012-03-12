// -*- mode: go; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-

package mpack

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
)

/*
type FullReader interface {
	io.ByteReader
	io.Reader
}
*/

type PackReader struct {
	reader io.Reader
	framed bool
}

func NewPackReader(r io.Reader, f bool) *PackReader {
	result := new(PackReader)
	result.reader = r
	result.framed = f
	return result
}

func (pr PackReader) ReadBinary(result interface{}) error {
	return binary.Read(pr.reader, binary.BigEndian, result)
}

func (pr PackReader) ReadUint32() (tmp uint32, e error) {
	e = pr.ReadBinary(&tmp)
    return tmp, e
}

func (pr PackReader) ReadUint8() (tmp uint8, e error) {
    e = pr.ReadBinary(&tmp)
    return tmp, e
}

func (pr PackReader) unpackRaw(length uint32, prefixBytes int) (interface{}, int, error) {
	if length == 0 {
		// lenght == 0 => nil...
		return nil, 0, nil
	}
	numRead := prefixBytes
	data := make([]byte, length)
	n, err := pr.reader.Read(data)
	numRead += n
	if err != nil {
		return nil, numRead, err
	}
	return data, numRead, nil
}

func (pr PackReader) unpackArray(length uint32, prefixBytes int) (interface{}, int, error) {
	numRead := prefixBytes
	data := make([]interface{}, length)
	for i := uint32(0); i < length; i++ {
		elt, n, err := pr.unpack()
		numRead += n
		if err != nil {
			return nil, numRead, err
		}
		data[i] = elt
	}
	return data, numRead, nil
}

func (pr PackReader) unpackMap(length uint32, prefixBytes int) (interface{}, int, error) {
	numRead := prefixBytes

	m := make(map[interface{}]interface{})

	for i := uint32(0); i < length; i++ {
		key, n, err := pr.unpack()
		numRead += n
		if err != nil {
			return nil, numRead, err
		}

		val, n, err := pr.unpack()
		numRead += n
		if err != nil {
			return nil, numRead, err
		}

		if reflect.TypeOf(key).String() == "[]uint8" {
			m[string(key.([]uint8))] = val
		} else {
			m[key] = val
		}
	}

	return m, numRead, nil
}

func (pr PackReader) unpack() (interface{}, int, error) {

	numRead := 0
	frame := 0;
    var b uint8;
    var err error;

    var iRes interface{} = nil;

	// Threaded implementation won't need to worry, so we can
	// just throw the framing away....
    var ok bool = true;
	if pr.framed {
        var tmp uint32
        tmp, err = pr.ReadUint32()
        if err != nil {
            ok = false;
        } else {
            frame = int(tmp);
        }
	}

    if ok {
	    b, err = pr.ReadUint8()
	    if err != nil {
            ok = false;
        }
    }

    doswitch := false

    if ok {

		numRead += 1

		// how is this possible?
		if b < 0 {
			return nil, numRead, nil
		} else if b < positive_fix_max {
			return b, numRead, nil
		} else if b >= negative_fix_min && b <= negative_fix_max {
			return (b & negative_fix_mask) - negative_fix_offset, numRead, nil
		} else if b >= type_fix_raw && b <= type_fix_raw_max {
			return pr.unpackRaw(uint32(b&fix_raw_count_mask), numRead)
		} else if b >= type_fix_array_min && b <= type_fix_array_max {
			return pr.unpackArray(uint32(b&fix_array_count_mask), numRead)
		} else if b >= type_fix_map_min && b <= type_fix_map_max {
			return pr.unpackMap(uint32(b&fix_map_count_mask), numRead)
		} else {
            doswitch = true;
        }
    }

    if ok && doswitch {

        switch b {
		case type_nil:
			return nil, numRead, nil
		case type_false:
			return false, numRead, nil
		case type_true:
			return true, numRead, nil
		case type_uint8:
			b, err := pr.ReadUint8();
			if err != nil {
				return nil, numRead + 1, err
			}
			return b, numRead + 1, nil
		case type_uint16:
			var result uint16
			err := pr.ReadBinary(&result)
			if err != nil {
				return nil, numRead + 2, err
			}
			return result, numRead + 2, nil
		case type_uint32:
			var result uint32
			err := pr.ReadBinary(&result)
			if err != nil {
				return nil, numRead + 4, err
			}
			return result, numRead + 4, nil
		case type_uint64:
			var result uint64
			err := pr.ReadBinary(&result)
			if err != nil {
				return nil, numRead + 8, err
			}
			return result, numRead + 8, nil
		case type_int16:
			var result int16
			err := pr.ReadBinary(&result)
			if err != nil {
				return nil, numRead + 2, err
			}
			return result, numRead + 2, nil
		case type_int32:
			var result int32
			err := pr.ReadBinary(&result)
			if err != nil {
				return nil, numRead + 4, err
			}
			return result, numRead + 4, nil
		case type_int64:
			var result int64
			err := pr.ReadBinary(&result)
			if err != nil {
				return nil, numRead + 8, err
			}
			return result, numRead + 8, nil
		case type_float:
			var result float32
			err := pr.ReadBinary(&result)
			if err != nil {
				return nil, numRead + 4, err
			}
			return result, numRead + 4, nil
		case type_double:
			var result float64
			err := pr.ReadBinary(&result)
			if err != nil {
				return nil, numRead + 8, err
			}
			return result, numRead + 8, nil
		case type_raw16:
			var length uint16
			err := pr.ReadBinary(&length)
			numRead += 2
			if err != nil {
				return nil, numRead, err
			}
			return pr.unpackRaw(uint32(length), numRead)
		case type_raw32:
			var length uint32
			err := pr.ReadBinary(&length)
			numRead += 4
			if err != nil {
				return nil, numRead, err
			}
			return pr.unpackRaw(length, numRead)
		case type_array16:
			var length uint16
			err := pr.ReadBinary(&length)
			numRead += 2
			if err != nil {
				return nil, numRead, err
			}
			return pr.unpackArray(uint32(length), numRead)
		case type_array32:
			var length uint32
			err := pr.ReadBinary(&length)
			numRead += 2
			if err != nil {
				return nil, numRead, err
			}
			return pr.unpackArray(length, numRead)
		case type_map16:
			var length uint16
			err := pr.ReadBinary(&length)
			numRead += 2
			if err != nil {
				return nil, numRead, err
			}
			return pr.unpackMap(uint32(length), numRead)
		case type_map32:
			var length uint32
			err := pr.ReadBinary(&length)
			numRead += 4
			if err != nil {
				return nil, numRead, err
			}
			return pr.unpackMap(length, numRead)
		default:
			fmt.Printf("unhandled type prefix: %x\n", b)
		}
		return b, numRead, nil
	}

    if pr.framed && err == nil && numRead != frame {
        fmt.Printf ("bad frame value: %d v %d", numRead, frame);
    }

    return iRes, numRead, err
}
	
