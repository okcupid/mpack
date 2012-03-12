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
    offset uint64 
}

func NewPackReader(r io.Reader, f bool) *PackReader {
	result := new(PackReader)
	result.reader = r
	result.framed = f
    result.offset = 0
	return result
}

func (pr PackReader) incOffset (i int) {
    pr.offset += uint64(i);
}

func (pr PackReader) ReadBinary(result interface{}) error {
	e := binary.Read(pr.reader, binary.BigEndian, result)
    if e == nil {
        pr.incOffset (binary.Size(result));
    }
    return e
}

func (pr PackReader) ReadUint8() (tmp uint8, e error)   { e = pr.ReadBinary(&tmp); return }
func (pr PackReader) ReadUint32() (tmp uint32, e error) { e = pr.ReadBinary(&tmp); return }
func (pr PackReader) ReadUint16() (tmp uint16, e error) { e = pr.ReadBinary(&tmp); return }
func (pr PackReader) ReadUint64() (tmp uint64, e error) { e = pr.ReadBinary(&tmp); return }
func (pr PackReader) ReadInt16() (tmp int16, e error)   { e = pr.ReadBinary(&tmp); return }
func (pr PackReader) ReadInt32() (tmp int32, e error)   { e = pr.ReadBinary(&tmp); return }
func (pr PackReader) ReadInt64() (tmp uint64, e error)  { e = pr.ReadBinary(&tmp); return }

func (pr PackReader) ReadFloat32() (tmp float32, e error)  { e = pr.ReadBinary(&tmp); return }
func (pr PackReader) ReadFloat64() (tmp float64, e error)  { e = pr.ReadBinary(&tmp); return }

func (pr PackReader) unpackRaw(length uint32) (res []byte, e error) {

    e = nil
    res = nil
    var n int
    
	if length > 0 {
	    res = make([]byte, length)
	    n, e = pr.reader.Read(res)
        if e != nil {
            pr.incOffset(n);
            res = nil
        }
    }
    return 
}

func (pr PackReader) unpackArray(length uint32) (res []interface{}, e error) {

    res = nil
    e = nil

	res = make([]interface{}, length)
	for i := uint32(0); e == nil && i < length; i++ {
        var elt interface{}
		elt,_,e = pr.unpack()
		if e == nil {
		    res[i] = elt
        }
	}
	return
}

func (pr PackReader) unpackMap(length uint32) (res map[interface{}]interface{}, err error) {

	res = make(map[interface{}]interface{})
    err = nil

	for i := uint32(0); err == nil && i < length; i++ {
        var key, val interface{}

		key, _, err = pr.unpack()
		if err == nil {
		    val, _, err = pr.unpack()
        }

        if err != nil { 
            /* noop */ 
        } else if reflect.TypeOf(key).String() == "[]uint8" {
			res[string(key.([]uint8))] = val
		} else {
			res[key] = val
        }
	}

	return res, err
}

func (pr PackReader) unpack() (interface{}, int, error) {

	frame := 0;
    var b uint8;
    var err error = nil;
    var start uint64 = pr.offset;

    var iRes interface{} = nil;

	// Threaded implementation won't need to worry, so we can
	// just throw the framing away....
	if pr.framed {
        var tmp uint32
        tmp, err = pr.ReadUint32()
        if err == nil {
            frame = int(tmp);
        }
	}

    if err != nil {
	    b, err = pr.ReadUint8()
    }

    doswitch := false

    if err != nil {

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
            doswitch = true;
        }
    }

    if doswitch && err == nil {

        switch b {
		case type_nil:
		case type_false:
            iRes = false;
		case type_true:
            iRes = true;
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
            iRes, err = pr.ReadInt32();
		case type_int64:
            iRes, err = pr.ReadInt64();
		case type_float:
            iRes, err = pr.ReadFloat32();
		case type_double:
            iRes, err = pr.ReadFloat64();
		case type_raw16:
			var length uint16
            length, err = pr.ReadUint16();
			if err == nil {
			    iRes, err = pr.unpackRaw(uint32(length));
            }
		case type_raw32:
			var length uint32
            length, err = pr.ReadUint32()
			if err == nil {
			    iRes, err = pr.unpackRaw(length)
            }
		case type_array16:
			var length uint16
			length, err := pr.ReadUint16();
			if err == nil {
			    iRes, err = pr.unpackArray(uint32(length));
            }
		case type_array32:
			var length uint32
            length, err = pr.ReadUint32();
			if err == nil {
			    iRes, err= pr.unpackArray(length);
            }
		case type_map16:
			var length uint16
			length, err = pr.ReadUint16();
			if err == nil {
			    iRes, err = pr.unpackMap(uint32(length));
            }
		case type_map32:
			var length uint32
			length, err = pr.ReadUint32();
			if err == nil {
			    iRes, err = pr.unpackMap(length)
            }
		default:
			fmt.Printf("unhandled type prefix: %x\n", b)
		}
	}

    end := pr.offset;
    numRead := int(start - end);

    if pr.framed && err == nil && numRead != frame {
        fmt.Printf ("bad frame value: %d v %d", numRead, frame);
    }

    return iRes, numRead, err
}
	
