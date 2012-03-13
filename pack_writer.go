// -*- mode: go; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-

package mpack
import (
    "encoding/binary"
    "fmt"
    "io"
    "reflect"
    "buffer"
    )


type PackWriter struct {
    writer io.Writer
    sink io.Writer
    offset uint64
    framed bool
}

func NewPackWriter(writer io.Writer, framed bool) *PackWriter {
    result := new(PackWriter)
    if framed {
        result.writer = *buffer.NewBuffer(make([]bytes, 0, 4196));
        result.sink = writer
    } else {
        result.writer = writer
        result.sink = writer
    }
    result.framed = framed
    return result
}

func (pr PackWriter) incOffset(i int) { pr.offset += uint64(i) }



func (pr PackWriter) expectFraming() bool {
    return pr.framed && pr.offset == 0
}

func (pw PackWriter) writeCode(code byte) (int, error) {
    return pw.writer.Write([]byte{code})
}

func (pw PackWriter) writeByte(b byte) (int, error) {
    return pw.writer.Write([]byte{b})
}

func (pw PackWriter) writeBinary(n interface{}) error {
    return binary.Write(pw.writer, binary.BigEndian, n)
}

func (pw PackWriter) writeBlock(code byte, n interface{}, bytes int) (int, error) {
    total, err := pw.writeCode(code)
    if err != nil {
        return 0, err
    }
    err = pw.writeBinary(n)
    if err != nil {
        return total, err
    }
    total += bytes
    return total, nil
}

func (pw PackWriter) packUint8(n uint8) (int, error) {
    if n < 128 {
        return pw.writeByte(n)
    }
    return pw.writer.Write([]byte{type_uint8, byte(n)})
}

func (pw PackWriter) packByte(n byte) (int, error) {
    return pw.packUint8(n)
}

func (pw PackWriter) packUint16(n uint16) (int, error) {
    if n < 0x100 {
        return pw.packUint8(uint8(n))
    }
    return pw.writeBlock(type_uint16, n, 2)
}

func (pw PackWriter) packUint32(n uint32) (int, error) {
    if n < 65536 {
        return pw.packUint16(uint16(n))
    }
    return pw.writeBlock(type_uint32, n, 4)
}

func (pw PackWriter) packUint64(n uint64) (int, error) {
    if n < 4294967296 {
        return pw.packUint32(uint32(n))
    }
    return pw.writeBlock(type_uint64, n, 8)
}

func (pw PackWriter) packPositiveInt(u uint64) (e error) {
    if u <= 0x7f {
        e = pw.packByte(u)
    } else if u <= 0xff {
        pw.packByte(byte(type_uint8))
        e = pw.packBinary(uint8(u));
    } else if u <= 0xffff {
        pw.packByte(byte(type_uint16));
        e = pw.packBinary(uint16(u));
    } else if u <= 0xffffffff  {
        pw.packByte(byte(type_uint32));
        e = pw.packBinary(uint32(u));
    } else {
        pw.packByte(type_uint64);
        e = pw.packBinary(u);
    }
    return
}

func (pw PackWriter) packNegativeInt(i int64) (e error) {
    if i >= -32 {
        e = pw.packByte(i);
    } else if i >= -128 {
        pw.packByte(type_int8);
        e = pw.packBinary(int8(i))
    } else if i >= -32768 {
        pw.packByte(type_int16);
        e = pw.packBinary(int16(i))
    } else if i >= -2147483648 {
        pw.packByte(type_int32);
        e = pw.packBinary (int32(i))
    } else {
        pw.packByte (type_int64)
        e = pw.packBinary (int64 (i))
    }
    return
}

//
// Pack a length. If it's a small length, OR in the 's' mask.
// If it's a medium length, pack out the short prefix 's'.
// If it's a long length, pack out the big prefix 'b'
func (pw PackWriter) packLen(l int, s uint8, m uint8, b uint8) (e error) {
    if l <= 0x1f {
        l |= int(s)
        e = pw.packByte (l)
    } else if l <= 0xffff {
        pw.packByte(m);
        e = pw.packBinary(uint16(l))
    } else {
        pw.packByte(b);
        e = pw.packBinary(uint32(l))
    }
    return e
}

func (pw PackWriter) packBinary(v interface{}) error {
    e := binary.Write(pr.writer, binary.BigEndian, v)
    if e != nil {
        pr.incOffset (binary.Size(v));
    }
    return e;
}


func (pw PackWriter) packInt16(n int16) (int, error) {
    if n >= -128 && n <= 127 {
        return pw.packInt8(int8(n))
    }
    return pw.writeBlock(type_int16, n, 2)
}

func (pw PackWriter) packInt32(n int32) (int, error) {
    if n >= -32768 && n <= 32767 {
        return pw.packInt16(int16(n))
    }
    return pw.writeBlock(type_int32, n, 4)
}

func (pw PackWriter) packInt64(n int64) (int, error) {
    if n >= -2147483648 && n <= 2147483647 {
        return pw.packInt32(int32(n))
    }
    return pw.writeBlock(type_int64, n, 8)
}

func (pw PackWriter) packNil() (int, error) {
    return pw.writeCode(type_nil)
}

func (pw PackWriter) packBool(v bool) (int, error) {
    b := type_true
    if v == false {
        b = type_false
    }
    return pw.writeByte(b)
}

func (pw PackWriter) packFloat32(n float32) (int, error) {
    return pw.writeBlock(type_float, n, 4)
}

func (pw PackWriter) packFloat64(n float64) (int, error) {
    return pw.writeBlock(type_double, n, 8)
}

func (pw PackWriter) packBytes(b []byte) (int, error) {
    if len(b) < 32 {
        pw.writeByte(type_fix_raw | uint8(len(b)))
        pw.writer.Write(b)
        return 1 + len(b), nil
    } else if len(b) < 65536 {
        pw.writeCode(type_raw16)
        var length uint16 = uint16(len(b))
        pw.writeBinary(length)
        pw.writer.Write(b)
        return 3 + len(b), nil
    } else if uint32(len(b)) <= uint32(4294967295) {
        pw.writeCode(type_raw32)
        length := uint32(len(b))
        pw.writeBinary(length)
        pw.writer.Write(b)
        return 5 + len(b), nil
    }
    return 0, nil
}

func (pw PackWriter) packString(s string) (int, error) {
    return pw.packBytes([]byte(s))
}

func (pw PackWriter) packInt64Array(a []int64) (int, error) {
    numBytes := 0
    if len(a) < 16 {
        n, err := pw.writeCode(type_fix_array_min | uint8(len(a)))
        if err != nil {
            return numBytes, err
        }
        numBytes += n
    } else if len(a) < 65536 {
        n, err := pw.writeCode(type_array16)
        if err != nil {
            return numBytes, err
        }
        numBytes += n
        length := uint16(len(a))
        err = pw.writeBinary(length)
        if err != nil {
            return numBytes, err
        }
        numBytes += 2
    } else if uint32(len(a)) <= uint32(4294967295) {
        n, err := pw.writeCode(type_array32)
        if err != nil {
            return numBytes, err
        }
        numBytes += n
        length := uint32(len(a))
        err = pw.writeBinary(length)
        if err != nil {
            return numBytes, err
        }
        numBytes += 4
    }

    for i := 0; i < len(a); i++ {
        n, err := pw.packInt64(a[i])
        if err != nil {
            return numBytes, err
        }
        numBytes += n
    }
    return numBytes, nil
}

func (pw PackWriter) packArray(a reflect.Value) (int, error) {
    numBytes := 0
    if a.Len() < 16 {
        n, err := pw.writeCode(type_fix_array_min | uint8(a.Len()))
        if err != nil {
            return numBytes, err
        }
        numBytes += n
    } else if a.Len() < 65536 {
        n, err := pw.writeCode(type_array16)
        if err != nil {
            return numBytes, err
        }
        numBytes += n
        length := uint16(a.Len())
        err = pw.writeBinary(length)
        if err != nil {
            return numBytes, err
        }
        numBytes += 2
    } else if uint32(a.Len()) <= uint32(4294967295) {
        n, err := pw.writeCode(type_array32)
        if err != nil {
            return numBytes, err
        }
        numBytes += n
        length := uint32(a.Len())
        err = pw.writeBinary(length)
        if err != nil {
            return numBytes, err
        }
        numBytes += 4
    }
    for i := 0; i < a.Len(); i++ {
        elt := a.Index(i)
        n, err := pw.pack(elt.Interface())
        if err != nil {
            return numBytes, err
        }
        numBytes += n
    }
    return numBytes, nil
}

func (pw PackWriter) packMap(m reflect.Value) (int, error) {
    numBytes := 0

    if m.Len() < 16 {
        n, err := pw.writeCode(type_fix_map_min | uint8(m.Len()))
        if err != nil {
            return numBytes, err
        }
        numBytes += n
    } else if m.Len() < 65536 {
        n, err := pw.writeCode(type_map16)
        if err != nil {
            return numBytes, err
        }
        numBytes += n
        length := uint16(m.Len())
        err = pw.writeBinary(length)
        if err != nil {
            return numBytes, err
        }
        numBytes += 2
    } else if uint32(m.Len()) <= uint32(4294967295) {
        n, err := pw.writeCode(type_map32)
        if err != nil {
            return numBytes, err
        }
        numBytes += n
        length := uint32(m.Len())
        err = pw.writeBinary(length)
        if err != nil {
            return numBytes, err
        }
        numBytes += 4
    }

    keys := m.MapKeys()
    for i := 0; i < len(keys); i++ {
        n, err := pw.pack(keys[i].Interface())
        if err != nil {
            return numBytes, err
        }
        numBytes += n
        n, err = pw.pack(m.MapIndex(keys[i]).Interface())
        if err != nil {
            return numBytes, err
        }
        numBytes += n
    }

    return numBytes, nil
}

func (pw PackWriter) pack(value interface{}) (e error) {

    var packed bool = false
    if value == nil {
        pw.packNil()
        packed = true;
    }

    if !packed {
        packed = true
        switch tvalue := value.(type) {
        case int8:
            e = pw.packInt8(tvalue)
        case int16:
            e = pw.packInt16(tvalue)
        case int32:
            e  = pw.packInt32(tvalue)
        case int64:
            e = pw.packInt64(tvalue)
        case int:
            e = pw.packInt64(int64(tvalue))
        case uint8:
            e = pw.packUint8(tvalue)
        case uint16:
            e = pw.packUint16(tvalue)
        case uint32:
            e = pw.packUint32(tvalue)
        case uint64:
            e = pw.packUint64(tvalue)
        case uint:
            e = pw.packUint64(uint64(tvalue))
        case bool:
            e = pw.packBool(tvalue)
        case float32:
            e = pw.packFloat32(tvalue)
        case float64:
            e = pw.packFloat64(tvalue)
        case []byte:
            e = pw.packBytes(tvalue)
        case []int64:
            e = pw.packInt64Array(tvalue)
        case string:
            e = pw.packString(tvalue)
        default:
            packed = false
        }
    }
    
    if !packed {

        // see if it is an array...
        rvalue := reflect.ValueOf(value)
        if rvalue.Kind() == reflect.Array || rvalue.Kind() == reflect.Slice {
            e = pw.packArray(rvalue)
            packed = true;
        }
        // is it a map?
        else if rvalue.Kind() == reflect.Map {
            e = pw.packMap(rvalue)
            packed = true
        }
    }

    if !packed {
        e = errors.New ("unknown type: %s\n", value)
    }

    return e
}
