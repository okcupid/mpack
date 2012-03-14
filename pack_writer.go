// -*- mode: go; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-

package mpack
import (
    "errors"
    "encoding/binary"
    "io"
    "reflect"
    "bytes"
    )


type PackWriter struct {
    writer io.Writer
    sink io.Writer
    buffer *bytes.Buffer
    offset int
    framed bool
}

const (
    packed_uint32_len = 5
    )

func NewPackWriter(writer io.Writer, framed bool) *PackWriter {
    result := new(PackWriter)
    if !framed {
        result.writer = writer
    } else {

        // If they passed us a Buffer, then just reuse that buffer
        buffer, ok := writer.(*bytes.Buffer)
        
        if !ok {
            // Otherwise, we need to write to a buffer, and then flush
            buffer = bytes.NewBuffer(make([]byte, 0, 4196));
            result.sink = writer
        }  
        result.buffer = buffer
        result.writer = buffer
        
        // Make room for up to 5 bytes of data
        buffer.Write(make([]byte, packed_uint32_len))
    }
    result.offset = 0
    result.framed = framed
    return result
}

func (pr *PackWriter) incOffset(i int) { 
    pr.offset += i 
}


func (pr PackWriter) expectFraming() bool {
    return pr.framed && pr.offset == 0
}

func (pw *PackWriter) packByte(b byte) error {
    return pw.packBinary(b);
}

func (pw *PackWriter) packPositiveInt(u uint64) (e error) {
    if u <= 0x7f {
        e = pw.packByte(byte(u))
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

func (pw *PackWriter) packNegativeInt(i int64) (e error) {
    if i >= -32 {
        e = pw.packByte(byte(i));
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
// If it's a medium length, pack out the medium 16-bit prefix 'm'.
// If it's a big length, pack out the big prefix 'b'
//
func (pw *PackWriter) packLen(l int, s uint8, m uint8, b uint8) (e error) {
    if l <= 0x1f {
        l |= int(s)
        e = pw.packByte (byte(l))
    } else if l <= 0xffff {
        pw.packByte(m);
        e = pw.packBinary(uint16(l))
    } else {
        pw.packByte(b);
        e = pw.packBinary(uint32(l))
    }
    return e
}

func (pw *PackWriter) packBinary(v interface{}) error {
    e := binary.Write(pw.writer, binary.BigEndian, v)
    if e == nil {
        n := binary.Size(v)
        pw.incOffset (n);
    }
    return e;
}

func (pw *PackWriter) packRaw(b []byte) error {
    n,e := pw.writer.Write(b)
    if e == nil {
        pw.incOffset (n);
    }
    return e
}


func (pw *PackWriter) packInt (i int64) (e error) {
    if (i >= 0) {
        e = pw.packPositiveInt(uint64(i))
    } else {
        e = pw.packNegativeInt(i)
    }
    return
}

func (pw *PackWriter) packUint (u uint64) error {
    e := pw.packPositiveInt(u)
    return e
}

func (pw *PackWriter) packNil() error {
    return pw.packByte(byte(type_nil))
}

func (pw *PackWriter) packBool(v bool) error {
    b := type_true
    if v == false {
        b = type_false
    }
    return pw.packByte(b)
}

func (pw *PackWriter) packFloat32(n float32) error {
    pw.packByte(type_float)
    e := pw.packBinary(n)
    return e
}

func (pw *PackWriter) packFloat64(n float64) error {
    pw.packByte(type_double)
    e := pw.packBinary(n)
    return e
}

func (pw *PackWriter) packBytes(b []byte) error {
    l := len(b)
    pw.packLen(l, type_fix_raw, type_raw16, type_raw32)
    e := pw.packRaw(b)
    return e
}

func (pw *PackWriter) packString(s string) error {
    return pw.packBytes([]byte(s))
}

func (pw *PackWriter) packArray(a reflect.Value) error {
    e := pw.packLen(a.Len(), type_fix_array_min, type_array16, type_array32)

    for i := 0; e == nil && i < a.Len(); i++ {
        elt := a.Index(i)
        e = pw.pack(elt.Interface())
    }
    return e
}

func (pw *PackWriter) packMap(m reflect.Value) error {
    e := pw.packLen(m.Len(), type_fix_map_min, type_map16, type_map32)

    keys := m.MapKeys()
    for i := 0; e == nil && i < len(keys); i++ {
        e = pw.pack(keys[i].Interface())
        if e == nil {
            e = pw.pack(m.MapIndex(keys[i]).Interface())
        }
    }
    return e
}

func (pw *PackWriter) pack(value interface{}) (e error) {

    var packed bool = false
    if value == nil {
        pw.packNil()
        packed = true;
    }

    if !packed {
        packed = true
        switch tvalue := value.(type) {

        case int8:
            e = pw.packInt(int64(tvalue))
        case int16:
            e = pw.packInt(int64(tvalue))
        case int32:
            e  = pw.packInt(int64(tvalue))
        case int64:
            e = pw.packInt(int64(tvalue))
        case int:
            e = pw.packInt(int64(tvalue))

        case uint8:
            e = pw.packUint(uint64(tvalue))
        case uint16:
            e = pw.packUint(uint64(tvalue))
        case uint32:
            e = pw.packUint(uint64(tvalue))
        case uint64:
            e = pw.packUint(uint64(tvalue))
        case uint:
            e = pw.packUint(uint64(tvalue))

        case bool:
            e = pw.packBool(tvalue)

        case float32:
            e = pw.packFloat32(tvalue)
        case float64:
            e = pw.packFloat64(tvalue)
        case []byte:
            e = pw.packBytes(tvalue)
        case string:
            e = pw.packString(tvalue)
        default:
            packed = false
        }
    }
    
    if !packed {

        rvalue := reflect.ValueOf(value)
        if rvalue.Kind() == reflect.Array || rvalue.Kind() == reflect.Slice {
            e = pw.packArray(rvalue)
            packed = true;
        } else if rvalue.Kind() == reflect.Map {
            e = pw.packMap(rvalue)
            packed = true
        }
    }

    if !packed {
        e = errors.New ("unknown type given");
    }

    return e
}

func (pw *PackWriter) flush() (n int, e error) {

    if pw.framed {

        nbytes := pw.offset
        tmp := bytes.NewBuffer(make([]byte,0,packed_uint32_len));
        pw.writer = tmp
        pw.offset = 0
        e = pw.packInt(int64(nbytes))
        
        if e == nil {
            frame_len := pw.offset

            // We had allocated up to 5 bytes, but we only needed frame_len
            // bytes.  In this case, we're going to have to offset into the
            // beginning of the buffer by offset many bytes
            offset := packed_uint32_len - frame_len

            // Advance the pointer in the buffer that many bytes (offset)
            // This is the best way I found to throw away offset number of bytes
            pw.buffer.Next(offset)

            // Copy the prefix over.  In go's copy, the
            // destination comes first, and the source second
            copy (pw.buffer.Bytes()[0:frame_len], tmp.Bytes()[0:frame_len]);

            // Reset the offset to be the number of bytes read
            pw.offset = nbytes + frame_len

            // If there's a sink that we need to flush to, do that now.
            // Otherwise, we're ok where we were, since the writer and
            // the buffer were one in the same
            if pw.sink != nil {
                pw.sink.Write(pw.buffer.Bytes())
            }
        } 
    }
    n = pw.offset

    return

}
