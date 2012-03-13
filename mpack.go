// -*- mode: go; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-

package mpack

import (
    "io"
    )

func Pack(w io.Writer, value interface{}) (n int, e error) {
    //	stime := time.Nanoseconds()
    pw := NewPackWriter(w)
    e = pw.pack(value)

    if e == nil {
        n,e = pw.flush()
    }


    //	etime := time.Nanoseconds()
    //	msecs := (float64)(etime-stime) / 1000000
    //	fmt.Printf("pack time: %.3fms\n", msecs)

    return
}

func Unpack(reader io.Reader, framed bool) (interface{}, int, error) {
    pr := NewPackReader(reader, framed)
    return pr.unpack()
}
