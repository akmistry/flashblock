package flashblock

import (
	"io"
)

var (
	zeroBuf = make([]byte, 65536)
)

type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

type offsetReadWriterAt struct {
	backing ReadWriterAt
	offset  int64
}

func NewOffsetReadWriterAt(backing ReadWriterAt, offset int64) ReadWriterAt {
	return &offsetReadWriterAt{
		backing: backing,
		offset:  offset,
	}
}

func (o *offsetReadWriterAt) ReadAt(p []byte, off int64) (int, error) {
	return o.backing.ReadAt(p, off+o.offset)
}

func (o *offsetReadWriterAt) WriteAt(p []byte, off int64) (int, error) {
	return o.backing.WriteAt(p, off+o.offset)
}

func WriteZero(w io.WriterAt, off, length int64) (int64, error) {
	n := int64(0)
	for length > 0 {
		writeLen := len(zeroBuf)
		if int64(writeLen) > length {
			writeLen = int(length)
		}

		written, err := w.WriteAt(zeroBuf[:writeLen], off+n)
		n += int64(written)
		length -= int64(written)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}
