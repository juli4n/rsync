package rsync

import (
	"errors"
	"io"
	"sync"
)

type slidingWindowBuf struct {
	source    io.Reader
	chunkSize int64

	s      []byte
	slider int64

	lastOffset int64
	size       int64
	eof        bool

	byteSlicePool *sync.Pool
}

func newSlidingWindowBuf(source io.Reader, chunkSize int64) (*slidingWindowBuf, error) {
	r := &slidingWindowBuf{
		source:    source,
		chunkSize: chunkSize,
		byteSlicePool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 3*chunkSize)
			},
		},
	}

	err := r.grow(0)
	return r, err
}

func (r *slidingWindowBuf) grow(requestedOffset int64) error {
	if r.eof || requestedOffset+(2*r.chunkSize) <= r.size {
		return nil
	}
	chunk := r.byteSlicePool.Get().([]byte)
	n, err := r.source.Read(chunk)
	if err != nil {
		if err != io.EOF {
			return err
		}
		r.eof = true
		return nil
	}

	if r.lastOffset-r.slider < (3 * r.chunkSize) {
		r.s = append(r.s, chunk[:n]...)
		r.size += int64(n)
	} else {
		r.byteSlicePool.Put(r.s[0 : r.chunkSize*3])
		r.s = append(r.s[r.chunkSize*3:], chunk[:n]...)
		r.slider += r.chunkSize * 3
		r.size += int64(n)
	}
	//fmt.Printf("readseakerbuf is now %d bytes\n", len(r.s))

	if int64(n) < r.chunkSize {
		return nil
	}

	// allow for tail recursion optimization
	return r.grow(requestedOffset)
}

// ReadAt() without copy()
func (r *slidingWindowBuf) ReadAt(off int64, n int64) ([]byte, int64, error) {
	if off < 0 {
		return []byte{}, 0, errors.New("bytes.Reader.ReadAt: negative offset")
	}

	if err := r.grow(off); err != nil {
		return []byte{}, 0, err
	}

	if (off - r.slider) >= int64(len(r.s)) {
		return []byte{}, 0, io.EOF
	}

	if (off-r.slider)+n > int64(len(r.s)) {
		n = int64(len(r.s)) - (off - r.slider)
	}

	r.lastOffset = off
	return r.s[(off - r.slider) : (off-r.slider)+n], n, nil
}

func (r *slidingWindowBuf) ReadByteAt(off int64) (content byte, err error) {
	if off < 0 {
		return 0, errors.New("bytes.Reader.ReadAt: negative offset")
	}

	if (off-r.slider)+1 >= int64(len(r.s)) {
		return 0, io.EOF
	}

	r.lastOffset = off

	//if err := r.grow(off); err != nil {
	//	return 0, err
	//}

	return r.s[(off - r.slider) : (off-r.slider)+1][0], nil
}
