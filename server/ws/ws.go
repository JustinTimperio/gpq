package ws

import (
	"bytes"
	"errors"
	"io"
)

// WriterSeeker is an in-memory io.WriteSeeker implementation.
type WriterSeeker struct {
	Buf bytes.Buffer
	Pos int
}

// Write writes to the buffer of this WriterSeeker instance.
func (ws *WriterSeeker) Write(p []byte) (n int, err error) {
	// If the offset is past the end of the buffer, grow the buffer with null bytes.
	if extra := ws.Pos - ws.Buf.Len(); extra > 0 {
		if _, err := ws.Buf.Write(make([]byte, extra)); err != nil {
			return n, err
		}
	}

	// If the offset isn't at the end of the buffer, write as much as we can.
	if ws.Pos < ws.Buf.Len() {
		n = copy(ws.Buf.Bytes()[ws.Pos:], p)
		p = p[n:]
	}

	// If there are remaining bytes, append them to the buffer.
	if len(p) > 0 {
		var bn int
		bn, err = ws.Buf.Write(p)
		n += bn
	}

	ws.Pos += n
	return n, err
}

// Seek seeks in the buffer of this WriterSeeker instance.
func (ws *WriterSeeker) Seek(offset int64, whence int) (int64, error) {
	newPos, offs := 0, int(offset)
	switch whence {
	case io.SeekStart:
		newPos = offs
	case io.SeekCurrent:
		newPos = ws.Pos + offs
	case io.SeekEnd:
		newPos = ws.Buf.Len() + offs
	}
	if newPos < 0 {
		return 0, errors.New("negative result Pos")
	}
	ws.Pos = newPos
	return int64(newPos), nil
}

// Reader returns an io.Reader. Use it, for example, with io.Copy, to copy the content of the WriterSeeker buffer to an io.Writer.
func (ws *WriterSeeker) Reader() io.Reader {
	return bytes.NewReader(ws.Buf.Bytes())
}

// Close is a no-op function that satisfies the io.Closer interface.
func (ws *WriterSeeker) Close() error {
	return nil
}

// BytesReader returns a *bytes.Reader. Use it when you need a reader that implements the io.ReadSeeker interface.
func (ws *WriterSeeker) BytesReader() *bytes.Reader {
	return bytes.NewReader(ws.Buf.Bytes())
}
