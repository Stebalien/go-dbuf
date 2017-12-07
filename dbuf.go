package dbuf

import (
	"errors"
	"github.com/libp2p/go-msgio/mpool"
	"io"
	"sync"
)

// Default Buffer Size
const BufferSize = 4096

// ErrClosed is an error returned when operating on a closed writer.
var ErrClosed = errors.New("writer closed")

type writer struct {
	inner   io.Writer
	err     error
	writing bool
	buf     []byte
	cond    sync.Cond
	mu      sync.Mutex
}

func NewWriter(w io.Writer) io.WriteCloser {
	bw := &writer{
		inner: w,
	}
	bw.cond.L = &bw.mu
	go bw.writeLoop()
	return bw
}

func (w *writer) writeDirect(buf []byte) (int, error) {
	for w.writing {
		w.cond.Wait()
		if w.err != nil {
			return 0, w.err
		}
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.buf != nil {
		// Write this ourselves. No need to wait for go to
		// schedule the other goroutine.
		if err := w.writeAndReturn(w.buf); err != nil {
			w.setErr(err)
			return 0, w.err
		}
		w.buf = nil
		w.cond.Broadcast()
		if w.err != nil {
			return 0, w.err
		}
	}
	written := 0
	// Loop this. No need to keep taking the lock.
	for written < len(buf) {
		n, err := w.inner.Write(buf[written:])
		written += n
		if err != nil {
			w.setErr(err)
			return written, w.err
		}
	}
	return written, nil
}

func (w *writer) setErr(err error) {
	if w.err != nil {
		w.err = err
		if wc, ok := w.inner.(io.WriteCloser); ok {
			wc.Close()
		}
		if w.buf != nil {
			mpool.ByteSlicePool.Put(BufferSize, w.buf)
			w.buf = nil
		}
		w.cond.Broadcast()
	}
}

func (w *writer) Write(buf []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	length := len(buf)
	if length >= BufferSize {
		return w.writeDirect(buf)
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.buf != nil {
		// Fill existing buffer
		written := copy(w.buf[len(w.buf):cap(w.buf)], buf)
		w.buf = w.buf[:len(w.buf)+written]
		// Leftover bytes.
		buf = buf[written:]
		w.cond.Broadcast()
		if len(buf) == 0 {
			return length, nil
		}
		for w.buf != nil {
			w.cond.Wait()
			if w.err != nil {
				return written, w.err
			}
		}
	}
	w.buf = mpool.ByteSlicePool.Get(BufferSize).([]byte)[:len(buf)]
	copy(w.buf, buf)
	w.cond.Broadcast()
	return length, nil
}

func (w *writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	for w.writing {
		w.cond.Wait()
		if w.err != nil {
			return w.err
		}
	}
	if w.err != nil {
		return w.err
	}
	if w.buf != nil {
		err := w.writeAndReturn(w.buf)
		if err != nil {
			w.setErr(err)
		}
	}
	return w.err
}

func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return w.err
	}
	for w.writing {
		w.cond.Wait()
		if w.err != nil {
			return w.err
		}
	}
	if w.buf != nil {
		w.err = w.writeAndReturn(w.buf)
	}
	if wc, ok := w.inner.(io.WriteCloser); ok {
		err := wc.Close()
		// Only use the close error if no error is currently set.
		if w.err != nil {
			w.err = err
		}
	}
	w.cond.Broadcast()
	if w.err == nil {
		w.err = ErrClosed
		return nil
	}
	return w.err
}

func (w *writer) writeAndReturn(buf []byte) error {
	var err error
	var written, n int
	for err == nil && written < len(buf) {
		n, err = w.inner.Write(buf[written:])
		written += n
	}
	mpool.ByteSlicePool.Put(BufferSize, buf)
	return err
}

func (w *writer) writeLoop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	var err error
	for {
		w.writing = false
		if w.err != nil {
			return
		}
		if err != nil {
			w.setErr(err)
			return
		}
		w.cond.Broadcast()
		for w.buf == nil {
			w.cond.Wait()
			if w.err != nil {
				return
			}
		}
		buf := w.buf
		w.buf = nil
		w.writing = true

		w.mu.Unlock()
		w.cond.Broadcast()
		err = w.writeAndReturn(buf)
		w.mu.Lock()
	}
}
