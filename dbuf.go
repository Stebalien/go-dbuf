package dbuf

import (
	"errors"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/libp2p/go-buffer-pool"
)

// BufferSize is the maximum amount of data we'll buffer before sending.
const BufferSize = 4096

// SmallWriteSize is the point at which we'll try waiting a bit before sending more data.
const SmallWriteSize = BufferSize / 8

// ErrClosed is an error returned when operating on a closed writer.
var ErrClosed = errors.New("writer closed")

// Writer is an auto-buffered write adapter
type Writer struct {
	inner   io.Writer
	writing bool
	err     error
	buf     []byte
	cond    sync.Cond
	mu      sync.Mutex
}

// NewWriter constructs a new auto-buffered Writer.
func NewWriter(w io.Writer) *Writer {
	bw := &Writer{
		inner: w,
	}
	bw.cond.L = &bw.mu
	go bw.writeLoop()
	return bw
}

func (w *Writer) writeDirect(buf []byte) (int, error) {
	// Steal the buffer and write it here.
	curBuf := w.buf
	w.buf = nil
	for w.writing {
		w.cond.Wait()
		if w.err != nil {
			if curBuf != nil {
				pool.Put(curBuf)
			}
			return 0, w.err
		}
	}
	if curBuf != nil {
		// Write this ourselves. No need to wait for go to
		// schedule the other goroutine.
		if err := w.writeAndReturn(curBuf); err != nil {
			w.setErr(err)
			return 0, w.err
		}
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

func (w *Writer) setErr(err error) {
	if w.err != nil {
		w.err = err
		if wc, ok := w.inner.(io.WriteCloser); ok {
			wc.Close()
		}
		if w.buf != nil {
			pool.Put(w.buf)
			w.buf = nil
		}
		w.cond.Broadcast()
	}
}

// Write writes to the underlying writer, buffering as necessary.
//
// Thread-safety: Write is *not* thread-safe. Do not write from multiple
// threads.
func (w *Writer) Write(buf []byte) (int, error) {
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
		// No need to notify the write loop, that has already happened.
		written := copy(w.buf[len(w.buf):cap(w.buf)], buf)
		w.buf = w.buf[:len(w.buf)+written]
		// Leftover bytes.
		buf = buf[written:]
		if len(buf) == 0 {
			return length, nil
		}
		// If we've filled it, wait for the write loop to take the
		// buffer from us.
		for w.buf != nil {
			w.cond.Wait()
			if w.err != nil {
				return written, w.err
			}
		}
	}
	w.buf = pool.Get(BufferSize)[:len(buf)]
	copy(w.buf, buf)
	// Notify write loop that we have a new buffer ready.
	w.cond.Broadcast()
	return length, nil
}

// Flush waits for any buffered data to be written.
//
// Thread-safety: Flush is not thread-safe. It should not be called concurrently
// with either itself or Write.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.err == nil && (w.writing || w.buf != nil) {
		w.cond.Wait()
	}
	return w.err
}

// Close flushes the Writer and, if the underlying writer implements
// `io.Closer`, it closes it.
//
// Thread-safety: Close is thread-safe.
func (w *Writer) Close() error {
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
		w.buf = nil
	}
	if wc, ok := w.inner.(io.WriteCloser); ok {
		err := wc.Close()
		// Only use the close error if no error is currently set.
		if w.err != nil {
			w.err = err
		}
	}
	err := w.err
	if err == nil {
		w.err = ErrClosed
	}
	// Interrupt the write loop and any concurrent writers.
	// Now that err is set, they'll all bail.
	w.cond.Broadcast()
	return err
}

func (w *Writer) writeAndReturn(buf []byte) error {
	var err error
	var written, n int
	for err == nil && written < len(buf) {
		n, err = w.inner.Write(buf[written:])
		written += n
	}
	pool.Put(buf)
	return err
}

func (w *Writer) writeLoop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return
	}

	var err error
	for {
		for w.buf == nil {
			w.cond.Wait()
			if w.err != nil {
				return
			}
		}
		sleeps := 0
		// If we're trying to send less than 512B, try waiting a bit.
		// Limit to 10 microseconds of "sleep" time.
		for len(w.buf) < SmallWriteSize && sleeps < 10 {
			curLen := len(w.buf)

			// Fast
			w.mu.Unlock()
			runtime.Gosched()
			w.mu.Lock()
			if curLen != len(w.buf) {
				continue
			}

			// Slow
			w.mu.Unlock()
			time.Sleep(time.Microsecond)
			w.mu.Lock()
			if curLen != len(w.buf) {
				continue
			}
			break
		}
		buf := w.buf

		// Ready for a value
		w.buf = nil
		w.cond.Broadcast()

		w.writing = true
		w.mu.Unlock()

		err = w.writeAndReturn(buf)
		w.mu.Lock()
		w.writing = false

		// Done writing, notify.
		w.cond.Broadcast()

		if w.err != nil {
			return
		}

		if err != nil {
			w.setErr(err)
			return
		}
	}
}
