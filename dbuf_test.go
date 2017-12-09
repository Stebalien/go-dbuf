package dbuf

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"testing"
)

const bufsize = 1024 * 256

type pipeFunc func(testing.TB) (io.ReadCloser, io.WriteCloser)
type writeBufferFunc func(w io.WriteCloser) io.WriteCloser
type chopperFunc func(r uint32) int
type chopperMaker func() chopperFunc

// Pipes

func tcpPipe(t testing.TB) (io.ReadCloser, io.WriteCloser) {
	list, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	con1, err := net.Dial("tcp", list.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	con2, err := list.Accept()
	if err != nil {
		t.Fatal(err)
	}

	return con1, con2
}

func ioPipe(testing.TB) (io.ReadCloser, io.WriteCloser) { return io.Pipe() }

// Buffers

func writeBufferNone(w io.WriteCloser) io.WriteCloser { return w }

type BufWriteCloser struct {
	bw    bufio.Writer
	inner io.WriteCloser
}

func (b *BufWriteCloser) Close() error {
	err1 := b.bw.Flush()
	err2 := b.inner.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (b *BufWriteCloser) Write(buf []byte) (int, error) {
	return b.bw.Write(buf)
}

func writeBufferBufio(w io.WriteCloser) io.WriteCloser {
	return &BufWriteCloser{bw: *bufio.NewWriter(w), inner: w}
}

func writeBufferDBuf(w io.WriteCloser) io.WriteCloser {
	return NewWriter(w)
}

// Choppers

func randomSmallChopper() chopperFunc {
	return func(r uint32) int {
		size := 2 << (r % 8)
		size += int(r) % size
		return size
	}
}

func randomLargeChopper() chopperFunc {
	return func(r uint32) int {
		size := 2 << (r % 12)
		size += int(r) % size
		return size
	}
}

func alternatingSmallChopper() chopperFunc {
	small := false
	return func(r uint32) int {
		small = !small
		if small {
			return 10
		}
		return int(r % 512)
	}
}

func alternatingMediumChopper() chopperFunc {
	small := false
	return func(r uint32) int {
		small = !small
		if small {
			return 10
		}
		return int(r % 2929)
	}
}
func alternatingLargeChopper() chopperFunc {
	small := false
	return func(r uint32) int {
		small = !small
		if small {
			return 10
		}
		return int(r % 8000)
	}
}

// Test function
func doTest(t testing.TB, data []byte, chopper chopperFunc, out io.WriteCloser, in io.ReadCloser) {
	go func(data []byte) {
		defer out.Close()
		for len(data) > 0 {
			write := data
			if len(data) > 16 {
				size := chopper(binary.BigEndian.Uint32(data))
				if len(data) > size {
					write = data[:size]
				}
			}
			n, err := out.Write(write)
			data = data[n:]
			if err != nil {
				t.Fatal(err)
			}
		}
	}(data[:])

	dataout, err := ioutil.ReadAll(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dataout, data) {
		t.Fatal("got bad data")
	}
}

func BenchmarkAll(t *testing.B) {
	var data [bufsize]byte

	rand.Seed(0xDEADBEEF)

	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}

	pipeFns := map[string]pipeFunc{
		"io":  ioPipe,
		"tcp": tcpPipe,
	}

	cmFns := map[string]chopperMaker{
		"alternating-large":     alternatingLargeChopper,
		"alternating-medium":    alternatingMediumChopper,
		"alternating-small":     alternatingSmallChopper,
		"weighted_random-large": randomLargeChopper,
		"weighted_random-small": randomSmallChopper,
	}

	bufFns := map[string]writeBufferFunc{
		"bufio": writeBufferBufio,
		"dbuf":  writeBufferDBuf,
		"none":  writeBufferNone,
	}
	for bn, bufFn := range bufFns {
		for pn, pipeFn := range pipeFns {
			for cn, cmFn := range cmFns {
				name := fmt.Sprintf("buffer=%s;pipe=%s;chunking=%s", bn, pn, cn)
				t.Run(name, func(t *testing.B) {
					runBenchmark(t, data[:], pipeFn, cmFn, bufFn)
				})
			}
		}
	}
}

func runBenchmark(t *testing.B, data []byte, pipeFn pipeFunc, cmFn chopperMaker, bufFn writeBufferFunc) {
	t.StopTimer()
	t.ResetTimer()

	t.SetBytes(int64(len(data)))

	for i := 0; i < t.N; i++ {
		in, out := pipeFn(t)
		bufout := bufFn(out)
		chopper := cmFn()
		rand.Seed(1912)
		t.StartTimer()
		doTest(t, data, chopper, bufout, in)
		t.StopTimer()
	}
}
