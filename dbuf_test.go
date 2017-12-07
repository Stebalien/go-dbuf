package dbuf

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"io"
	"io/ioutil"
	"net"
	"testing"
)

const size = 256
const bufsize = 1024 * 256

func tcpPipe(t testing.TB) (net.Conn, net.Conn) {
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

var pipe = tcpPipe

//var pipe = func(testing.TB) (io.ReadCloser, io.WriteCloser) { return io.Pipe() }

func doTest(t testing.TB, data []byte, out io.WriteCloser, in io.ReadCloser) {
	go func(data []byte) {
		defer out.Close()
		for len(data) > 0 {
			n, err := out.Write(data[:size])
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
	if !bytes.Equal(dataout, data[:]) {
		t.Fatal("got bad data")
	}
}

func BenchmarkDBuf(t *testing.B) {
	var data [bufsize]byte

	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}

	t.StopTimer()
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		in, out := pipe(t)
		bufout := NewWriter(out)
		t.StartTimer()
		doTest(t, data[:], bufout, in)
		t.StopTimer()
	}
}

func BenchmarkNormal(t *testing.B) {
	var data [bufsize]byte

	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()
	t.StopTimer()

	for i := 0; i < t.N; i++ {
		in, out := pipe(t)
		t.StartTimer()
		doTest(t, data[:], out, in)
		t.StopTimer()
	}
}

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

func BenchmarkBuf(t *testing.B) {
	var data [bufsize]byte

	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()
	t.StopTimer()

	for i := 0; i < t.N; i++ {
		in, out := pipe(t)
		t.StartTimer()
		bufout := &BufWriteCloser{bw: *bufio.NewWriter(out), inner: out}
		doTest(t, data[:], bufout, in)
		t.StopTimer()
	}
}
