package dbuf

import (
	"bytes"
	"crypto/rand"
	"io"
	"io/ioutil"
	"testing"
)

const size = 256

func BenchmarkBuf(t *testing.B) {
	var data [1024 * 1024]byte

	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		out, in := io.Pipe()
		bufin := NewWriter(in)
		go func(data []byte) {
			defer bufin.Close()
			for len(data) > 0 {
				n, err := bufin.Write(data[:size])
				data = data[n:]
				if err != nil {
					t.Fatal(err)
				}
			}
		}(data[:])

		dataout, err := ioutil.ReadAll(out)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(dataout, data[:]) {
			t.Fatal("got bad data")
		}
	}
}

func BenchmarkNormal(t *testing.B) {
	var data [1024 * 1024]byte

	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		out, in := io.Pipe()
		bufin := in
		go func(data []byte) {
			defer bufin.Close()
			for len(data) > 0 {
				n, err := bufin.Write(data[:size])
				data = data[n:]
				if err != nil {
					t.Fatal(err)
				}
			}
		}(data[:])

		dataout, err := ioutil.ReadAll(out)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(dataout, data[:]) {
			t.Fatal("got bad data")
		}
	}
}
