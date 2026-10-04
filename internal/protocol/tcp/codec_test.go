package tcpproto

import (
	"bytes"
	"testing"
)

func TestFrameRoundtrip(t *testing.T) {
	payload := []byte("hello world")
	var buf bytes.Buffer
	if err := WriteFrame(&buf, APIProduce, 42, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	api, corr, got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if api != APIProduce || corr != 42 {
		t.Fatalf("api/corr mismatch: %d %d", api, corr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: %q vs %q", got, payload)
	}
}

func TestBufReaderRoundtrip(t *testing.T) {
	b := NewBuf()
	b.WriteString("topic-1")
	b.WriteI32(-1)
	b.WriteI64(1_000_000_000)
	b.WriteBytesI32([]byte("payload"))
	b.WriteBytesI32(nil)

	r := NewReader(b.Bytes())
	if s := r.String(); s != "topic-1" {
		t.Fatalf("string: %q", s)
	}
	if v := r.I32(); v != -1 {
		t.Fatalf("i32: %d", v)
	}
	if v := r.I64(); v != 1_000_000_000 {
		t.Fatalf("i64: %d", v)
	}
	if v := r.BytesI32(); !bytes.Equal(v, []byte("payload")) {
		t.Fatalf("bytes: %q", v)
	}
	if v := r.BytesI32(); v != nil {
		t.Fatalf("expected nil bytes, got %v", v)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestReaderShortDetected(t *testing.T) {
	r := NewReader([]byte{0x00, 0x01})
	_ = r.I64()
	if r.Err() == nil {
		t.Fatal("expected err on short read")
	}
}
