// Package tcpproto implements the custom TCP wire protocol.
//
// Frame format:
//   [totalLen:4 BE][apiKey:2][correlationId:4][payload...]
// totalLen counts (apiKey + correlationId + payload). All integers are
// big-endian. Requests and responses share this outer framing; the response
// echoes the request's apiKey and correlationId so a client can multiplex.
//
// API keys:
//   0 PRODUCE          1 FETCH          2 METADATA          3 CREATE_TOPIC
//
// Every response payload starts with:
//   [errCode:2][errMsgLen:2][errMsg]
// followed by API-specific body (only meaningful when errCode == 0).
//
// PRODUCE request body:
//   [topicLen:2][topic][partition:4][keyLen:4][key][valueLen:4][value]
//   partition == -1 -> broker picks; key/value len == -1 -> nil.
// PRODUCE response body:
//   [topicLen:2][topic][partition:4][offset:8]
//
// FETCH request body:
//   [topicLen:2][topic][partition:4][offset:8][maxBytes:4]
// FETCH response body:
//   [highWatermark:8][numRecords:4]<record...>
//   record: [offset:8][ts:8][keyLen:4][key][valueLen:4][value]
//
// METADATA request body: empty
// METADATA response body: [numTopics:4]<topic...>
//   topic: [nameLen:2][name][numPartitions:4]
//
// CREATE_TOPIC request body: [nameLen:2][name][numPartitions:4]
// CREATE_TOPIC response body: empty
package tcpproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	APIProduce     uint16 = 0
	APIFetch       uint16 = 1
	APIMetadata    uint16 = 2
	APICreateTopic uint16 = 3
)

const (
	ErrOK         uint16 = 0
	ErrUnknown    uint16 = 1
	ErrBadRequest uint16 = 2
)

const maxFrameBytes = 16 * 1024 * 1024

func ReadFrame(r io.Reader) (uint16, uint32, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, 0, nil, err
	}
	total := binary.BigEndian.Uint32(lenBuf[:])
	if total < 6 {
		return 0, 0, nil, fmt.Errorf("frame too small: %d", total)
	}
	if total > maxFrameBytes {
		return 0, 0, nil, fmt.Errorf("frame too large: %d", total)
	}
	buf := make([]byte, total)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, 0, nil, err
	}
	apiKey := binary.BigEndian.Uint16(buf[0:2])
	corr := binary.BigEndian.Uint32(buf[2:6])
	return apiKey, corr, buf[6:], nil
}

func WriteFrame(w io.Writer, apiKey uint16, correlationID uint32, payload []byte) error {
	total := uint32(6 + len(payload))
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf[0:4], total)
	binary.BigEndian.PutUint16(buf[4:6], apiKey)
	binary.BigEndian.PutUint32(buf[6:10], correlationID)
	copy(buf[10:], payload)
	_, err := w.Write(buf)
	return err
}

// Buf is an append-only encoder for payload bodies.
type Buf struct{ b []byte }

func NewBuf() *Buf         { return &Buf{} }
func (b *Buf) Bytes() []byte { return b.b }

func (b *Buf) WriteU16(v uint16) {
	var x [2]byte
	binary.BigEndian.PutUint16(x[:], v)
	b.b = append(b.b, x[:]...)
}
func (b *Buf) WriteU32(v uint32) {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], v)
	b.b = append(b.b, x[:]...)
}
func (b *Buf) WriteI32(v int32) { b.WriteU32(uint32(v)) }
func (b *Buf) WriteI64(v int64) {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], uint64(v))
	b.b = append(b.b, x[:]...)
}
func (b *Buf) WriteString(s string) { b.WriteU16(uint16(len(s))); b.b = append(b.b, s...) }
func (b *Buf) WriteBytesI32(s []byte) {
	if s == nil {
		b.WriteI32(-1)
		return
	}
	b.WriteI32(int32(len(s)))
	b.b = append(b.b, s...)
}

// PutErr writes the standard error prefix at the front of a fresh Buf.
func (b *Buf) PutErr(code uint16, msg string) {
	b.WriteU16(code)
	b.WriteString(msg)
}

// Reader is a cursor-based decoder for payload bodies.
type Reader struct {
	b   []byte
	pos int
	err error
}

func NewReader(b []byte) *Reader { return &Reader{b: b} }
func (r *Reader) Err() error     { return r.err }

func (r *Reader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if r.pos+n > len(r.b) {
		r.err = fmt.Errorf("short read at pos=%d need=%d have=%d", r.pos, n, len(r.b)-r.pos)
		return false
	}
	return true
}

func (r *Reader) U16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(r.b[r.pos:])
	r.pos += 2
	return v
}
func (r *Reader) U32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return v
}
func (r *Reader) I32() int32 { return int32(r.U32()) }
func (r *Reader) I64() int64 {
	if !r.need(8) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(r.b[r.pos:]))
	r.pos += 8
	return v
}
func (r *Reader) String() string {
	n := int(r.U16())
	if !r.need(n) {
		return ""
	}
	s := string(r.b[r.pos : r.pos+n])
	r.pos += n
	return s
}
func (r *Reader) BytesI32() []byte {
	n := r.I32()
	if n < 0 {
		return nil
	}
	if !r.need(int(n)) {
		return nil
	}
	v := make([]byte, n)
	copy(v, r.b[r.pos:])
	r.pos += int(n)
	return v
}

var ErrShortFrame = errors.New("short frame")
