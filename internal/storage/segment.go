package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record is one message stored in a partition log.
type Record struct {
	Offset    int64
	Timestamp int64 // unix millis
	Key       []byte
	Value     []byte
}

// On-disk record framing:
//   [recordSize:4]                      <- length of the payload that follows
//   [offset:8][timestamp:8]
//   [keySize:4 (-1 for nil)] [key: keySize]
//   [valueSize:4 (-1 for nil)] [value: valueSize]
//   [crc:4]                             <- CRC32 IEEE of everything above,
//                                          from offset through value
//
// This layout is deliberately similar to (but simpler than) Kafka's on-disk
// record format so that adding batching, headers, or compression later is a
// natural extension.

const (
	recordSizePrefix = 4
	minBodyBytes     = 8 + 8 + 4 + 4 + 4 // offset+ts+keySize+valueSize+crc
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrCorruptRecord    = errors.New("corrupt record: crc mismatch")
)

// Segment is a single append-only log file plus its sparse index.
type Segment struct {
	mu         sync.RWMutex
	baseOffset int64
	dir        string
	logPath    string
	logFile    *os.File
	index      *Index
	position   int64
	nextOffset int64
}

// OpenSegment opens (or creates) a segment starting at baseOffset. On open,
// it scans forward from the last index entry to recover the true end of the
// log — this fixes torn writes and rebuilds nextOffset after a crash.
func OpenSegment(dir string, baseOffset int64, indexInterval int64) (*Segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	idx, err := OpenIndex(idxPath, indexInterval)
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &Segment{
		baseOffset: baseOffset,
		dir:        dir,
		logPath:    logPath,
		logFile:    f,
		index:      idx,
		position:   stat.Size(),
		nextOffset: baseOffset,
	}
	if err := s.recover(); err != nil {
		s.Close()
		return nil, err
	}
	// Ensure future writes append.
	if _, err := s.logFile.Seek(0, io.SeekEnd); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Segment) recover() error {
	// Start from the last known index entry (or 0 if none) and walk forward
	// through the log to figure out where the next append should go. If a
	// torn frame is encountered, truncate to the last good record.
	pos := int64(0)
	if _, p, ok := s.index.LastEntry(); ok {
		pos = p
	}
	nextOff := s.baseOffset
	for pos < s.position {
		var hdr [recordSizePrefix]byte
		if _, err := s.logFile.ReadAt(hdr[:], pos); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		size := int64(binary.BigEndian.Uint32(hdr[:]))
		if size <= 0 || pos+int64(recordSizePrefix)+size > s.position {
			if err := s.logFile.Truncate(pos); err != nil {
				return err
			}
			s.position = pos
			break
		}
		body := make([]byte, size)
		if _, err := s.logFile.ReadAt(body, pos+int64(recordSizePrefix)); err != nil {
			return err
		}
		recOff := int64(binary.BigEndian.Uint64(body[0:8]))
		nextOff = recOff + 1
		pos += int64(recordSizePrefix) + size
	}
	s.position = pos
	s.nextOffset = nextOff
	return nil
}

// Append writes one record to the segment. Returns the assigned absolute
// offset and the starting file position of the record.
func (s *Segment) Append(timestamp int64, key, value []byte) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	off := s.nextOffset
	keyLen := int32(-1)
	if key != nil {
		keyLen = int32(len(key))
	}
	valLen := int32(-1)
	if value != nil {
		valLen = int32(len(value))
	}
	bodyLen := 8 + 8 + 4 + max0(keyLen) + 4 + max0(valLen) + 4
	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint64(body[0:8], uint64(off))
	binary.BigEndian.PutUint64(body[8:16], uint64(timestamp))
	binary.BigEndian.PutUint32(body[16:20], uint32(keyLen))
	p := int32(20)
	if key != nil {
		copy(body[p:p+int32(len(key))], key)
		p += int32(len(key))
	}
	binary.BigEndian.PutUint32(body[p:p+4], uint32(valLen))
	p += 4
	if value != nil {
		copy(body[p:p+int32(len(value))], value)
		p += int32(len(value))
	}
	crc := crc32.ChecksumIEEE(body[:p])
	binary.BigEndian.PutUint32(body[p:p+4], crc)

	frame := make([]byte, recordSizePrefix+len(body))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(body)))
	copy(frame[4:], body)

	writePos := s.position
	if _, err := s.logFile.Write(frame); err != nil {
		return 0, 0, err
	}
	s.position += int64(len(frame))
	s.nextOffset++
	if err := s.index.MaybeAppend(off-s.baseOffset, writePos, int64(len(frame))); err != nil {
		return 0, 0, err
	}
	return off, writePos, nil
}

func max0(v int32) int32 {
	if v < 0 {
		return 0
	}
	return v
}

// Read returns up to maxBytes' worth of records with offset >= startOffset.
func (s *Segment) Read(startOffset int64, maxBytes int64) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if startOffset >= s.nextOffset {
		return nil, nil
	}
	if startOffset < s.baseOffset {
		startOffset = s.baseOffset
	}
	rel := startOffset - s.baseOffset
	pos := s.index.LookupPosition(rel)

	var out []Record
	var readBytes int64
	for pos < s.position {
		var hdr [recordSizePrefix]byte
		if _, err := s.logFile.ReadAt(hdr[:], pos); err != nil {
			return nil, err
		}
		size := int64(binary.BigEndian.Uint32(hdr[:]))
		body := make([]byte, size)
		if _, err := s.logFile.ReadAt(body, pos+int64(recordSizePrefix)); err != nil {
			return nil, err
		}
		rec, err := decodeBody(body)
		if err != nil {
			return nil, err
		}
		advance := int64(recordSizePrefix) + size
		pos += advance
		if rec.Offset < startOffset {
			continue
		}
		out = append(out, rec)
		readBytes += advance
		if readBytes >= maxBytes {
			break
		}
	}
	return out, nil
}

func decodeBody(body []byte) (Record, error) {
	if len(body) < minBodyBytes {
		return Record{}, fmt.Errorf("record body too small: %d", len(body))
	}
	off := int64(binary.BigEndian.Uint64(body[0:8]))
	ts := int64(binary.BigEndian.Uint64(body[8:16]))
	keySize := int32(binary.BigEndian.Uint32(body[16:20]))
	p := int64(20)
	var key []byte
	if keySize >= 0 {
		if int64(keySize) > int64(len(body))-p {
			return Record{}, errors.New("record key size overruns body")
		}
		key = make([]byte, keySize)
		copy(key, body[p:p+int64(keySize)])
		p += int64(keySize)
	}
	if p+4 > int64(len(body)) {
		return Record{}, errors.New("record truncated before valueSize")
	}
	valSize := int32(binary.BigEndian.Uint32(body[p : p+4]))
	p += 4
	var val []byte
	if valSize >= 0 {
		if int64(valSize) > int64(len(body))-p-4 {
			return Record{}, errors.New("record value size overruns body")
		}
		val = make([]byte, valSize)
		copy(val, body[p:p+int64(valSize)])
		p += int64(valSize)
	}
	if p+4 > int64(len(body)) {
		return Record{}, errors.New("record truncated before crc")
	}
	want := binary.BigEndian.Uint32(body[p : p+4])
	got := crc32.ChecksumIEEE(body[:p])
	if want != got {
		return Record{}, ErrCorruptRecord
	}
	return Record{Offset: off, Timestamp: ts, Key: key, Value: val}, nil
}

func (s *Segment) BaseOffset() int64 { return s.baseOffset }
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position
}

func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.index.Sync()
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if s.logFile != nil {
		if err := s.logFile.Close(); err != nil {
			firstErr = err
		}
		s.logFile = nil
	}
	if s.index != nil {
		if err := s.index.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.index = nil
	}
	return firstErr
}

func (s *Segment) Delete() error {
	s.mu.Lock()
	logPath := s.logPath
	var idxPath string
	if s.index != nil {
		idxPath = s.index.Path()
	}
	s.mu.Unlock()
	if err := s.Close(); err != nil {
		return err
	}
	err1 := os.Remove(logPath)
	err2 := os.Remove(idxPath)
	if err1 != nil && !errors.Is(err1, os.ErrNotExist) {
		return err1
	}
	if err2 != nil && !errors.Is(err2, os.ErrNotExist) {
		return err2
	}
	return nil
}

func (s *Segment) ModTime() (time.Time, error) {
	stat, err := os.Stat(s.logPath)
	if err != nil {
		return time.Time{}, err
	}
	return stat.ModTime(), nil
}
