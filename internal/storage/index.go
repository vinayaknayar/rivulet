package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Index is a sparse map from a segment's relative offset -> byte position in
// the log file. One entry is written roughly every `interval` bytes of log,
// matching Kafka's approach: reads binary-search this small file to seek near
// the target, then scan forward in the big log.
//
// On-disk format: repeated [relOffset:4 BE][filePosition:4 BE].
const indexEntrySize = 8

type indexEntry struct {
	relOffset int32
	position  int32
}

type Index struct {
	mu                  sync.Mutex
	path                string
	file                *os.File
	entries             []indexEntry
	bytesSinceLastEntry int64
	interval            int64
}

func OpenIndex(path string, interval int64) (*Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	idx := &Index{path: path, file: f, interval: interval}
	if stat.Size() > 0 {
		n := stat.Size() / indexEntrySize
		buf := make([]byte, n*indexEntrySize)
		if _, err := io.ReadFull(f, buf); err != nil {
			f.Close()
			return nil, err
		}
		for i := int64(0); i < n; i++ {
			off := int32(binary.BigEndian.Uint32(buf[i*indexEntrySize:]))
			pos := int32(binary.BigEndian.Uint32(buf[i*indexEntrySize+4:]))
			idx.entries = append(idx.entries, indexEntry{off, pos})
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return idx, nil
}

// MaybeAppend records an index entry if this is the first record in the
// segment or if enough bytes have accumulated since the last entry.
// recordSize is the size of the record just written (frame + body).
func (i *Index) MaybeAppend(relOffset int64, position int64, recordSize int64) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.entries) == 0 || i.bytesSinceLastEntry >= i.interval {
		e := indexEntry{relOffset: int32(relOffset), position: int32(position)}
		i.entries = append(i.entries, e)
		var buf [indexEntrySize]byte
		binary.BigEndian.PutUint32(buf[0:4], uint32(e.relOffset))
		binary.BigEndian.PutUint32(buf[4:8], uint32(e.position))
		if _, err := i.file.Write(buf[:]); err != nil {
			return fmt.Errorf("index write: %w", err)
		}
		i.bytesSinceLastEntry = 0
	}
	i.bytesSinceLastEntry += recordSize
	return nil
}

// LookupPosition returns the largest file position whose relative offset is
// <= target. Callers scan forward from there in the log until they hit the
// desired offset.
func (i *Index) LookupPosition(target int64) int64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	lo, hi := 0, len(i.entries)-1
	var result int64
	for lo <= hi {
		mid := (lo + hi) / 2
		if int64(i.entries[mid].relOffset) <= target {
			result = int64(i.entries[mid].position)
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

// LastEntry returns the last index entry (relOffset, filePos), if any.
func (i *Index) LastEntry() (relOffset int64, position int64, ok bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.entries) == 0 {
		return 0, 0, false
	}
	e := i.entries[len(i.entries)-1]
	return int64(e.relOffset), int64(e.position), true
}

func (i *Index) Path() string  { return i.path }
func (i *Index) Sync() error   { return i.file.Sync() }
func (i *Index) Close() error  { return i.file.Close() }
