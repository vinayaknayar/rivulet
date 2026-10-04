package storage

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PartitionLog is an ordered chain of segments for one partition. Writes go
// to the tail (active) segment; when it exceeds SegmentBytes, a new one is
// rolled with base offset = (previous nextOffset).
type PartitionLog struct {
	mu            sync.RWMutex
	dir           string
	segments      []*Segment
	segmentBytes  int64
	indexInterval int64
}

// OpenPartitionLog opens (creating if necessary) the partition directory and
// loads all existing segments. If none exist, an empty segment starting at 0
// is created so appends can begin immediately.
func OpenPartitionLog(dir string, segmentBytes, indexInterval int64) (*PartitionLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	pl := &PartitionLog{dir: dir, segmentBytes: segmentBytes, indexInterval: indexInterval}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".log"), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	for _, b := range bases {
		seg, err := OpenSegment(dir, b, indexInterval)
		if err != nil {
			return nil, err
		}
		pl.segments = append(pl.segments, seg)
	}
	if len(pl.segments) == 0 {
		seg, err := OpenSegment(dir, 0, indexInterval)
		if err != nil {
			return nil, err
		}
		pl.segments = []*Segment{seg}
	}
	return pl, nil
}

func (pl *PartitionLog) active() *Segment {
	return pl.segments[len(pl.segments)-1]
}

// NextOffset returns the offset that will be assigned to the next append —
// also the "high watermark" for a single-broker system where there are no
// followers.
func (pl *PartitionLog) NextOffset() int64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.active().NextOffset()
}

// Append writes a record and returns its assigned offset.
func (pl *PartitionLog) Append(key, value []byte) (int64, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if pl.active().Size() >= pl.segmentBytes {
		nextBase := pl.active().NextOffset()
		seg, err := OpenSegment(pl.dir, nextBase, pl.indexInterval)
		if err != nil {
			return 0, err
		}
		pl.segments = append(pl.segments, seg)
	}
	off, _, err := pl.active().Append(time.Now().UnixMilli(), key, value)
	return off, err
}

// Read returns records with offset >= startOffset, up to maxBytes total, from
// the single segment that contains startOffset. If more records are wanted,
// the caller re-calls with the next offset (Kafka semantics).
func (pl *PartitionLog) Read(startOffset int64, maxBytes int64) ([]Record, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	idx := sort.Search(len(pl.segments), func(i int) bool {
		return pl.segments[i].BaseOffset() > startOffset
	}) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(pl.segments) {
		return nil, nil
	}
	return pl.segments[idx].Read(startOffset, maxBytes)
}

// EnforceRetention deletes non-active segments whose age or cumulative size
// exceeds the configured limits. Never deletes the active segment.
func (pl *PartitionLog) EnforceRetention(maxBytes, maxAgeMS int64) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	var total int64
	for _, s := range pl.segments {
		total += s.Size()
	}
	now := time.Now()
	keep := make([]*Segment, 0, len(pl.segments))
	for i, s := range pl.segments {
		isActive := i == len(pl.segments)-1
		if isActive {
			keep = append(keep, s)
			continue
		}
		drop := false
		if maxBytes > 0 && total > maxBytes {
			drop = true
		}
		if !drop && maxAgeMS > 0 {
			if mt, err := s.ModTime(); err == nil && now.Sub(mt).Milliseconds() > maxAgeMS {
				drop = true
			}
		}
		if drop {
			total -= s.Size()
			if err := s.Delete(); err != nil {
				return fmt.Errorf("delete segment: %w", err)
			}
		} else {
			keep = append(keep, s)
		}
	}
	pl.segments = keep
	return nil
}

func (pl *PartitionLog) Sync() error {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.active().Sync()
}

func (pl *PartitionLog) Close() error {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	var firstErr error
	for _, s := range pl.segments {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
