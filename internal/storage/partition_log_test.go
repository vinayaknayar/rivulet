package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestLog(t *testing.T, segBytes int64) (*PartitionLog, string) {
	t.Helper()
	dir := t.TempDir()
	pl, err := OpenPartitionLog(dir, segBytes, 128)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return pl, dir
}

func TestAppendAndReadRoundtrip(t *testing.T) {
	pl, _ := newTestLog(t, 1<<20)
	defer pl.Close()

	const N = 500
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("k-%d", i))
		val := []byte(fmt.Sprintf("value-payload-%d", i))
		off, err := pl.Append(key, val)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if off != int64(i) {
			t.Fatalf("expected offset %d, got %d", i, off)
		}
	}
	if got := pl.NextOffset(); got != N {
		t.Fatalf("expected NextOffset %d, got %d", N, got)
	}

	// Read from various starting offsets.
	for _, start := range []int64{0, 1, 42, 400, N - 1} {
		recs, err := pl.Read(start, 1<<20)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(recs) == 0 {
			t.Fatalf("no records read for start=%d", start)
		}
		if recs[0].Offset != start {
			t.Fatalf("first read offset=%d, want %d", recs[0].Offset, start)
		}
		wantKey := []byte(fmt.Sprintf("k-%d", start))
		if !bytes.Equal(recs[0].Key, wantKey) {
			t.Fatalf("key mismatch: got %q want %q", recs[0].Key, wantKey)
		}
	}
}

func TestSegmentRollover(t *testing.T) {
	pl, dir := newTestLog(t, 512) // tiny segments to force rollover
	defer pl.Close()

	for i := 0; i < 200; i++ {
		if _, err := pl.Append(nil, bytes.Repeat([]byte{'x'}, 32)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// Multiple .log files should exist.
	entries, _ := os.ReadDir(dir)
	logs := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logs++
		}
	}
	if logs < 2 {
		t.Fatalf("expected multiple segments, got %d", logs)
	}
	// Read the final record via its offset — this crosses the last rollover.
	recs, err := pl.Read(199, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) == 0 || recs[0].Offset != 199 {
		t.Fatalf("did not find offset 199 after rollover, got %+v", recs)
	}
}

func TestRestartRecovers(t *testing.T) {
	dir := t.TempDir()
	pl, err := OpenPartitionLog(dir, 512, 128)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 50; i++ {
		if _, err := pl.Append([]byte("k"), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	pl.Close()

	pl2, err := OpenPartitionLog(dir, 512, 128)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer pl2.Close()
	if got := pl2.NextOffset(); got != 50 {
		t.Fatalf("expected NextOffset 50 after reopen, got %d", got)
	}
	recs, err := pl2.Read(30, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) == 0 || recs[0].Offset != 30 {
		t.Fatalf("bad read after reopen: %+v", recs)
	}
	if string(recs[0].Value) != "v30" {
		t.Fatalf("wrong value after reopen: %q", recs[0].Value)
	}
}

func TestReadPastEndReturnsEmpty(t *testing.T) {
	pl, _ := newTestLog(t, 1<<20)
	defer pl.Close()
	for i := 0; i < 10; i++ {
		pl.Append(nil, []byte("v"))
	}
	recs, err := pl.Read(1000, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected empty read past end, got %d records", len(recs))
	}
}

func TestRetentionDropsOldSegments(t *testing.T) {
	pl, dir := newTestLog(t, 256)
	defer pl.Close()
	for i := 0; i < 100; i++ {
		pl.Append(nil, bytes.Repeat([]byte{'x'}, 40))
	}
	before, _ := os.ReadDir(dir)
	if err := pl.EnforceRetention(200, 0); err != nil {
		t.Fatalf("retention: %v", err)
	}
	after, _ := os.ReadDir(dir)
	if len(after) >= len(before) {
		t.Fatalf("retention did not drop any segments (before=%d after=%d)", len(before), len(after))
	}
	// Tail records are still readable.
	recs, err := pl.Read(pl.NextOffset()-1, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) == 0 {
		t.Fatalf("no records left after retention")
	}
	_ = time.Second
}
