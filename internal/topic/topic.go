package topic

import (
	"path/filepath"
	"strconv"
	"sync/atomic"

	"rivulet/internal/storage"
)

type Topic struct {
	Name       string
	Partitions []*Partition
	rrCounter  atomic.Uint32
}

// OpenTopic opens (creating if necessary) all partition directories for a
// topic and loads their logs. Directory layout: <dataDir>/<name>/<partition>/.
func OpenTopic(dataDir, name string, numPartitions int32, segmentBytes, indexInterval int64) (*Topic, error) {
	t := &Topic{Name: name}
	for i := int32(0); i < numPartitions; i++ {
		dir := filepath.Join(dataDir, name, strconv.FormatInt(int64(i), 10))
		pl, err := storage.OpenPartitionLog(dir, segmentBytes, indexInterval)
		if err != nil {
			return nil, err
		}
		t.Partitions = append(t.Partitions, &Partition{ID: i, Log: pl})
	}
	return t, nil
}

func (t *Topic) NumPartitions() int32 { return int32(len(t.Partitions)) }

func (t *Topic) NextRoundRobin() uint32 { return t.rrCounter.Add(1) }
