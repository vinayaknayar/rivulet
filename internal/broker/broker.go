package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"rivulet/internal/config"
	"rivulet/internal/storage"
	"rivulet/internal/topic"
)

// Broker is the protocol-neutral core: TCP and gRPC edges convert their wire
// messages into these request types and call the same methods. Any new edge
// (Kafka-wire compatibility, HTTP, whatever) plugs in the same way.
type Broker struct {
	cfg    config.Config
	mu     sync.RWMutex
	topics map[string]*topic.Topic
}

type ProduceRequest struct {
	Topic     string
	Partition int32 // -1 => broker picks via partitioner
	Key       []byte
	Value     []byte
}

type ProduceResponse struct {
	Topic     string
	Partition int32
	Offset    int64
}

type FetchRequest struct {
	Topic     string
	Partition int32
	Offset    int64
	MaxBytes  int64
}

type FetchResponse struct {
	Topic         string
	Partition     int32
	HighWatermark int64
	Records       []storage.Record
}

type TopicMeta struct {
	Name       string `json:"name"`
	Partitions int32  `json:"partitions"`
}

type MetadataResponse struct {
	Topics []TopicMeta
}

type persistedMeta struct {
	Topics []TopicMeta `json:"topics"`
}

func New(cfg config.Config) (*Broker, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}
	b := &Broker{cfg: cfg, topics: map[string]*topic.Topic{}}
	if err := b.loadMeta(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) metaPath() string {
	return filepath.Join(b.cfg.DataDir, "meta.json")
}

func (b *Broker) loadMeta() error {
	data, err := os.ReadFile(b.metaPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var m persistedMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	for _, tm := range m.Topics {
		t, err := topic.OpenTopic(b.cfg.DataDir, tm.Name, tm.Partitions, b.cfg.SegmentBytes, b.cfg.IndexInterval)
		if err != nil {
			return fmt.Errorf("open topic %q: %w", tm.Name, err)
		}
		b.topics[tm.Name] = t
	}
	return nil
}

func (b *Broker) saveMeta() error {
	m := persistedMeta{}
	for _, t := range b.topics {
		m.Topics = append(m.Topics, TopicMeta{Name: t.Name, Partitions: t.NumPartitions()})
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(b.metaPath(), data, 0644)
}

func (b *Broker) CreateTopic(name string, partitions int32) error {
	if name == "" {
		return errors.New("empty topic name")
	}
	if partitions <= 0 {
		return errors.New("partitions must be > 0")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}
	t, err := topic.OpenTopic(b.cfg.DataDir, name, partitions, b.cfg.SegmentBytes, b.cfg.IndexInterval)
	if err != nil {
		return err
	}
	b.topics[name] = t
	return b.saveMeta()
}

func (b *Broker) Produce(req ProduceRequest) (ProduceResponse, error) {
	b.mu.RLock()
	t, ok := b.topics[req.Topic]
	b.mu.RUnlock()
	if !ok {
		return ProduceResponse{}, fmt.Errorf("unknown topic %q", req.Topic)
	}
	pid := req.Partition
	if pid < 0 {
		pid = topic.PickPartition(req.Key, t.NumPartitions(), t.NextRoundRobin())
	}
	if pid >= t.NumPartitions() {
		return ProduceResponse{}, fmt.Errorf("partition %d out of range for %q (has %d)", pid, req.Topic, t.NumPartitions())
	}
	off, err := t.Partitions[pid].Log.Append(req.Key, req.Value)
	if err != nil {
		return ProduceResponse{}, err
	}
	return ProduceResponse{Topic: req.Topic, Partition: pid, Offset: off}, nil
}

func (b *Broker) Fetch(req FetchRequest) (FetchResponse, error) {
	b.mu.RLock()
	t, ok := b.topics[req.Topic]
	b.mu.RUnlock()
	if !ok {
		return FetchResponse{}, fmt.Errorf("unknown topic %q", req.Topic)
	}
	if req.Partition < 0 || req.Partition >= t.NumPartitions() {
		return FetchResponse{}, fmt.Errorf("partition %d out of range for %q (has %d)", req.Partition, req.Topic, t.NumPartitions())
	}
	p := t.Partitions[req.Partition]
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	recs, err := p.Log.Read(req.Offset, maxBytes)
	if err != nil {
		return FetchResponse{}, err
	}
	return FetchResponse{
		Topic:         req.Topic,
		Partition:     req.Partition,
		HighWatermark: p.Log.NextOffset(),
		Records:       recs,
	}, nil
}

func (b *Broker) Metadata() MetadataResponse {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := MetadataResponse{}
	for _, t := range b.topics {
		out.Topics = append(out.Topics, TopicMeta{Name: t.Name, Partitions: t.NumPartitions()})
	}
	return out
}

func (b *Broker) EnforceRetention() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, t := range b.topics {
		for _, p := range t.Partitions {
			_ = p.Log.EnforceRetention(b.cfg.RetentionBytes, b.cfg.RetentionMS)
		}
	}
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, t := range b.topics {
		for _, p := range t.Partitions {
			if err := p.Log.Sync(); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := p.Log.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
