package config

import "time"

type Config struct {
	DataDir                string
	TCPListenAddr          string
	GRPCListenAddr         string
	SegmentBytes           int64
	IndexInterval          int64
	RetentionMS            int64
	RetentionBytes         int64
	RetentionCheckInterval time.Duration
}

func Default() Config {
	return Config{
		DataDir:                "data",
		TCPListenAddr:          ":9092",
		GRPCListenAddr:         ":9093",
		SegmentBytes:           1 << 20,
		IndexInterval:          4 * 1024,
		RetentionMS:            7 * 24 * 3600 * 1000,
		RetentionBytes:         100 * (1 << 20),
		RetentionCheckInterval: 30 * time.Second,
	}
}
