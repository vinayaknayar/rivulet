package topic

import "rivulet/internal/storage"

// Partition wraps a per-partition log. Kept as its own type so replication,
// per-partition ISR state, and leader/follower roles can be added later
// without disturbing storage or broker code.
type Partition struct {
	ID  int32
	Log *storage.PartitionLog
}
