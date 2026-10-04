package topic

import "hash/fnv"

// PickPartition maps a record to a partition. If key is present it's a stable
// hash-mod (so all records with the same key land in the same partition —
// the property consumers rely on to guarantee per-key ordering). Otherwise
// round-robin using the supplied counter.
func PickPartition(key []byte, numPartitions int32, rrCounter uint32) int32 {
	if numPartitions <= 0 {
		return 0
	}
	if len(key) == 0 {
		return int32(rrCounter % uint32(numPartitions))
	}
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(numPartitions))
}
