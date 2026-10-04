# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build all binaries into ./bin/
go build -o bin/broker ./cmd/broker
go build -o bin/producer ./cmd/producer
go build -o bin/consumer ./cmd/consumer

# Run all tests
go test ./...

# Run a single test or package
go test ./internal/storage/...
go test -run TestSegmentRollover ./internal/storage/

# Verbose output
go test -v ./...

# Run the full end-to-end demo (builds, starts broker, produces, consumes, restarts, verifies durability)
./demo.sh

# Start the broker directly (defaults: TCP :9092, gRPC :9093, data in ./data/)
go run ./cmd/broker -data ./data -tcp :9092 -grpc :9093

# Regenerate protobuf (requires protoc + protoc-gen-go + protoc-gen-go-grpc)
protoc --go_out=. --go-grpc_out=. api/proto/broker.proto
```

## Architecture

The codebase is layered in a strict dependency direction: protocol edges → broker core → topic/partition → storage. No layer reaches upward.

### Storage layer (`internal/storage/`)

The foundational piece. A **Segment** is one append-only `.log` file paired with a sparse `.index` file. The index maps relative offsets to byte positions (one entry per ~4 KiB of log by default) so reads binary-search the small index then scan forward in the large log — the same approach as Kafka's storage engine.

On-disk record framing per `.log` file:
```
[recordSize:4][offset:8][timestamp:8][keySize:4][key][valueSize:4][value][crc32:4]
```

On-disk index entries are `[relOffset:4][filePos:4]` (8 bytes each, big-endian).

A **PartitionLog** chains multiple segments. Writes go to the active (last) segment; when it exceeds `SegmentBytes` (default 1 MiB) a new segment rolls with `baseOffset = activeSegment.NextOffset()`. Segment filenames are the base offset zero-padded to 20 digits.

**Crash recovery**: `Segment.recover()` on open scans forward from the last index entry, finds the true end of valid records, and truncates any torn write. This means `nextOffset` and `position` are always consistent after restart, no WAL needed.

### Broker core (`internal/broker/broker.go`)

Protocol-neutral. Holds a `map[string]*topic.Topic` guarded by a single `sync.RWMutex`. Exposes `CreateTopic`, `Produce`, `Fetch`, `Metadata`, `EnforceRetention`. Topic/partition metadata is persisted in `<dataDir>/meta.json` and reloaded on startup.

`Produce` with `Partition == -1` lets the partitioner choose: keyed messages hash to a stable partition; keyless messages round-robin.

`Fetch` returns one segment's worth of records at a time (Kafka semantics) — callers re-call with the next offset to continue.

### Protocol edges

**TCP** (`internal/protocol/tcp/`): custom binary framing. Every frame is `[totalLen:4][apiKey:2][correlationID:4][payload...]`. API keys 0–3 map to PRODUCE/FETCH/METADATA/CREATE_TOPIC. `codec.go` provides `Buf` (append-only encoder) and `Reader` (cursor-based decoder) used by `server.go` to parse requests and write responses. Every response payload starts with `[errCode:2][errMsgLen:2][errMsg]`.

**gRPC** (`internal/protocol/grpc/server.go`): thin translation layer over `api/proto/broker.proto`. The `Fetch` RPC is server-streaming — it drains existing records then polls every 200 ms for new ones when `follow=true`, pushing each record to the client until the context is cancelled.

Both edges convert wire types to `broker.ProduceRequest` / `broker.FetchRequest` and call the same `Broker` methods. Adding a new protocol edge is one file.

### Extension points (by design)

- **Replication**: insert a `Replicator` between `Broker.Produce` and `Partition.Log.Append` — the storage layer is unchanged.
- **Consumer groups**: add a `GroupCoordinator` that commits offsets to an internal `__offsets` topic (already supported since topics are just persistent logs).
- **Multi-broker**: add a controller that owns leader metadata; metadata RPCs already exist for client discovery.
- **Zero-copy reads**: `Segment.Read` currently copies through user space; swap for `sendfile(2)` on Linux without changing callers.

### Config defaults (`internal/config/config.go`)

| Field | Default |
|---|---|
| `SegmentBytes` | 1 MiB |
| `IndexInterval` | 4 KiB |
| `RetentionMS` | 7 days |
| `RetentionBytes` | 100 MiB |
| `RetentionCheckInterval` | 30s |
