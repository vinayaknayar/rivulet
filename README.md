# rivulet

A minimal Kafka-inspired message streaming broker in Go. Single node, persistent, dual-protocol (custom TCP **and** gRPC). Built to learn Kafka internals, and layered so that multi-broker / replication / consumer groups can be added later without rewriting the storage core.

## What it does today

- **Persistent append-only log** per partition, made of rolling `.log` segments + sparse `.index` files — the same shape as Kafka's storage layer.
- **Topics with N partitions**; producer picks partition by key hash (stable) or round-robin.
- **Two client protocols talking to the same broker core**:
  - Custom TCP binary protocol (length-prefixed frames).
  - gRPC — `Fetch` is a server-streaming RPC so consumers get real-time push.
- **CRC32 per record**, torn-write recovery on restart, retention by bytes and by age.
- **Crash-safe replay**: restart the broker, everything replays from disk with the same offsets.

## Architecture

```
    producer CLI (TCP)      consumer CLI (gRPC)
             │                       │
     ┌───────▼──────┐        ┌───────▼──────┐
     │  TCP Server  │        │ gRPC Server  │
     └───────┬──────┘        └───────┬──────┘
             └────────────┬──────────┘
                          ▼
                 ┌────────────────┐
                 │     Broker     │  ← protocol-neutral core
                 │  (topics,      │
                 │   partitions)  │
                 └────────┬───────┘
                          ▼
                 ┌────────────────┐
                 │ PartitionLog   │
                 │  segments +    │
                 │  sparse index  │
                 └────────┬───────┘
                          ▼
              data/<topic>/<partition>/
                00000000000000000000.log
                00000000000000000000.index
```

Both the TCP and gRPC edges convert wire messages into the same protocol-neutral request types (`ProduceRequest`, `FetchRequest`, `MetadataRequest`) and call the same `Broker` methods. Adding another edge is one file.

## Directory layout

```
cmd/
  broker/     starts TCP + gRPC listeners
  producer/   TCP CLI: create topics, produce from stdin
  consumer/   gRPC CLI: server-streaming consumer
internal/
  storage/    the crown jewel: segments, sparse index, partition_log
  topic/      topics, partitions, partitioner
  broker/     protocol-neutral Broker + request types
  protocol/
    tcp/      binary framing + server
    grpc/     server impl over generated proto
  config/     defaults
api/proto/    broker.proto + generated Go
```

## On-disk record format

Each record in a `.log` file is framed as:

```
[recordSize:4]                         ← size of what follows
[offset:8][timestamp:8]
[keySize:4 (-1 for nil)][key: keySize]
[valueSize:4 (-1 for nil)][value: valueSize]
[crc:4]                                ← CRC32 IEEE of everything above
```

Segments roll over at `--segment-bytes` (default 1 MiB — tune up for real usage). Filenames are the segment's base offset zero-padded to 20 digits.

## TCP wire protocol

```
[totalLen:4][apiKey:2][correlationId:4][payload...]

API keys: 0 PRODUCE  1 FETCH  2 METADATA  3 CREATE_TOPIC

Every response payload starts with:
  [errCode:2][errMsgLen:2][errMsg]
followed by API-specific body when errCode == 0.
```

Full spec is in [internal/protocol/tcp/codec.go](internal/protocol/tcp/codec.go).

## Quick start

```bash
# 1. Build
go build ./...

# 2. Start the broker (in one terminal)
go run ./cmd/broker

# 3. Create a topic (in another terminal)
go run ./cmd/producer -topic events -create-topic -partitions 3

# 4. Produce some messages
echo -e "hello\nworld\nfrom rivulet" | go run ./cmd/producer -topic events

# 5. Consume partition 0, streaming forever (Ctrl-C to stop)
go run ./cmd/consumer -topic events -partition 0
```

### Full end-to-end demo

```bash
./demo.sh
```

The script starts the broker, creates a topic with 3 partitions, produces 30 messages, consumes each partition, kills the broker, restarts it, and re-reads from offset 0 to prove durability.

## Tests

```bash
go test ./...
```

Covers segment rollover, restart recovery, retention, and the TCP codec.

## Where the extension points live

- **Replication** → add a `Replicator` between `Broker.Produce` and `Partition.Log.Append` that forks writes to follower brokers. The log layer doesn't change.
- **Multi-broker** → add a controller service that owns topic/partition-leader metadata; brokers register with it. Metadata RPCs already exist for clients to discover partition placement.
- **Consumer groups** → add a `GroupCoordinator` that assigns partitions and commits offsets to an internal `__offsets` topic — which works because we already have persistent topics.
- **Zero-copy sendfile** → `Segment.Read` currently copies through user space; the read path can be swapped for `sendfile(2)` on Linux without changing callers.
