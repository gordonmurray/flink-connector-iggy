# flink-connector-iggy

A Flink 1.18 Source Connector for **Apache Iggy (v0.7.0)** with Flink SQL support,
metrics, and TLS.

This project provides a bridge between the Rust-native Iggy event store and the
Apache Flink stream processing ecosystem, targeting Flink 1.18 on Java 17.

---

## Why Build Our Own?

An official Flink connector exists at `org.apache.iggy:flink-connector:0.7.0`,
but it has hard blockers for our use case:

| Issue | Impact |
|---|---|
| Targets **Flink 2.1.1** (uses `WriterInitContext`) | Binary incompatible with Flink 1.18 |
| Jackson 3.x dependency | Classpath conflict with Flink 1.18's bundled Jackson 2.x |
| `autoCommit = true` in poll loop | At-most-once on failure — offset advances before checkpoint |
| `isAvailable()` always returns completed | Busy-polls, burns CPU when idle |
| No Flink SQL support | No `DynamicTableSourceFactory` |
| No Flink metrics | No consumer lag or throughput counters |

This connector addresses all of the above.

---

## Technical Features

- **Flink Source API (v2)**: Built on the FLIP-27 `Source` interface (not legacy `SourceFunction`).
- **Flink SQL**: `CREATE TABLE ... WITH ('connector' = 'iggy')` via `DynamicTableSourceFactory`.
- **Flink 1.18 compatible**: No Flink 2.x API dependencies.
- **Flink metrics**: `numRecordsIn` and `numBytesIn` counters per reader.
- **TLS support**: Optional TLS via `'tls' = 'true'` and `'tls.certificate' = '/path'`.
- **Generic source**: `IggySource<T>` supports any output type via `IggyDeserializationSchema<T>`.
- **Multi-partition**: Discovers partitions at startup and periodically (every 60s) for runtime-added partitions. Round-robin assignment to parallel readers.
- **Correct checkpoint semantics**: Polls with explicit offsets, `autoCommit = false`.
- **Non-blocking backoff**: Empty polls trigger a 100ms delayed retry instead of hanging.
- **Fair polling**: Round-robin across assigned splits prevents partition starvation.
- **TCP binary protocol**: High-throughput connection via `IggyTcpClient`.

---

## Status: All Phases Complete (2026-03-18)

Production-ready source connector. 11/11 tests pass against a live Iggy 0.7.0
instance with the `crypto/prices` topic (~430K messages, 3 partitions).
Testcontainers integration test available for self-contained CI.

---

## Usage

### Flink SQL

```sql
CREATE TABLE iggy_prices (
  pair STRING,
  price STRING,
  best_bid STRING,
  best_ask STRING,
  volume_24h STRING,
  `time` STRING
) WITH (
  'connector' = 'iggy',
  'stream'    = 'crypto',
  'topic'     = 'prices',
  'format'    = 'json'
);

SELECT * FROM iggy_prices;
```

Connection properties (with defaults):

| Property | Default | Required |
|---|---|---|
| `stream` | — | Yes |
| `topic` | — | Yes |
| `format` | — | Yes |
| `host` | `localhost` | No |
| `port` | `8090` | No |
| `username` | `iggy` | No |
| `password` | `iggy` | No |
| `tls` | `false` | No |
| `tls.certificate` | — | No |

### DataStream API (raw bytes)

```java
IggySource<byte[]> source = IggySource.forBytes("crypto", "prices");

DataStream<byte[]> stream = env.fromSource(
    source, WatermarkStrategy.noWatermarks(), "iggy-source");
```

### DataStream API (with deserialization)

```java
IggySource<MyPojo> source = IggySource.<MyPojo>builder()
    .setHost("localhost")
    .setStream("crypto")
    .setTopic("prices")
    .setDeserializer(bytes -> objectMapper.readValue(bytes, MyPojo.class))
    .build();
```

---

## Architecture

| Class | Role |
|---|---|
| `IggySource<T>` | Generic entry point. Implements `Source<T, IggySplit, IggyEnumeratorState>`. Supports TLS config. |
| `IggyDeserializationSchema<T>` | Functional interface for converting `byte[]` to `T`. |
| `IggyDynamicTableFactory` | SPI factory for Flink SQL `'connector' = 'iggy'`. |
| `IggyDynamicTableSource` | Creates `IggySource<RowData>` with format-based deserialization. |
| `IggySplitEnumerator` | Discovers partitions via `getTopic()`, assigns splits round-robin. TLS-aware. |
| `IggyEnumeratorState` | Checkpoint state holding assigned + unassigned split lists. |
| `IggyEnumeratorStateSerializer` | Binary serializer for enumerator checkpoint state. |
| `IggySourceReader<T>` | Polls messages, deserializes, tracks offsets. Emits Flink metrics. TLS-aware. |
| `IggySplit` | Single Iggy partition with mutable offset tracking. |
| `IggySplitSerializer` | Binary serialization of splits for Flink checkpoints. |

---

## Build

No local Maven needed — build and test via Docker:

```bash
# Compile and package (shaded JAR)
docker run --rm -v "$(pwd)":/build -w /build \
  maven:3.9-eclipse-temurin-17 mvn -B package -DskipTests

# Run integration tests (requires running Iggy on localhost:8090)
docker run --rm --network host -e IGGY_HOST=localhost \
  -v "$(pwd)":/build -w /build \
  maven:3.9-eclipse-temurin-17 mvn -B test

# Run Testcontainers test (requires Docker-on-host, not DinD)
# Set TESTCONTAINERS=true and mount Docker socket
```

Produces a shaded JAR at `target/flink-connector-iggy-0.1.0-SNAPSHOT.jar` (12 MB)
that bundles the Iggy SDK + Netty but excludes Flink runtime classes.
