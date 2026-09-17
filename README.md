# flink-connector-iggy

A Flink Source Connector for **Apache Iggy (v0.8.0)** with Flink SQL support,
metrics, and TLS.

Tested on **Flink 1.18.1** and **Flink 1.20.3** (LTS). Built on the FLIP-27
Source v2 API, which is binary compatible across Flink 1.18 → 1.20.

---

## Compatibility

| Component | Tested Versions |
|---|---|
| **Flink** | 1.18.1, 1.20.3 (LTS) |
| **Iggy Server** | 0.8.0 |
| **Iggy Java SDK** | `org.apache.iggy:iggy:0.8.0` |
| **Java** | 17+ |

The connector JAR is compiled with Java 17. Use a `-java17` Flink Docker image
(e.g., `flink:1.20.3-java17`) or ensure your Flink cluster runs Java 17+.

---

## Why Build Our Own?

An official Flink connector exists at `org.apache.iggy:flink-connector:0.7.0`,
but it has hard blockers:

| Issue | Impact |
|---|---|
| Targets **Flink 2.1.1** (uses `WriterInitContext`) | Binary incompatible with Flink 1.x |
| Jackson 3.x dependency | Classpath conflict with Flink 1.x's bundled Jackson 2.x |
| `autoCommit = true` in poll loop | At-most-once on failure — offset advances before checkpoint |
| `isAvailable()` always returns completed | Busy-polls, burns CPU when idle |
| No Flink SQL support | No `DynamicTableSourceFactory` |
| No Flink metrics | No consumer lag or throughput counters |

This connector addresses all of the above.

---

## Technical Features

- **Flink Source API (v2)**: FLIP-27 `Source` interface, compatible across Flink 1.18–1.20.
- **Flink SQL**: `CREATE TABLE ... WITH ('connector' = 'iggy')` via `DynamicTableSourceFactory`.
- **Flink metrics**: `numRecordsIn`, `numBytesIn` and `staleOffsetResets` counters per reader.
- **TLS support**: Optional TLS via `'tls' = 'true'` and `'tls.certificate' = '/path'`.
- **Generic source**: `IggySource<T>` supports any output type via `IggyDeserializationSchema<T>`.
- **Multi-partition**: Discovers partitions at startup and periodically (configurable) for runtime-added partitions. Round-robin assignment to parallel readers.
- **Correct checkpoint semantics**: Polls with explicit offsets, `autoCommit = false`.
- **Non-blocking backoff**: Empty polls trigger a configurable delayed retry instead of hanging.
- **Fair polling**: Round-robin across assigned splits prevents partition starvation.
- **Failure handling**: a poll that fails, or gets no response within `poll.timeout`, discards the connection and reconnects (an empty partition answers immediately, so it never hits the timeout). A failed request is never retried on the same connection, because the SDK matches responses to requests in order. The same split failing 3 times in a row, or 3 failed reconnects, fails the task so Flink restarts it, instead of a dead connection looking like an empty partition.
- **Connections are released**: readers and the enumerator close their Iggy connection on shutdown and task restart. Iggy Java SDK 0.7.0 has no close method, so the connector disposes the underlying connection itself.
- **TCP binary protocol**: High-throughput connection via `IggyTcpClient`.

---

## Usage

### Flink SQL

```sql
SET 'pipeline.jars' = 'file:///opt/flink/lib/flink-connector-iggy.jar';

CREATE TABLE iggy_prices (
  pair STRING,
  price STRING,
  best_bid STRING,
  best_ask STRING,
  volume_24h STRING,
  `time` STRING
) WITH (
  'connector' = 'iggy',
  'host'      = 'iggy',
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
| `starting-offset` | `earliest` | No |
| `poll.timeout` | `5000` | No |
| `stale-offset` | `earliest` | No |

The `starting-offset` property controls where new splits begin consuming:
- `earliest` (default) — start from the beginning of the partition
- `latest` — start from the current end, skipping existing messages
- `specific-offset:<long>` — start from a specific offset (e.g., `specific-offset:48291`)

The `stale-offset` property controls what a reader does when an offset restored from a
checkpoint is beyond the end of its partition. That only happens when the topic was
deleted and recreated, or purged, after the checkpoint: Iggy restarts the partition at
offset 0 and answers a poll past the end with an empty batch, not an error. Without
this check the job looked healthy, waited until the new partition grew past the old
offset, and then silently skipped every message below it.
- `earliest` (default) — re-read the partition from offset 0; everything in a recreated topic is new
- `latest` — skip to the current end of the partition
- `fail` — fail the task with an explanation (restarts fail again until the job is resubmitted)

A split that is still on a configured `specific-offset` beyond the end is left alone
until it has read something, since starting there was deliberate. Each reset is logged as
a warning and counted in the reader's `staleOffsetResets` metric.

**Upgrading from 0.2.0 or earlier:** checkpoints written before `stale-offset` existed do not
record that a split is deliberately waiting at a future `specific-offset`. If you restore one
while a split is still waiting there (its partition has not reached that offset yet), the
stale-offset policy applies to it. Let the partition reach the offset first, or resubmit the
job instead of restoring it.

```sql
-- Start from latest (skip historical messages)
CREATE TABLE iggy_traffic (
  ...
) WITH (
  'connector'       = 'iggy',
  'stream'          = 'web-traffic',
  'topic'           = 'requests',
  'format'          = 'json',
  'starting-offset' = 'latest'
);

-- Start from a specific offset (replay use case)
CREATE TABLE iggy_traffic_replay (
  ...
) WITH (
  'connector'       = 'iggy',
  'stream'          = 'web-traffic',
  'topic'           = 'requests',
  'format'          = 'json',
  'starting-offset' = 'specific-offset:48291'
);
```

**Note:** When running Flink in Docker, set `'host'` to the Iggy container name
(e.g., `'host' = 'iggy'`), not `localhost`.

---

## Working Example

A ready-to-run Docker Compose quickstart is available at
[flink-iggy-quickstart](https://github.com/gordonmurray/flink-iggy-quickstart).
It spins up Iggy, a Python producer, and a Flink cluster with this connector
pre-loaded so you can try Flink SQL against a live Iggy stream in under a minute.

---

## Architecture

| Class | Role |
|---|---|
| `IggySource<T>` | Generic entry point. Builder pattern with configurable batch size, consumer ID, poll backoff, and discovery interval. |
| `IggyConnectionConfig` | Immutable connection config record shared by reader and enumerator. Single `connect()` factory method. |
| `IggyDeserializationSchema<T>` | Functional interface for converting `byte[]` to `T`. |
| `IggyDynamicTableFactory` | SPI factory for Flink SQL `'connector' = 'iggy'`. |
| `IggyDynamicTableSource` | Creates `IggySource<RowData>` with format-based deserialization. |
| `IggySplitEnumerator` | Discovers partitions, assigns splits round-robin, periodic re-scan. |
| `IggyEnumeratorState` | Checkpoint state record holding assigned + unassigned split lists. |
| `IggySourceReader<T>` | Round-robin polling, transient error handling, Flink metrics. |
| `IggySplit` | Single Iggy partition with mutable offset tracking. |

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
```

Produces a shaded JAR at `target/flink-connector-iggy-0.1.0-SNAPSHOT.jar` (12 MB)
that bundles the Iggy SDK + Netty but excludes Flink runtime classes.

### Deployment

Mount the JAR into your Flink TaskManager and JobManager:

```yaml
# docker-compose.yml
jobmanager:
  image: flink:1.20.3-java17
  volumes:
    - ./jars/flink-connector-iggy.jar:/opt/flink/lib/flink-connector-iggy.jar:ro

taskmanager:
  image: flink:1.20.3-java17
  volumes:
    - ./jars/flink-connector-iggy.jar:/opt/flink/lib/flink-connector-iggy.jar:ro
```