# Search Trends Service

## Project Overview

Search Trends Service is a Go backend service for a marketplace widget similar to "Currently searched".

The service consumes a stream of search events from Kafka and exposes Top-N most popular search queries for the last 5 minutes through a gRPC API.

The design is optimized for a read-heavy workload where `GetTop` requests are expected to be 10-50x more frequent than incoming Kafka events. Reads are served from a cached Top-N snapshot, while the exact aggregation state is maintained in memory using a sliding window.

Main properties:

- Fast `GetTop` through cached Top-N.
- In-memory sliding window for the last 5 minutes.
- Kafka consumer with manual commit after processing.
- Dynamic stop-list through gRPC without service restart.
- Anti-abuse limiter extension point and metrics.
- Prometheus observability.
- Docker Compose for local development.
- Go benchmarks and `ghz` load-test scripts.

## Problem Statement

Marketplace search traffic is represented as a continuous event stream. The product widget must show users what is popular "right now", meaning the most frequent search queries in a recent rolling time window.

The service must:

- read search events from Kafka;
- validate and normalize input;
- aggregate query counts for the last 5 minutes;
- expose a low-latency `GetTop` API;
- support dynamic stop-list updates;
- protect aggregation from obviously abusive traffic;
- provide metrics and reproducible performance checks;
- run locally without managed cloud services.

## Features

- Kafka JSON consumer based on `github.com/segmentio/kafka-go`.
- Manual Kafka commit after successful processing or intentional invalid-message discard.
- In-memory exact aggregation.
- Sliding window with 1-second buckets.
- Cached Top-N snapshot for cheap reads.
- gRPC API:
  - `GetTop`
  - `AddStopWord`
  - `RemoveStopWord`
  - `ListStopWords`
  - `HealthCheck`
- Dynamic stop-list stored as an atomic copy-on-write snapshot.
- Anti-abuse limiter interface with Prometheus metrics.
- Prometheus metrics endpoint.
- Local Kafka producer for testing.
- Docker Compose with Kafka, Zookeeper, Kafka UI and Prometheus.
- Unit tests, race-test friendly code and benchmarks.
- `ghz` load-test scripts.

## Tech Stack

- Go
- Kafka
- gRPC
- Protocol Buffers
- Prometheus
- Docker Compose
- `github.com/segmentio/kafka-go`
- `github.com/prometheus/client_golang`
- `go.uber.org/zap`

`kafka-go` was chosen because it is pure Go, does not require CGO, is easy to run in Docker Compose, and is sufficient for this test highload scenario.

## Architecture

```text
Kafka -> Consumer -> Decoder/Validator -> StopList -> AntiSpam -> Aggregator -> Top Cache -> gRPC API
                                                       |
                                                       v
                                                Prometheus Metrics
```

Components:

- `Kafka Consumer`: reads messages from Kafka, decodes JSON, handles errors and commits offsets manually.
- `Decoder/Validator`: validates required fields and parses RFC3339 timestamps.
- `Aggregator`: owns the in-memory sliding window and global query counters.
- `Sliding Window Buckets`: 300 buckets with 1-second granularity for a 5-minute window.
- `Global Counters`: exact query counts for all active buckets.
- `Top Cache`: immutable cached Top-N snapshot used by `GetTop`.
- `StopList`: dynamic copy-on-write list of blocked queries.
- `AntiSpam Limiter`: actor/query check layer for abuse protection and future rolling counters.
- `gRPC Server`: public API for reads, stop-list operations and health checks.
- `Metrics Server`: HTTP server exposing `/metrics` and `/healthz`.

## Data Flow

1. Search event is published to Kafka topic `search-events`.
2. Consumer fetches the message.
3. JSON decoder parses payload and validates required fields.
4. Stop-list rejects blocked queries.
5. Anti-abuse limiter checks the actor/query pair.
6. Aggregator normalizes the query and adds it to the sliding window.
7. Background rebuild refreshes the cached Top-N snapshot.
8. gRPC `GetTop` returns data from the cached snapshot.
9. Prometheus scrapes service metrics from `/metrics`.

## Quick Start

Start the full local stack:

```bash
make docker-up
```

Send test events:

```bash
go run ./cmd/producer -query "iphone 15" -count 100
```

Read Top-N:

```bash
grpcurl -plaintext \
  -d '{"limit":10}' \
  localhost:9090 \
  search_trends.v1.SearchTrendsService/GetTop
```

Open metrics:

```bash
curl http://localhost:2112/metrics
```

Stop local stack:

```bash
make docker-down
```

## Configuration

Example config:

```yaml
server:
  grpc_addr: ":9090"

metrics:
  host: "0.0.0.0"
  port: 2112
  path: "/metrics"

kafka:
  brokers:
    - "localhost:9092"
  topic: "search-events"
  group_id: "search-trends-service"
  client_id: "search-trends-service"
  min_bytes: 1
  max_bytes: 10485760
  max_wait_ms: 1000
  commit_interval_ms: 0
  start_offset: "latest"

aggregator:
  window: 5m
  bucket_size: 1s
  default_limit: 10
  max_limit: 100
  top_refresh_interval: 500ms
  max_query_runes: 256
  allowed_future_skew: 5s
```

Configuration files:

- `config.example.yaml`: local host configuration.
- `config.docker.yaml`: Docker Compose configuration.

`commit_interval_ms: 0` means synchronous manual commit after successful processing.

## Kafka Event Contract

Topic:

```text
search-events
```

Payload format: JSON.

```json
{
  "event_id": "01HZY6Y4JQ9X8V9V7K2X3M4N5P",
  "query": "iPhone 15 Pro",
  "user_id": "user-123",
  "session_id": "session-456",
  "ip": "192.168.1.10",
  "user_agent": "Mozilla/5.0",
  "timestamp": "2026-05-23T12:00:00Z"
}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `event_id` | yes | Logging, tracing and potential deduplication key. |
| `query` | yes | Raw search query. Normalization is performed inside the aggregator. |
| `user_id` | no | Stable actor id for anti-abuse checks. |
| `session_id` | no | Anonymous/session actor id for anti-abuse checks. |
| `ip` | no | Fallback actor key for anonymous traffic and abuse detection. |
| `user_agent` | no | Input for future anti-bot heuristics. |
| `timestamp` | yes | RFC3339/RFC3339Nano event time used for 5-minute aggregation. |

Invalid payload behavior:

- invalid JSON: log, commit, continue;
- missing `event_id`: log, commit, continue;
- empty `query`: log, commit, continue;
- invalid `timestamp`: log, commit, continue;
- aggregator business validation rejection: log, commit, continue.

## gRPC API

Proto file:

```text
proto/search_trends/v1/search_trends.proto
```

Generate protobuf code:

```bash
make proto
```

Methods:

```proto
service SearchTrendsService {
  rpc GetTop(GetTopRequest) returns (GetTopResponse);
  rpc AddStopWord(StopWordRequest) returns (StopWordResponse);
  rpc RemoveStopWord(StopWordRequest) returns (StopWordResponse);
  rpc ListStopWords(ListStopWordsRequest) returns (ListStopWordsResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

Example `GetTop`:

```bash
grpcurl -plaintext \
  -d '{"limit":10}' \
  localhost:9090 \
  search_trends.v1.SearchTrendsService/GetTop
```

Response shape:

```json
{
  "items": [
    {
      "query": "iphone 15",
      "count": "100"
    }
  ],
  "generatedAt": "2026-05-23T12:00:00Z",
  "windowSeconds": 300
}
```

## Stop-List API

Add stop word:

```bash
grpcurl -plaintext \
  -d '{"word":"spam"}' \
  localhost:9090 \
  search_trends.v1.SearchTrendsService/AddStopWord
```

Remove stop word:

```bash
grpcurl -plaintext \
  -d '{"word":"spam"}' \
  localhost:9090 \
  search_trends.v1.SearchTrendsService/RemoveStopWord
```

List stop words:

```bash
grpcurl -plaintext \
  -d '{}' \
  localhost:9090 \
  search_trends.v1.SearchTrendsService/ListStopWords
```

Stop-list implementation:

- stored in memory;
- normalized before matching;
- copy-on-write updates;
- atomic read path;
- no restart required.

## Anti-Abuse Logic

The service has a dedicated anti-abuse layer in the Kafka processing path. It receives:

- `event_id`
- `query`
- `user_id`
- `session_id`
- `ip`
- `timestamp`

The current implementation provides the limiter interface, metrics and integration points. It is intentionally isolated from Kafka and Aggregator code so that rolling counters and deduplication can be extended without changing transport or aggregation logic.

Current behavior:

- every event is checked through the limiter;
- allowed/blocked decisions are exported as Prometheus metrics;
- active key gauge is exposed;
- rejected events can be counted as `rate_limited`.

Production extension:

- event-id TTL deduplication;
- per-user and per-session rolling counters;
- per-actor/query limits;
- stricter anonymous IP-based limits;
- user-agent heuristics.

## Sliding Window Aggregation

The aggregator uses exact in-memory counting.

Configuration:

- window: `5m`;
- bucket size: `1s`;
- bucket count: `300`;
- bucket structure: `map[string]int64`;
- global counters: `map[string]int64`;
- synchronization: `sync.RWMutex`.

On add:

1. Validate timestamp.
2. Normalize query.
3. Ignore events older than the active window.
4. Reject events too far in the future.
5. Expire old buckets.
6. Add query count to the correct second bucket.
7. Increment global counter.

On expiration:

1. Find buckets older than the window.
2. Subtract bucket counts from global counters.
3. Delete zero or negative global counters.
4. Clear bucket.

## Top Cache Strategy

`GetTop` is optimized for frequent reads.

Instead of calculating Top-N on every request:

- writes update buckets and global counters;
- a background loop rebuilds cached Top-N periodically;
- `GetTop` reads from the cached snapshot;
- response allocation is limited to copying the requested result slice.

Top rebuild:

- scans global counters;
- keeps Top-N using a min-heap;
- sorts final result by count descending and query ascending;
- stores generated timestamp and window size.

This is a good fit for read-heavy traffic because rebuild work is decoupled from the hot read path.

## Prometheus Monitoring

Metrics endpoint:

```text
http://localhost:2112/metrics
```

Prometheus UI:

```text
http://localhost:9091
```

Key metrics:

- `search_trends_events_total{status}`
- `search_trends_kafka_messages_total{status}`
- `search_trends_kafka_processing_duration_seconds`
- `search_trends_kafka_last_message_timestamp_seconds`
- `search_trends_kafka_decode_errors_total`
- `search_trends_kafka_commit_errors_total`
- `search_trends_kafka_read_errors_total`
- `search_trends_grpc_requests_total{method,code}`
- `search_trends_grpc_request_duration_seconds{method}`
- `search_trends_unique_queries`
- `search_trends_top_cache_size`
- `search_trends_top_rebuild_duration_seconds`
- `search_trends_top_rebuild_total{status}`
- `search_trends_window_buckets_total`
- `search_trends_current_window_seconds`
- `search_trends_stoplist_size`
- `search_trends_stoplist_operations_total{operation,status}`
- `search_trends_antispam_checks_total{result}`
- `search_trends_antispam_active_keys`

Local checks:

```bash
curl http://localhost:2112/metrics
curl -s http://localhost:2112/metrics | grep search_trends_events_total
curl -s http://localhost:2112/metrics | grep search_trends_unique_queries
```

Metric labels intentionally do not include query, user id, session id or IP to avoid unbounded cardinality.

## Performance Testing

Go benchmarks:

```bash
go test -bench=. -benchmem ./...
```

Focused benchmarks:

```bash
go test -bench='BenchmarkAggregator' -benchmem ./internal/aggregator
go test -bench='BenchmarkStopListContains' -benchmem ./internal/stoplist
go test -bench='BenchmarkAntiSpamAllow' -benchmem ./internal/antispam
```

gRPC load test for cached `GetTop`:

```bash
GRPC_ADDR=localhost:9090 \
TOTAL=100000 \
CONCURRENCY=200 \
CONNECTIONS=50 \
./scripts/loadtest_get_top.sh
```

Kafka ingestion load:

```bash
BROKERS=localhost:9092 \
TOPIC=search-events \
COUNT=100000 \
QUERY_POOL_SIZE=10000 \
USERS=50000 \
./scripts/produce_load.sh
```

Combined scenario:

```bash
COUNT=100000 \
TOTAL=100000 \
CONCURRENCY=200 \
CONNECTIONS=50 \
./scripts/perf_scenario.sh
```

Reports are written to:

```text
reports/loadtest
```

## Unit Tests and Benchmarks

Run tests:

```bash
go test ./...
```

Run race tests:

```bash
go test -race ./...
```

Run benchmarks:

```bash
go test -bench=. -benchmem ./...
```

Covered areas:

- query normalization;
- sliding window aggregation;
- bucket expiration;
- Top-N ordering;
- config loading;
- Kafka decoder;
- Kafka consumer behavior with fake reader;
- gRPC handlers;
- gRPC metrics interceptor;
- metrics HTTP endpoint.

## Docker Compose Services

Compose services:

| Service | Description |
|---|---|
| `app` | Go service with gRPC and metrics endpoints. |
| `zookeeper` | Kafka dependency for Confluent Kafka image. |
| `kafka` | Local Kafka broker. |
| `kafka-init` | Creates topic `search-events`. |
| `kafka-ui` | Browser UI for local Kafka inspection. |
| `prometheus` | Scrapes service metrics. |

Ports:

| Endpoint | URL |
|---|---|
| gRPC | `localhost:9090` |
| Metrics | `http://localhost:2112/metrics` |
| Prometheus | `http://localhost:9091` |
| Kafka | `localhost:9092` |
| Kafka UI | `http://localhost:8080` |

## Trade-Offs

- In-memory aggregation gives fast reads and simple exact counting, but state is lost on restart.
- Kafka remains the source event log; restoring the last 5 minutes by replay is possible but not fully implemented in this MVP.
- `GetTop` can be stale by up to `top_refresh_interval`, which is acceptable for a realtime widget and keeps reads cheap.
- Exact counters require memory proportional to unique queries in the active 5-minute window.
- A single service instance produces exact local Top-N. Horizontal scaling needs either replicated consumers or an additional merge layer.
- Stop-list is in-memory and dynamic, but not persisted across restarts.
- Consumer lag is not exported in the MVP. Accurate consumer group lag requires comparing committed offsets with partition high-water marks through additional Kafka calls; a fake lag from fetched messages would be misleading.
- `TopRebuildDuration` is more important than `GetTop` latency internally because `GetTop` only reads cached data.

## Known Limitations

- No persistent state for aggregation or stop-list.
- No warm-up replay from Kafka on service restart.
- Anti-abuse limiter currently provides integration points and metrics; production-grade rolling counters and event-id deduplication are listed as future work.
- No authentication/authorization on gRPC admin stop-list methods.
- No TLS for gRPC in local setup.
- No multi-region or multi-instance global Top-N merge.

## Future Improvements

- Implement Kafka replay from `now - 5m` on startup.
- Add event-id TTL deduplication.
- Add rolling per-actor and per-actor/query anti-abuse counters.
- Persist stop-list in a durable store or compacted Kafka topic.
- Add auth for stop-list admin methods.
- Add dashboards for Prometheus metrics.
- Add CI pipeline with test, race test, lint and Docker build.
- Add approximate Top-K mode for extreme query cardinality.
- Add horizontal merge-layer design for multiple aggregator instances.
