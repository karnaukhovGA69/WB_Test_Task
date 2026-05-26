# Load Testing Reports

This directory stores local benchmark and load-test outputs. Generated JSON reports are ignored by Git except this README.

## Go benchmarks

Run all package benchmarks:

```bash
go test -bench=. -benchmem ./...
```

Useful focused runs:

```bash
go test -bench='BenchmarkAggregator' -benchmem ./internal/aggregator
go test -bench='BenchmarkStopListContains' -benchmem ./internal/stoplist
go test -bench='BenchmarkAntiSpamAllow' -benchmem ./internal/antispam
```

## gRPC load test

Install `ghz` first:

```bash
go install github.com/bojand/ghz/cmd/ghz@latest
```

Run cached Top-N read test:

```bash
GRPC_ADDR=localhost:9090 TOTAL=100000 CONCURRENCY=200 CONNECTIONS=50 ./scripts/loadtest_get_top.sh
```

Run stop-list admin endpoint test:

```bash
GRPC_ADDR=localhost:9090 TOTAL=10000 CONCURRENCY=50 CONNECTIONS=10 ./scripts/loadtest_stoplist.sh
```

## Kafka ingestion test

Produce a realistic mixed workload:

```bash
BROKERS=localhost:9092 \
TOPIC=search-events \
COUNT=100000 \
QUERY_POOL_SIZE=10000 \
USERS=50000 \
INTERVAL=0s \
./scripts/produce_load.sh
```

The producer uses a weighted query pool: popular queries appear more frequently than rare queries, with a small configurable bot-like traffic share from one actor key.

## Full performance scenario

```bash
COUNT=100000 TOTAL=100000 CONCURRENCY=200 CONNECTIONS=50 ./scripts/perf_scenario.sh
```

The scenario starts Docker Compose when the metrics endpoint is unavailable, produces an initial Kafka dataset, waits for the consumer, then runs the `GetTop` gRPC load test.

## What to watch

- `search_trends_grpc_requests_total`
- `search_trends_grpc_request_duration_seconds`
- `search_trends_events_total`
- `search_trends_kafka_messages_total`
- `search_trends_unique_queries`
- `search_trends_top_rebuild_duration_seconds`

## Sample results

Run on: `<machine/cpu/ram>`
Date: `<date>`

| Scenario | RPS | Avg latency | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|
| GetTop cached read | TBD | TBD | TBD | TBD | TBD |
| Kafka ingestion | TBD | TBD | TBD | TBD | TBD |
