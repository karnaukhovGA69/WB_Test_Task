#!/usr/bin/env bash
set -euo pipefail

BROKERS="${BROKERS:-localhost:9092}"
TOPIC="${TOPIC:-search-events}"
COUNT="${COUNT:-100000}"
QUERY_POOL_SIZE="${QUERY_POOL_SIZE:-10000}"
USERS="${USERS:-50000}"
INTERVAL="${INTERVAL:-0s}"
BOT_RATIO="${BOT_RATIO:-0.03}"

go run ./cmd/producer \
  -brokers "${BROKERS}" \
  -topic "${TOPIC}" \
  -count "${COUNT}" \
  -query-pool-size "${QUERY_POOL_SIZE}" \
  -users "${USERS}" \
  -bot-ratio "${BOT_RATIO}" \
  -interval "${INTERVAL}"
