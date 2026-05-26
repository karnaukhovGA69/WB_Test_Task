#!/usr/bin/env bash
set -euo pipefail

if ! command -v ghz >/dev/null 2>&1; then
  echo "ghz is required: https://ghz.sh/docs/install" >&2
  exit 1
fi

GRPC_ADDR="${GRPC_ADDR:-localhost:9090}"
TOTAL="${TOTAL:-10000}"
CONCURRENCY="${CONCURRENCY:-50}"
CONNECTIONS="${CONNECTIONS:-10}"
REPORT_DIR="${REPORT_DIR:-reports/loadtest}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

mkdir -p "${REPORT_DIR}"

ghz \
  --insecure \
  --proto proto/search_trends/v1/search_trends.proto \
  --call search_trends.v1.SearchTrendsService.AddStopWord \
  --data '{"word":"loadtest-stop-word"}' \
  --connections "${CONNECTIONS}" \
  --concurrency "${CONCURRENCY}" \
  --total "${TOTAL}" \
  --format json \
  --output "${REPORT_DIR}/add_stop_word_${TIMESTAMP}.json" \
  "${GRPC_ADDR}"

ghz \
  --insecure \
  --proto proto/search_trends/v1/search_trends.proto \
  --call search_trends.v1.SearchTrendsService.ListStopWords \
  --data '{}' \
  --connections "${CONNECTIONS}" \
  --concurrency "${CONCURRENCY}" \
  --total "${TOTAL}" \
  --format json \
  --output "${REPORT_DIR}/list_stop_words_${TIMESTAMP}.json" \
  "${GRPC_ADDR}"

echo "Stop-list load test reports written to ${REPORT_DIR}"
