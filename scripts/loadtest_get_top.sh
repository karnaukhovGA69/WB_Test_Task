#!/usr/bin/env bash
set -euo pipefail

if ! command -v ghz >/dev/null 2>&1; then
  echo "ghz is required: https://ghz.sh/docs/install" >&2
  exit 1
fi

GRPC_ADDR="${GRPC_ADDR:-localhost:9090}"
TOTAL="${TOTAL:-100000}"
CONCURRENCY="${CONCURRENCY:-200}"
CONNECTIONS="${CONNECTIONS:-50}"
REPORT_DIR="${REPORT_DIR:-reports/loadtest}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT="${REPORT_DIR}/get_top_${TIMESTAMP}.json"

mkdir -p "${REPORT_DIR}"

ghz \
  --insecure \
  --proto proto/search_trends/v1/search_trends.proto \
  --call search_trends.v1.SearchTrendsService.GetTop \
  --data '{"limit":10}' \
  --connections "${CONNECTIONS}" \
  --concurrency "${CONCURRENCY}" \
  --total "${TOTAL}" \
  --format json \
  --output "${OUTPUT}" \
  "${GRPC_ADDR}"

echo "GetTop load test report: ${OUTPUT}"
