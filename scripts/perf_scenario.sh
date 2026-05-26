#!/usr/bin/env bash
set -euo pipefail

GRPC_ADDR="${GRPC_ADDR:-localhost:9090}"
METRICS_URL="${METRICS_URL:-http://localhost:2112/metrics}"
REPORT_DIR="${REPORT_DIR:-reports/loadtest}"
COUNT="${COUNT:-100000}"
TOTAL="${TOTAL:-100000}"
CONCURRENCY="${CONCURRENCY:-200}"
CONNECTIONS="${CONNECTIONS:-50}"
WAIT_AFTER_PRODUCE="${WAIT_AFTER_PRODUCE:-5}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
REPORT="${REPORT_DIR}/perf_scenario_get_top_${TIMESTAMP}.json"

mkdir -p "${REPORT_DIR}"

if ! curl -fsS "${METRICS_URL}" >/dev/null 2>&1; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "service is not reachable at ${METRICS_URL}, and docker is not installed" >&2
    exit 1
  fi
  docker compose up -d --build
fi

COUNT="${COUNT}" ./scripts/produce_load.sh
sleep "${WAIT_AFTER_PRODUCE}"

if ! command -v ghz >/dev/null 2>&1; then
  echo "ghz is required: https://ghz.sh/docs/install" >&2
  exit 1
fi

ghz \
  --insecure \
  --proto proto/search_trends/v1/search_trends.proto \
  --call search_trends.v1.SearchTrendsService.GetTop \
  --data '{"limit":10}' \
  --connections "${CONNECTIONS}" \
  --concurrency "${CONCURRENCY}" \
  --total "${TOTAL}" \
  --format json \
  --output "${REPORT}" \
  "${GRPC_ADDR}"

echo "Performance scenario report: ${REPORT}"

if command -v jq >/dev/null 2>&1; then
  echo "Summary:"
  echo "  total requests: $(jq -r '.count // "n/a"' "${REPORT}")"
  echo "  success rate: $(jq -r 'if .count and .statusCodeDistribution.OK then ((.statusCodeDistribution.OK / .count) * 100 | tostring) + "%" else "n/a" end' "${REPORT}")"
  echo "  average latency: $(jq -r '.average // "n/a"' "${REPORT}")"
  echo "  p95 latency: $(jq -r '[.latencyDistribution[]? | select(.percentage == 95)][0].latency // "n/a"' "${REPORT}")"
  echo "  p99 latency: $(jq -r '[.latencyDistribution[]? | select(.percentage == 99)][0].latency // "n/a"' "${REPORT}")"
  echo "  requests/sec: $(jq -r '.rps // "n/a"' "${REPORT}")"
else
  echo "Install jq to print a parsed summary. Raw report was saved."
fi
