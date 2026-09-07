#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

: "${SOAK_MAX_STABLE_RATE:?Set SOAK_MAX_STABLE_RATE to the measured maximum stable rate}"
if ! [[ "$SOAK_MAX_STABLE_RATE" =~ ^[1-9][0-9]*$ ]]; then
  echo "SOAK_MAX_STABLE_RATE must be a positive integer" >&2
  exit 2
fi

# Leave enough room for the bounded broker data, Docker logs, and result artifacts.
readonly minimum_free_kib=$((5 * 1024 * 1024))
readonly available_kib=$(df -Pk . | awk 'NR == 2 { print $4 }')
if (( available_kib < minimum_free_kib )); then
  echo "At least 5 GiB free disk is required; only ${available_kib} KiB is available" >&2
  exit 1
fi

readonly rate=$((SOAK_MAX_STABLE_RATE * 75 / 100))
if (( rate < 1 )); then
  echo "75% of SOAK_MAX_STABLE_RATE rounded down to zero" >&2
  exit 2
fi

readonly compose_file="test/impl/soak.compose.yml"
readonly log_file="out/soak/release-72h.log"
readonly max_log_bytes=$((5 * 1024 * 1024))
readonly retained_logs=3
mkdir -p out/soak
: >"$log_file"

docker compose -f "$compose_file" up -d --wait
trap 'docker compose -f "$compose_file" stop >/dev/null' EXIT

rotate_log() {
  local index
  rm -f "${log_file}.${retained_logs}"
  for ((index = retained_logs - 1; index >= 1; index--)); do
    if [[ -f "${log_file}.${index}" ]]; then
      mv "${log_file}.${index}" "${log_file}.$((index + 1))"
    fi
  done
  mv "$log_file" "${log_file}.1"
  : >"$log_file"
}

# The harness normally emits one progress line every five minutes. This loop also
# protects the host if an unexpected error starts producing output continuously.
set +e
KAFKA_BROKERS=127.0.0.1:19092 \
SOAK_BROKER_VERSION=redpanda-v25.2.1 \
SOAK_DURATION_S=259200 \
SOAK_MAX_STABLE_RATE="$SOAK_MAX_STABLE_RATE" \
SOAK_RATE="$rate" \
SOAK_BURST_INTERVAL_S=3600 \
SOAK_BURST_S=600 \
SOAK_LOG_INTERVAL_S=300 \
SOAK_RETENTION_MS=3600000 \
SOAK_RETENTION_BYTES=268435456 \
bun scripts/soak.ts 2>&1 | while IFS= read -r line || [[ -n "$line" ]]; do
  printf '%.65536s\n' "$line" >>"$log_file"
  if (( $(stat -c %s "$log_file") >= max_log_bytes )); then
    rotate_log
  fi
done
status=${PIPESTATUS[0]}
set -e

exit "$status"
