#!/bin/sh
set -eu

base_url=${BASE_URL:-http://backend:3030}
success_url="${base_url%/}/kv/__hey_load_present"
error_url="${base_url%/}/kv/__hey_load_missing"
churn_url="${base_url%/}/kv/__hey_load_churn"

echo "Seeding the successful GET target"
until wget -q -O /dev/null "$success_url" 2>/dev/null; do
    hey -n 1 -c 1 -m PUT -d ready "$success_url" >/dev/null
    sleep 1
done
if wget -q -O /dev/null "$error_url" 2>/dev/null; then
    echo "Expected $error_url to return 404" >&2
    exit 1
fi

run_phase() {
    qps_per_worker=$1
    duration=$2
    echo "Running for $duration at about $(awk "BEGIN { print 10 * $qps_per_worker }") requests/s (60% GET hits, 10% GET misses, 20% PUT, 10% DELETE)"

    hey -c 6 -q "$qps_per_worker" -z "$duration" "$success_url" >/dev/null &
    success_pid=$!
    hey -c 1 -q "$qps_per_worker" -z "$duration" "$error_url" >/dev/null &
    error_pid=$!
    hey -c 2 -q "$qps_per_worker" -z "$duration" -m PUT -d updated "$churn_url" >/dev/null &
    put_pid=$!
    hey -c 1 -q "$qps_per_worker" -z "$duration" -m DELETE "$churn_url" >/dev/null &
    delete_pid=$!
    wait "$success_pid"
    wait "$error_pid"
    wait "$put_pid"
    wait "$delete_pid"
}

run_phase 0.25 20s
run_phase 0.5 20s
run_phase 1 20s
while :; do
    run_phase 2 5m
done
