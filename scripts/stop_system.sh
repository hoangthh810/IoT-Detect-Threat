#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
PID_DIR="$REPOSITORY_ROOT/.runtime/pids"
SQLITE_HISTORY="$REPOSITORY_ROOT/data/history/iot_ids_history.sqlite3"

port_is_open() {
    local port="$1"
    timeout 1 bash -c "</dev/tcp/127.0.0.1/$port" >/dev/null 2>&1
}

process_matches() {
    local pid="$1"
    local expected="$2"
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    [[ -r "/proc/$pid/cmdline" ]] || return 1
    tr '\0' ' ' < "/proc/$pid/cmdline" | grep -Fq -- "$expected"
}

wait_until_stopped() {
    local pid="$1"
    local timeout_seconds="$2"
    local elapsed=0
    while (( elapsed < timeout_seconds )); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 1
        ((elapsed += 1))
    done
    return 1
}

stop_owned_process() {
    local name="$1"
    local pid_file="$2"
    local expected="$3"
    local first_signal="$4"
    local pid=""

    if [[ ! -f "$pid_file" ]]; then
        printf '%s: not launcher-owned\n' "$name"
        return 2
    fi
    read -r pid < "$pid_file" || true
    if ! process_matches "$pid" "$expected"; then
        rm -f -- "$pid_file"
        printf '%s: stale PID removed; no process stopped\n' "$name"
        return 2
    fi

    kill "-$first_signal" "$pid" 2>/dev/null || true
    if ! wait_until_stopped "$pid" 20; then
        kill -TERM "$pid" 2>/dev/null || true
        wait_until_stopped "$pid" 10 || true
    fi
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
        wait_until_stopped "$pid" 5 || true
    fi

    if kill -0 "$pid" 2>/dev/null; then
        printf '%s: could not stop PID %s\n' "$name" "$pid" >&2
        return 1
    fi
    rm -f -- "$pid_file"
    printf '%s: stopped\n' "$name"
    return 0
}

mkdir -p -- "$PID_DIR"

streamlit_result=0
spark_result=0
kafka_result=0
stop_owned_process \
    "Streamlit" "$PID_DIR/streamlit.pid" "dashboard/app.py" INT || streamlit_result=$?
stop_owned_process \
    "Spark" "$PID_DIR/spark.pid" "spark/stream_analytics.py" INT || spark_result=$?
stop_owned_process \
    "Kafka" "$PID_DIR/kafka.pid" "config/server.properties" TERM || kafka_result=$?

if (( kafka_result == 2 )); then
    if port_is_open 9092; then
        printf 'Kafka: left running externally\n'
    else
        printf 'Kafka: no launcher-owned broker was running\n'
    fi
fi
if [[ -f "$SQLITE_HISTORY" ]]; then
    printf 'SQLite history: preserved (%s)\n' "$SQLITE_HISTORY"
else
    printf 'SQLite history: not created yet; no history was deleted\n'
fi

if (( streamlit_result == 1 || spark_result == 1 || kafka_result == 1 )); then
    exit 1
fi
