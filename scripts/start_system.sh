#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
PYTHON="$REPOSITORY_ROOT/.venv/bin/python"
STREAMLIT="$REPOSITORY_ROOT/.venv/bin/streamlit"
MODEL_DIR="$REPOSITORY_ROOT/models/random_forest_full_weighted"
RUNTIME_DIR="$REPOSITORY_ROOT/.runtime"
LOG_DIR="$RUNTIME_DIR/logs"
PID_DIR="$RUNTIME_DIR/pids"
KAFKA_TOPIC="iot-network-events"
KAFKA_CONFIG_NAME="config/server.properties"
SPARK_READY_TEXT="Prediction, metric, and alert history queries plus invalid console query started"
KAFKA_HOME_OVERRIDE=""

usage() {
    cat <<'EOF'
Usage: ./scripts/start_system.sh [--kafka-home PATH]

Starts the existing Kafka/Spark/Streamlit pipeline without formatting Kafka
storage or resetting SQLite history.
EOF
}

fail() {
    printf 'ERROR: %s\n' "$1" >&2
    exit 1
}

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

owned_process_pid() {
    local pid_file="$1"
    local expected="$2"
    local pid=""
    [[ -f "$pid_file" ]] || return 1
    read -r pid < "$pid_file" || true
    if process_matches "$pid" "$expected"; then
        printf '%s\n' "$pid"
        return 0
    fi
    rm -f -- "$pid_file"
    return 1
}

wait_for_port() {
    local port="$1"
    local pid="$2"
    local timeout_seconds="$3"
    local elapsed=0
    while (( elapsed < timeout_seconds )); do
        port_is_open "$port" && return 0
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 1
        ((elapsed += 1))
    done
    return 1
}

wait_for_log_text() {
    local log_file="$1"
    local text="$2"
    local pid="$3"
    local timeout_seconds="$4"
    local elapsed=0
    while (( elapsed < timeout_seconds )); do
        grep -Fq -- "$text" "$log_file" 2>/dev/null && return 0
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 1
        ((elapsed += 1))
    done
    return 1
}

resolve_kafka_home() {
    local candidate=""
    if [[ -n "$KAFKA_HOME_OVERRIDE" ]]; then
        candidate="$KAFKA_HOME_OVERRIDE"
        [[ -x "$candidate/bin/kafka-server-start.sh" ]] ||
            fail "Kafka not found at --kafka-home path: $candidate"
        printf '%s\n' "$(cd -- "$candidate" && pwd)"
        return
    fi

    if [[ -n "${KAFKA_HOME:-}" && -x "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
        printf '%s\n' "$(cd -- "$KAFKA_HOME" && pwd)"
        return
    fi

    for candidate in \
        "$HOME/kafka_2.13-4.3.1" \
        "$REPOSITORY_ROOT/kafka_2.13-4.3.1" \
        "/tmp/ciciot-kafka-4.3.1/kafka_2.13-4.3.1"
    do
        if [[ -x "$candidate/bin/kafka-server-start.sh" ]]; then
            printf '%s\n' "$(cd -- "$candidate" && pwd)"
            return
        fi
    done

    fail "Kafka distribution not found. Set KAFKA_HOME=/path/to/kafka_2.13-4.3.1"
}

kafka_storage_directory() {
    local config="$1"
    local directory=""
    directory="$(sed -n \
        -e 's/^[[:space:]]*metadata\.log\.dir[[:space:]]*=[[:space:]]*//p' \
        "$config" | head -n 1)"
    if [[ -z "$directory" ]]; then
        directory="$(sed -n \
            -e 's/^[[:space:]]*log\.dirs[[:space:]]*=[[:space:]]*//p' \
            "$config" | head -n 1)"
    fi
    directory="${directory%%,*}"
    [[ -n "$directory" ]] || fail "Kafka log directory is missing from $config"
    if [[ "$directory" != /* ]]; then
        directory="$KAFKA_DISTRIBUTION/$directory"
    fi
    printf '%s\n' "$directory"
}

while (( $# > 0 )); do
    case "$1" in
        --kafka-home)
            (( $# >= 2 )) || fail "--kafka-home requires a path"
            KAFKA_HOME_OVERRIDE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "Unknown argument: $1"
            ;;
    esac
done

cd -- "$REPOSITORY_ROOT" || fail "Cannot enter repository root: $REPOSITORY_ROOT"
[[ -x "$PYTHON" ]] ||
    fail "Virtual environment missing. Run: python3.12 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt"
[[ -x "$STREAMLIT" ]] ||
    fail "Streamlit executable missing. Run: .venv/bin/python -m pip install -r requirements.txt"
command -v java >/dev/null 2>&1 || fail "Java is not available on PATH"
command -v timeout >/dev/null 2>&1 || fail "Required command not found: timeout"
[[ -d "$MODEL_DIR" ]] || fail "Model missing: models/random_forest_full_weighted/"

if ! KAFKA_DISTRIBUTION="$(resolve_kafka_home)"; then
    exit 1
fi
KAFKA_CONFIG="$KAFKA_DISTRIBUTION/$KAFKA_CONFIG_NAME"
[[ -f "$KAFKA_CONFIG" ]] || fail "Kafka config missing: $KAFKA_CONFIG"
[[ -x "$KAFKA_DISTRIBUTION/bin/kafka-topics.sh" ]] ||
    fail "Kafka topics command missing under: $KAFKA_DISTRIBUTION"

mkdir -p -- "$LOG_DIR" "$PID_DIR" "$RUNTIME_DIR/dashboard" ||
    fail "Cannot create .runtime directories"

KAFKA_LOG="$LOG_DIR/kafka.log"
SPARK_LOG="$LOG_DIR/spark.log"
STREAMLIT_LOG="$LOG_DIR/streamlit.log"
KAFKA_PID_FILE="$PID_DIR/kafka.pid"
SPARK_PID_FILE="$PID_DIR/spark.pid"
STREAMLIT_PID_FILE="$PID_DIR/streamlit.pid"

kafka_pid="$(owned_process_pid "$KAFKA_PID_FILE" "$KAFKA_CONFIG" || true)"
spark_pid="$(owned_process_pid "$SPARK_PID_FILE" "spark/stream_analytics.py" || true)"
streamlit_pid="$(owned_process_pid "$STREAMLIT_PID_FILE" "dashboard/app.py" || true)"

if port_is_open 8501 && [[ -z "$streamlit_pid" ]]; then
    fail "Port 8501 is already in use by a process not owned by this launcher"
fi

if port_is_open 9092; then
    if [[ -n "$kafka_pid" ]]; then
        printf 'Kafka: already running (launcher-owned)\n'
    else
        printf 'Kafka: already running (external)\n'
    fi
elif [[ -n "$kafka_pid" ]]; then
    printf 'Kafka: waiting for existing launcher process\n'
    if ! wait_for_port 9092 "$kafka_pid" 30; then
        fail "Kafka launcher process is alive but port 9092 is not ready. See .runtime/logs/kafka.log"
    fi
else
    KAFKA_STORAGE_DIR="$(kafka_storage_directory "$KAFKA_CONFIG")"
    if [[ ! -f "$KAFKA_STORAGE_DIR/meta.properties" ]]; then
        fail "Kafka KRaft storage is not formatted. Follow README ONE-TIME KRaft setup, then rerun ./scripts/start_system.sh"
    fi

    : > "$KAFKA_LOG" || fail "Cannot write $KAFKA_LOG"
    nohup "$KAFKA_DISTRIBUTION/bin/kafka-server-start.sh" "$KAFKA_CONFIG" \
        >> "$KAFKA_LOG" 2>&1 &
    kafka_pid=$!
    printf '%s\n' "$kafka_pid" > "$KAFKA_PID_FILE"
    printf 'Kafka: starting\n'
    if ! wait_for_port 9092 "$kafka_pid" 60; then
        kill -TERM "$kafka_pid" 2>/dev/null || true
        rm -f -- "$KAFKA_PID_FILE"
        fail "Kafka failed to become ready. See .runtime/logs/kafka.log"
    fi
fi

if ! topic_list="$("$KAFKA_DISTRIBUTION/bin/kafka-topics.sh" \
    --list --bootstrap-server localhost:9092 2>> "$KAFKA_LOG")"; then
    fail "Kafka is reachable on port 9092 but topic inspection failed. See .runtime/logs/kafka.log"
fi
if grep -Fxq -- "$KAFKA_TOPIC" <<< "$topic_list"; then
    printf 'Kafka topic: ready\n'
else
    if ! "$KAFKA_DISTRIBUTION/bin/kafka-topics.sh" \
        --create \
        --topic "$KAFKA_TOPIC" \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1 \
        >> "$KAFKA_LOG" 2>&1; then
        fail "Kafka topic creation failed. See .runtime/logs/kafka.log"
    fi
    printf 'Kafka topic: created\n'
fi

if [[ -n "$spark_pid" ]]; then
    printf 'Spark: already running (launcher-owned)\n'
else
    : > "$SPARK_LOG" || fail "Cannot write $SPARK_LOG"
    nohup env \
        PYSPARK_PYTHON="$PYTHON" \
        PYSPARK_DRIVER_PYTHON="$PYTHON" \
        PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 --conf spark.sql.shuffle.partitions=8 pyspark-shell' \
        "$PYTHON" -u "$REPOSITORY_ROOT/spark/stream_analytics.py" \
        --starting-offsets latest \
        --dashboard-output-dir "$RUNTIME_DIR/dashboard" \
        --history-db "$REPOSITORY_ROOT/data/history/iot_ids_history.sqlite3" \
        >> "$SPARK_LOG" 2>&1 &
    spark_pid=$!
    printf '%s\n' "$spark_pid" > "$SPARK_PID_FILE"
    printf 'Spark: starting\n'
fi

if ! wait_for_log_text "$SPARK_LOG" "$SPARK_READY_TEXT" "$spark_pid" 120; then
    if ! kill -0 "$spark_pid" 2>/dev/null; then
        rm -f -- "$SPARK_PID_FILE"
    fi
    fail "Spark analytics did not become ready. See .runtime/logs/spark.log"
fi

if [[ -n "$streamlit_pid" ]]; then
    printf 'Streamlit: already running (launcher-owned)\n'
else
    : > "$STREAMLIT_LOG" || fail "Cannot write $STREAMLIT_LOG"
    nohup "$STREAMLIT" run "$REPOSITORY_ROOT/dashboard/app.py" \
        --server.address 127.0.0.1 \
        --server.port 8501 \
        >> "$STREAMLIT_LOG" 2>&1 &
    streamlit_pid=$!
    printf '%s\n' "$streamlit_pid" > "$STREAMLIT_PID_FILE"
    printf 'Streamlit: starting\n'
fi

if ! wait_for_port 8501 "$streamlit_pid" 30; then
    if ! kill -0 "$streamlit_pid" 2>/dev/null; then
        rm -f -- "$STREAMLIT_PID_FILE"
    fi
    fail "Streamlit did not become ready. See .runtime/logs/streamlit.log"
fi

cat <<'EOF'

========================================
IoT Threat Detection System
========================================

Kafka broker:      RUNNING
Kafka topic:       READY
Spark analytics:   RUNNING
SQLite history:    ENABLED
Dashboard:         RUNNING

Dashboard URL:
http://127.0.0.1:8501

Try:
./scripts/run_scenario.sh NORMAL

or:
./scripts/demo.sh

Logs:
.runtime/logs/

Stop:
./scripts/stop_system.sh
EOF
