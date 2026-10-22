#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
PYTHON="$REPOSITORY_ROOT/.venv/bin/python"
KAFKA_TOPIC="iot-network-events"
RATE="5"
EVENTS="60"

usage() {
    cat <<'EOF'
Usage: ./scripts/run_scenario.sh SCENARIO [--rate EVENTS_PER_SECOND] [--events COUNT]

SCENARIO must be one of: NORMAL, DDOS_BURST, MIXED
Defaults: --rate 5 --events 60
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

system_is_ready() {
    port_is_open 9092 || return 1
    pgrep -f '[s]park/stream_analytics.py' >/dev/null 2>&1 || return 1
    return 0
}

(( $# >= 1 )) || {
    usage
    exit 1
}

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
    exit 0
fi

SCENARIO="$1"
shift
case "$SCENARIO" in
    NORMAL|DDOS_BURST|MIXED) ;;
    *)
        fail "Invalid scenario '$SCENARIO'. Use NORMAL, DDOS_BURST, or MIXED."
        ;;
esac

while (( $# > 0 )); do
    case "$1" in
        --rate)
            (( $# >= 2 )) || fail "--rate requires a value"
            RATE="$2"
            shift 2
            ;;
        --events)
            (( $# >= 2 )) || fail "--events requires a value"
            EVENTS="$2"
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

[[ -x "$PYTHON" ]] ||
    fail "Virtual environment missing. Run: python3.12 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt"
command -v timeout >/dev/null 2>&1 || fail "Required command not found: timeout"
command -v pgrep >/dev/null 2>&1 || fail "Required command not found: pgrep"

if ! system_is_ready; then
    cat >&2 <<'EOF'
System is not ready.
Run:

./scripts/start_system.sh
EOF
    exit 1
fi

cd -- "$REPOSITORY_ROOT" || fail "Cannot enter repository root: $REPOSITORY_ROOT"
printf 'Running scenario %s (%s events/s, %s events)\n' "$SCENARIO" "$RATE" "$EVENTS"
exec "$PYTHON" -u "$REPOSITORY_ROOT/kafka/producer.py" \
    --topic "$KAFKA_TOPIC" \
    --scenario "$SCENARIO" \
    --events-per-second "$RATE" \
    --max-events "$EVENTS"
