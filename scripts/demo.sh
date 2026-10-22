#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
RUN_SCENARIO="$SCRIPT_DIR/run_scenario.sh"
RATE="5"
EVENTS="60"
PAUSE_SECONDS="3"

usage() {
    cat <<'EOF'
Usage: ./scripts/demo.sh [--rate EVENTS_PER_SECOND] [--events COUNT] [--pause SECONDS]

Runs NORMAL -> DDOS_BURST -> NORMAL -> MIXED.
Defaults: --rate 5 --events 60 --pause 3
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
        --pause)
            (( $# >= 2 )) || fail "--pause requires a value"
            PAUSE_SECONDS="$2"
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

[[ -x "$RUN_SCENARIO" ]] || fail "Scenario runner is not executable: scripts/run_scenario.sh"
command -v timeout >/dev/null 2>&1 || fail "Required command not found: timeout"
command -v pgrep >/dev/null 2>&1 || fail "Required command not found: pgrep"
[[ "$PAUSE_SECONDS" =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]] ||
    fail "--pause must be a non-negative number"

if ! port_is_open 9092 || ! pgrep -f '[s]park/stream_analytics.py' >/dev/null 2>&1; then
    cat >&2 <<'EOF'
System is not ready.
Run ./scripts/start_system.sh first
EOF
    exit 1
fi

cd -- "$REPOSITORY_ROOT" || fail "Cannot enter repository root: $REPOSITORY_ROOT"

printf '[1/4] NORMAL\n'
"$RUN_SCENARIO" NORMAL --rate "$RATE" --events "$EVENTS" || exit $?
sleep "$PAUSE_SECONDS"

printf '[2/4] DDOS_BURST\n'
"$RUN_SCENARIO" DDOS_BURST --rate "$RATE" --events "$EVENTS" || exit $?
sleep "$PAUSE_SECONDS"

printf '[3/4] NORMAL RECOVERY\n'
"$RUN_SCENARIO" NORMAL --rate "$RATE" --events "$EVENTS" || exit $?
sleep "$PAUSE_SECONDS"

printf '[4/4] MIXED\n'
"$RUN_SCENARIO" MIXED --rate "$RATE" --events "$EVENTS" || exit $?

printf 'Demo sequence complete. Dashboard: http://127.0.0.1:8501\n'
