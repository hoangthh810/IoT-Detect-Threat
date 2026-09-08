#!/usr/bin/env python3
"""Replay historical CICIoT2023 rows as rate-limited JSON Kafka events."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data/sample/ciciot2023_phase1_sample.csv"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "iot-network-events"
LABEL_COLUMN = "Label"
SCENARIOS = ("NORMAL", "DDOS_BURST", "MIXED")

# Keep this ordered event contract aligned with spark/preprocess.py::SELECTED_FEATURES.
# The replay process uses only the standard-library CSV reader; it does not run Spark.
SELECTED_FEATURES = (
    "Header_Length",
    "Time_To_Live",
    "Rate",
    "fin_flag_number",
    "syn_flag_number",
    "rst_flag_number",
    "psh_flag_number",
    "ack_flag_number",
    "ack_count",
    "syn_count",
    "fin_count",
    "rst_count",
    "HTTP",
    "HTTPS",
    "DNS",
    "TCP",
    "UDP",
    "ARP",
    "ICMP",
    "IPv",
    "Tot sum",
    "Min",
    "Max",
    "AVG",
    "Std",
    "IAT",
    "Variance",
)

KNOWN_ATTACK_LABELS = frozenset(
    {
        "BACKDOOR_MALWARE",
        "BROWSERHIJACKING",
        "COMMANDINJECTION",
        "DDOS-ACK_FRAGMENTATION",
        "DDOS-HTTP_FLOOD",
        "DDOS-ICMP_FLOOD",
        "DDOS-ICMP_FRAGMENTATION",
        "DDOS-PSHACK_FLOOD",
        "DDOS-RSTFINFLOOD",
        "DDOS-SLOWLORIS",
        "DDOS-SYN_FLOOD",
        "DDOS-SYNONYMOUSIP_FLOOD",
        "DDOS-TCP_FLOOD",
        "DDOS-UDP_FLOOD",
        "DDOS-UDP_FRAGMENTATION",
        "DICTIONARYBRUTEFORCE",
        "DNS_SPOOFING",
        "DOS-HTTP_FLOOD",
        "DOS-SYN_FLOOD",
        "DOS-TCP_FLOOD",
        "DOS-UDP_FLOOD",
        "MIRAI-GREETH_FLOOD",
        "MIRAI-GREIP_FLOOD",
        "MIRAI-UDPPLAIN",
        "MITM-ARPSPOOFING",
        "RECON-HOSTDISCOVERY",
        "RECON-OSSCAN",
        "RECON-PINGSWEEP",
        "RECON-PORTSCAN",
        "SQLINJECTION",
        "UPLOADING_ATTACK",
        "VULNERABILITYSCAN",
        "XSS",
    }
)
KNOWN_LABELS = frozenset({"BENIGN", *KNOWN_ATTACK_LABELS})


@dataclass
class ReplayStats:
    sent: int = 0
    skipped_invalid: int = 0
    delivery_failed: int = 0


def positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return parsed


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be an integer greater than zero")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay historical CICIoT2023 rows to one Kafka topic."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="CICIoT2023 CSV file or directory of CSV partitions",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic (default: {DEFAULT_TOPIC})",
    )
    parser.add_argument("--scenario", required=True, choices=SCENARIOS)
    parser.add_argument(
        "--events-per-second",
        type=positive_float,
        default=10.0,
        help="Maximum replay send rate (default: 10)",
    )
    parser.add_argument(
        "--max-events",
        type=positive_int,
        help="Stop after this many successful deliveries; omit for continuous replay",
    )
    return parser.parse_args()


def csv_files(input_path: Path) -> tuple[Path, ...]:
    resolved = input_path.expanduser().resolve()
    if resolved.is_file():
        if resolved.suffix.lower() != ".csv":
            raise ValueError(f"input file is not CSV: {resolved}")
        files = (resolved,)
    elif resolved.is_dir():
        files = tuple(sorted(path for path in resolved.iterdir() if path.suffix.lower() == ".csv"))
        if not files:
            raise ValueError(f"input directory contains no CSV files: {resolved}")
    else:
        raise ValueError(f"input does not exist: {resolved}")
    return files


def validate_headers(files: tuple[Path, ...]) -> None:
    required = {LABEL_COLUMN, *SELECTED_FEATURES}
    reference_header: tuple[str, ...] | None = None
    reference_file: Path | None = None

    for path in files:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            header = csv.DictReader(handle).fieldnames
        if not header:
            raise ValueError(f"CSV has no header: {path}")
        normalized_header = tuple(header)
        duplicates = sorted({name for name in normalized_header if normalized_header.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate CSV columns in {path}: {', '.join(duplicates)}")
        missing = sorted(required.difference(normalized_header))
        if missing:
            raise ValueError(f"missing required columns in {path}: {', '.join(missing)}")
        if reference_header is None:
            reference_header = normalized_header
            reference_file = path
        elif normalized_header != reference_header:
            raise ValueError(
                f"CSV header mismatch between {reference_file} and {path}"
            )


def normalize_label(raw_label: str | None) -> str | None:
    if raw_label is None:
        return None
    normalized = raw_label.strip().upper()
    if not normalized or normalized not in KNOWN_LABELS:
        return None
    return normalized


def parse_matching_row(
    row: dict[str, str | None], kind: str, stats: ReplayStats
) -> dict[str, Any] | None:
    label = normalize_label(row.get(LABEL_COLUMN))
    if label is None:
        stats.skipped_invalid += 1
        return None

    is_match = label == "BENIGN" if kind == "normal" else label.startswith("DDOS-")
    if not is_match:
        return None

    numeric_values: dict[str, float] = {}
    try:
        for feature in SELECTED_FEATURES:
            raw_value = row.get(feature)
            if raw_value is None or not raw_value.strip():
                raise ValueError
            value = float(raw_value)
            if not math.isfinite(value):
                raise ValueError
            numeric_values[feature] = value
    except (TypeError, ValueError):
        stats.skipped_invalid += 1
        return None

    return {
        "ground_truth": "Normal" if label == "BENIGN" else "Attack",
        "source_label": label,
        **numeric_values,
    }


def iter_matching_rows(
    files: tuple[Path, ...], kind: str, stats: ReplayStats
) -> Iterator[dict[str, Any]]:
    """Yield matching valid rows forever, reopening files without retaining rows."""
    while True:
        matches_this_cycle = 0
        for path in files:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    parsed = parse_matching_row(row, kind, stats)
                    if parsed is not None:
                        matches_this_cycle += 1
                        yield parsed
        if matches_this_cycle == 0:
            label_description = "BENIGN" if kind == "normal" else "DDOS-*"
            raise RuntimeError(f"no valid {label_description} rows found in input")


def iter_scenario_rows(
    files: tuple[Path, ...], scenario: str, stats: ReplayStats
) -> Iterator[dict[str, Any]]:
    if scenario == "NORMAL":
        yield from iter_matching_rows(files, "normal", stats)
        return
    if scenario == "DDOS_BURST":
        yield from iter_matching_rows(files, "ddos", stats)
        return

    normal_rows = iter_matching_rows(files, "normal", stats)
    ddos_rows = iter_matching_rows(files, "ddos", stats)
    while True:
        yield next(normal_rows)
        yield next(ddos_rows)


def utc_send_time() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def build_event(scenario: str, source_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_time": utc_send_time(),
        "scenario": scenario,
        "ground_truth": source_row["ground_truth"],
        "source_label": source_row["source_label"],
        **{feature: source_row[feature] for feature in SELECTED_FEATURES},
    }


def deliver_event(producer: Any, topic: str, event: dict[str, Any], stats: ReplayStats) -> None:
    delivery_result: list[Any] = []

    def on_delivery(error: Any, _message: Any) -> None:
        delivery_result.append(error)

    payload = json.dumps(
        event, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    while True:
        try:
            producer.produce(topic, value=payload, on_delivery=on_delivery)
            break
        except BufferError:
            producer.poll(0.1)

    remaining = producer.flush(10.0)
    error = delivery_result[0] if delivery_result else None
    if remaining or not delivery_result or error is not None:
        stats.delivery_failed += 1
        reason = str(error) if error is not None else f"{remaining} message(s) unflushed"
        raise RuntimeError(f"Kafka delivery failed: {reason}")
    stats.sent += 1


def run_replay(args: argparse.Namespace) -> int:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "missing dependency: install requirements.txt before running the producer"
        ) from exc

    files = csv_files(args.input)
    validate_headers(files)
    stats = ReplayStats()
    source_rows = iter_scenario_rows(files, args.scenario, stats)
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "ciciot2023-replay-producer",
            "message.timeout.ms": 10_000,
        }
    )

    input_display = str(files[0]) if len(files) == 1 else f"{args.input.resolve()} ({len(files)} CSV files)"
    print("=== KAFKA DATASET REPLAY ===")
    print(f"Input: {input_display}")
    print(f"Scenario: {args.scenario}")
    print(f"Kafka bootstrap servers: {args.bootstrap_servers}")
    print(f"Topic: {args.topic}")
    print(f"Events/sec: {args.events_per_second:g}")
    print(f"Max events: {args.max_events if args.max_events is not None else 'continuous'}")
    print(f"Feature count: {len(SELECTED_FEATURES)}")

    started_at = time.perf_counter()
    interrupted = False
    flush_succeeded = False
    try:
        while args.max_events is None or stats.sent < args.max_events:
            scheduled_at = started_at + (stats.sent / args.events_per_second)
            delay = scheduled_at - time.perf_counter()
            if delay > 0:
                time.sleep(delay)

            event = build_event(args.scenario, next(source_rows))
            deliver_event(producer, args.topic, event, stats)
            if stats.sent <= 3:
                print(
                    "Delivered "
                    f"#{stats.sent}: scenario={event['scenario']}, "
                    f"ground_truth={event['ground_truth']}, "
                    f"source_label={event['source_label']}"
                )
            elif stats.sent % 100 == 0:
                print(f"Progress: sent={stats.sent}, skipped_invalid={stats.skipped_invalid}")
    except KeyboardInterrupt:
        interrupted = True
        print("Interrupt received; flushing producer...")
    finally:
        remaining = producer.flush(10.0)
        flush_succeeded = remaining == 0
        runtime = time.perf_counter() - started_at
        print("=== FINAL REPLAY REPORT ===")
        print(f"sent: {stats.sent}")
        print(f"skipped_invalid: {stats.skipped_invalid}")
        print(f"delivery_failed: {stats.delivery_failed}")
        print(f"runtime_seconds: {runtime:.3f}")
        print(f"flush_succeeded: {str(flush_succeeded).lower()}")
        print(f"interrupted: {str(interrupted).lower()}")

    return 0 if flush_succeeded else 1


def main() -> int:
    args = parse_args()
    try:
        return run_replay(args)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
