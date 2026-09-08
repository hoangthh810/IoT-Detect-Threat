#!/usr/bin/env python3
"""Inspect the durable SQLite history produced by realtime analytics."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = PROJECT_ROOT / "data/history/iot_ids_history.sqlite3"
REQUIRED_TABLES = ("predictions", "metrics", "alerts")


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be an integer greater than zero")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DATABASE,
        help=f"SQLite history database (default: {DEFAULT_DATABASE})",
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        default=10,
        help="maximum rows to show in each recent section (default: 10)",
    )
    return parser.parse_args()


def print_rows(title: str, headers: Sequence[str], rows: Sequence[sqlite3.Row]) -> None:
    print(f"\n{title}:")
    if not rows:
        print("(none)")
        return
    print(" | ".join(headers))
    for row in rows:
        print(" | ".join(str(row[header]) for header in headers))


def inspect_history(database: Path, limit: int) -> None:
    resolved = database.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"history database not found: {resolved}")

    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        table_rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
        tables = {str(row["name"]) for row in table_rows}
        missing = set(REQUIRED_TABLES).difference(tables)
        if missing:
            raise RuntimeError(
                "history database is missing required tables: "
                + ", ".join(sorted(missing))
            )

        counts = {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in REQUIRED_TABLES
        }
        metrics = connection.execute(
            """
            SELECT window_end, status, event_rate, attack_rate,
                   event_count, attack_count
            FROM metrics
            ORDER BY window_end DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        predictions = connection.execute(
            """
            SELECT event_timestamp, topic, partition, offset,
                   prediction_label, scenario, source_label
            FROM predictions
            ORDER BY event_timestamp DESC, topic DESC, partition DESC, offset DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        alerts = connection.execute(
            """
            SELECT event_timestamp, topic, partition, offset,
                   prediction_label, scenario, source_label
            FROM alerts
            ORDER BY event_timestamp DESC, topic DESC, partition DESC, offset DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        connection.close()

    print(f"Database: {resolved}")
    print(f"Predictions: {counts['predictions']}")
    print(f"Metrics: {counts['metrics']}")
    print(f"Alerts: {counts['alerts']}")
    print_rows(
        "Latest Metrics",
        ("window_end", "status", "event_rate", "attack_rate", "event_count", "attack_count"),
        metrics,
    )
    print_rows(
        "Recent Predictions",
        (
            "event_timestamp",
            "topic",
            "partition",
            "offset",
            "prediction_label",
            "scenario",
            "source_label",
        ),
        predictions,
    )
    print_rows(
        "Recent Alerts",
        (
            "event_timestamp",
            "topic",
            "partition",
            "offset",
            "prediction_label",
            "scenario",
            "source_label",
        ),
        alerts,
    )


def main() -> int:
    args = parse_args()
    try:
        inspect_history(args.db, args.limit)
    except (FileNotFoundError, OSError, RuntimeError, sqlite3.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
