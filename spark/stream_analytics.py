#!/usr/bin/env python3
"""Phase 7 prediction-derived realtime metrics, status, and alerts."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from py4j.protocol import Py4JError
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery

from stream_ingest import DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_TOPIC, STARTING_OFFSETS
from stream_infer import (
    ATTACK_PREDICTION,
    DEFAULT_MODEL_PATH,
    NORMAL_PREDICTION,
    PREDICTION_OUTPUT_COLUMNS,
    build_prediction_stream,
    load_realtime_model,
)


EVENT_TIME_COLUMN = "event_timestamp"
WINDOW_DURATION = "10 seconds"
WINDOW_DURATION_SECONDS = 10.0
SLIDE_DURATION = "5 seconds"
WATERMARK_DELAY = "30 seconds"
WARNING_ATTACK_RATE = 0.20
CRITICAL_ATTACK_RATE = 0.80
MAX_METRIC_WINDOWS = 60
MAX_ALERTS = 100
METRICS_SNAPSHOT_NAME = "metrics.json"
ALERTS_SNAPSHOT_NAME = "alerts.json"
DEFAULT_HISTORY_DB_PATH = Path("data/history/iot_ids_history.sqlite3")
SQLITE_BUSY_TIMEOUT_MS = 30_000

METRICS_OUTPUT_COLUMNS = (
    "window_start",
    "window_end",
    "event_count",
    "event_rate",
    "normal_count",
    "attack_count",
    "normal_rate",
    "attack_rate",
    "status",
)
ALERT_OUTPUT_COLUMNS = (
    "event_timestamp",
    "kafka_timestamp",
    "topic",
    "partition",
    "offset",
    "alert_type",
    "prediction",
    "prediction_label",
    "probability",
    "scenario",
    "ground_truth",
    "source_label",
)
PREDICTION_HISTORY_COLUMNS = PREDICTION_OUTPUT_COLUMNS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Phase 7 metrics and alerts from Phase 6 predictions."
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
    parser.add_argument(
        "--starting-offsets",
        choices=STARTING_OFFSETS,
        default="latest",
        help="Kafka start position for a new query (default: latest)",
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=DEFAULT_MODEL_PATH,
        help=f"frozen Random Forest directory (default: {DEFAULT_MODEL_PATH})",
    )
    parser.add_argument(
        "--dashboard-output-dir",
        type=Path,
        help=(
            "enable bounded transient dashboard snapshots in this directory "
            "(recommended: .runtime/dashboard)"
        ),
    )
    parser.add_argument(
        "--history-db",
        type=Path,
        help=(
            "enable durable SQLite prediction/metric/alert history at this path "
            f"(recommended: {DEFAULT_HISTORY_DB_PATH})"
        ),
    )
    return parser.parse_args()


def build_realtime_metrics(predictions: DataFrame) -> DataFrame:
    """Aggregate prediction counts and rates in bounded event-time state."""
    windowed_counts = (
        predictions.withWatermark(EVENT_TIME_COLUMN, WATERMARK_DELAY)
        .groupBy(
            F.window(
                F.col(EVENT_TIME_COLUMN),
                WINDOW_DURATION,
                SLIDE_DURATION,
            )
        )
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum(
                F.when(F.col("prediction") == NORMAL_PREDICTION, 1).otherwise(0)
            ).alias("normal_count"),
            F.sum(
                F.when(F.col("prediction") == ATTACK_PREDICTION, 1).otherwise(0)
            ).alias("attack_count"),
        )
    )

    rates = (
        windowed_counts.withColumn(
            "event_rate",
            F.col("event_count").cast("double") / F.lit(WINDOW_DURATION_SECONDS),
        )
        .withColumn(
            "normal_rate",
            F.col("normal_count").cast("double")
            / F.col("event_count").cast("double"),
        )
        .withColumn(
            "attack_rate",
            F.col("attack_count").cast("double")
            / F.col("event_count").cast("double"),
        )
    )

    return (
        rates.withColumn(
            "status",
            F.when(F.col("attack_rate") < WARNING_ATTACK_RATE, F.lit("Normal"))
            .when(F.col("attack_rate") < CRITICAL_ATTACK_RATE, F.lit("Warning"))
            .otherwise(F.lit("Critical")),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_count",
            "event_rate",
            "normal_count",
            "attack_count",
            "normal_rate",
            "attack_rate",
            "status",
        )
    )


def build_alert_stream(predictions: DataFrame) -> DataFrame:
    """Create one alert for each event the frozen model predicts as Attack."""
    return (
        predictions.filter(F.col("prediction") == ATTACK_PREDICTION)
        .withColumn("alert_type", F.lit("Predicted Attack"))
        .select(*ALERT_OUTPUT_COLUMNS)
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        timestamp = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )
    if hasattr(value, "toArray"):
        return [float(item) for item in value.toArray()]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _collect_json_rows(batch: DataFrame) -> list[dict[str, Any]]:
    return [
        {key: _json_value(value) for key, value in row.asDict().items()}
        for row in batch.collect()
    ]


def _open_history_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    return connection


def initialize_history_database(path: Path) -> None:
    """Create the durable Phase 9 schema without clearing existing history."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = _open_history_database(path)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        with connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS predictions (
                    topic TEXT NOT NULL,
                    partition INTEGER NOT NULL,
                    offset INTEGER NOT NULL,
                    kafka_timestamp TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    scenario TEXT NOT NULL,
                    ground_truth TEXT NOT NULL,
                    source_label TEXT NOT NULL,
                    prediction REAL NOT NULL,
                    prediction_label TEXT NOT NULL,
                    probability TEXT NOT NULL,
                    PRIMARY KEY (topic, partition, offset)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    window_start TEXT NOT NULL,
                    window_end TEXT NOT NULL,
                    event_count INTEGER NOT NULL,
                    event_rate REAL NOT NULL,
                    normal_count INTEGER NOT NULL,
                    attack_count INTEGER NOT NULL,
                    normal_rate REAL NOT NULL,
                    attack_rate REAL NOT NULL,
                    status TEXT NOT NULL,
                    PRIMARY KEY (window_start, window_end)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    event_timestamp TEXT NOT NULL,
                    kafka_timestamp TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    partition INTEGER NOT NULL,
                    offset INTEGER NOT NULL,
                    alert_type TEXT NOT NULL,
                    prediction REAL NOT NULL,
                    prediction_label TEXT NOT NULL,
                    probability TEXT NOT NULL,
                    scenario TEXT NOT NULL,
                    ground_truth TEXT NOT NULL,
                    source_label TEXT NOT NULL,
                    PRIMARY KEY (topic, partition, offset)
                )
                """
            )
    finally:
        connection.close()


def _probability_json(value: Any) -> str:
    return json.dumps(_json_value(value), separators=(",", ":"))


def persist_prediction_history(batch: DataFrame, history_db: Path) -> None:
    """Insert one idempotent history row per predicted Kafka event."""
    rows = _collect_json_rows(batch.select(*PREDICTION_HISTORY_COLUMNS))
    if not rows:
        return
    values = [
        (
            row["topic"],
            int(row["partition"]),
            int(row["offset"]),
            row["kafka_timestamp"],
            row["event_time"],
            row["event_timestamp"],
            row["scenario"],
            row["ground_truth"],
            row["source_label"],
            float(row["prediction"]),
            row["prediction_label"],
            _probability_json(row["probability"]),
        )
        for row in rows
    ]
    connection = _open_history_database(history_db)
    try:
        with connection:
            connection.executemany(
                """
                INSERT OR IGNORE INTO predictions (
                    topic, partition, offset, kafka_timestamp, event_time,
                    event_timestamp, scenario, ground_truth, source_label,
                    prediction, prediction_label, probability
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
    finally:
        connection.close()


def persist_metric_history(batch: DataFrame, history_db: Path) -> None:
    """Upsert update-mode metric windows without truncating durable history."""
    rows = _collect_json_rows(batch.select(*METRICS_OUTPUT_COLUMNS))
    if not rows:
        return
    values = [
        (
            row["window_start"],
            row["window_end"],
            int(row["event_count"]),
            float(row["event_rate"]),
            int(row["normal_count"]),
            int(row["attack_count"]),
            float(row["normal_rate"]),
            float(row["attack_rate"]),
            row["status"],
        )
        for row in rows
    ]
    connection = _open_history_database(history_db)
    try:
        with connection:
            connection.executemany(
                """
                INSERT INTO metrics (
                    window_start, window_end, event_count, event_rate,
                    normal_count, attack_count, normal_rate, attack_rate, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(window_start, window_end) DO UPDATE SET
                    event_count = excluded.event_count,
                    event_rate = excluded.event_rate,
                    normal_count = excluded.normal_count,
                    attack_count = excluded.attack_count,
                    normal_rate = excluded.normal_rate,
                    attack_rate = excluded.attack_rate,
                    status = excluded.status
                """,
                values,
            )
    finally:
        connection.close()


def persist_alert_history(batch: DataFrame, history_db: Path) -> None:
    """Insert idempotent history rows from the existing Phase 7 alert stream."""
    rows = _collect_json_rows(batch.select(*ALERT_OUTPUT_COLUMNS))
    if not rows:
        return
    values = [
        (
            row["event_timestamp"],
            row["kafka_timestamp"],
            row["topic"],
            int(row["partition"]),
            int(row["offset"]),
            row["alert_type"],
            float(row["prediction"]),
            row["prediction_label"],
            _probability_json(row["probability"]),
            row["scenario"],
            row["ground_truth"],
            row["source_label"],
        )
        for row in rows
    ]
    connection = _open_history_database(history_db)
    try:
        with connection:
            connection.executemany(
                """
                INSERT OR IGNORE INTO alerts (
                    event_timestamp, kafka_timestamp, topic, partition, offset,
                    alert_type, prediction, prediction_label, probability,
                    scenario, ground_truth, source_label
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
    finally:
        connection.close()


def _read_snapshot(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            content = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []
    if not isinstance(content, list):
        return []
    return [item for item in content if isinstance(item, dict)]


def _atomic_write_snapshot(path: Path, rows: list[dict[str, Any]]) -> None:
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, ensure_ascii=False, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary_path, path)


def initialize_dashboard_snapshots(output_dir: Path) -> None:
    """Reset transient dashboard state so an old demo is never shown as current."""
    output_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write_snapshot(output_dir / METRICS_SNAPSHOT_NAME, [])
    _atomic_write_snapshot(output_dir / ALERTS_SNAPSHOT_NAME, [])


def update_metrics_snapshot(batch: DataFrame, output_dir: Path) -> None:
    """Upsert update-mode windows and retain only the newest bounded history."""
    path = output_dir / METRICS_SNAPSHOT_NAME
    windows = {
        (row.get("window_start"), row.get("window_end")): row
        for row in _read_snapshot(path)
    }
    for row in _collect_json_rows(batch.select(*METRICS_OUTPUT_COLUMNS)):
        windows[(row.get("window_start"), row.get("window_end"))] = row
    recent_windows = sorted(
        windows.values(), key=lambda row: str(row.get("window_end", ""))
    )[-MAX_METRIC_WINDOWS:]
    _atomic_write_snapshot(path, recent_windows)


def update_alerts_snapshot(batch: DataFrame, output_dir: Path) -> None:
    """Deduplicate predicted-Attack alerts and retain a small recent feed."""
    path = output_dir / ALERTS_SNAPSHOT_NAME
    alerts = {
        (row.get("topic"), row.get("partition"), row.get("offset")): row
        for row in _read_snapshot(path)
    }
    for row in _collect_json_rows(batch.select(*ALERT_OUTPUT_COLUMNS)):
        alerts[(row.get("topic"), row.get("partition"), row.get("offset"))] = row
    recent_alerts = sorted(
        alerts.values(),
        key=lambda row: (
            str(row.get("event_timestamp", "")),
            str(row.get("topic", "")),
            int(row.get("partition", -1)),
            int(row.get("offset", -1)),
        ),
    )[-MAX_ALERTS:]
    _atomic_write_snapshot(path, recent_alerts)


def start_debug_queries(
    prediction_metrics: DataFrame,
    alerts: DataFrame,
    invalid_events: DataFrame,
) -> tuple[StreamingQuery, StreamingQuery, StreamingQuery]:
    """Start console-only metrics, alert, and invalid-event queries."""
    metrics_query = (
        prediction_metrics.select(*METRICS_OUTPUT_COLUMNS)
        .writeStream.queryName("phase7_metrics")
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .start()
    )
    alerts_query = (
        alerts.writeStream.queryName("phase7_alerts")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    invalid_query = (
        invalid_events.writeStream.queryName("phase7_invalid_events")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    return metrics_query, alerts_query, invalid_query


def start_dashboard_queries(
    prediction_metrics: DataFrame,
    alerts: DataFrame,
    invalid_events: DataFrame,
    output_dir: Path,
) -> tuple[StreamingQuery, StreamingQuery, StreamingQuery]:
    """Start bounded snapshot writers plus the existing invalid console output."""
    initialize_dashboard_snapshots(output_dir)
    metrics_query = (
        prediction_metrics.writeStream.queryName("phase8_metrics_snapshot")
        .outputMode("update")
        .foreachBatch(
            lambda batch, _batch_id: update_metrics_snapshot(batch, output_dir)
        )
        .start()
    )
    alerts_query = (
        alerts.writeStream.queryName("phase8_alerts_snapshot")
        .outputMode("append")
        .foreachBatch(
            lambda batch, _batch_id: update_alerts_snapshot(batch, output_dir)
        )
        .start()
    )
    invalid_query = (
        invalid_events.writeStream.queryName("phase8_invalid_events")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    return metrics_query, alerts_query, invalid_query


def start_history_queries(
    predictions: DataFrame,
    prediction_metrics: DataFrame,
    alerts: DataFrame,
    invalid_events: DataFrame,
    history_db: Path,
    dashboard_output_dir: Path | None,
) -> tuple[StreamingQuery, StreamingQuery, StreamingQuery, StreamingQuery]:
    """Persist all three histories and optionally refresh Phase 8 snapshots."""
    if dashboard_output_dir is not None:
        initialize_dashboard_snapshots(dashboard_output_dir)

    def write_metrics(batch: DataFrame, _batch_id: int) -> None:
        if dashboard_output_dir is not None:
            update_metrics_snapshot(batch, dashboard_output_dir)
        persist_metric_history(batch, history_db)

    def write_alerts(batch: DataFrame, _batch_id: int) -> None:
        if dashboard_output_dir is not None:
            update_alerts_snapshot(batch, dashboard_output_dir)
        persist_alert_history(batch, history_db)

    prediction_query = (
        predictions.select(*PREDICTION_HISTORY_COLUMNS)
        .writeStream.queryName("phase9_prediction_history")
        .outputMode("append")
        .foreachBatch(
            lambda batch, _batch_id: persist_prediction_history(batch, history_db)
        )
        .start()
    )
    metrics_query = (
        prediction_metrics.writeStream.queryName("phase9_metric_history")
        .outputMode("update")
        .foreachBatch(write_metrics)
        .start()
    )
    alerts_query = (
        alerts.writeStream.queryName("phase9_alert_history")
        .outputMode("append")
        .foreachBatch(write_alerts)
        .start()
    )
    invalid_query = (
        invalid_events.writeStream.queryName("phase9_invalid_events")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    return prediction_query, metrics_query, alerts_query, invalid_query


def run() -> int:
    args = parse_args()
    resolved_model_path = args.model.expanduser().resolve()
    dashboard_output_dir = (
        args.dashboard_output_dir.expanduser().resolve()
        if args.dashboard_output_dir is not None
        else None
    )
    history_db = (
        args.history_db.expanduser().resolve()
        if args.history_db is not None
        else None
    )
    if not resolved_model_path.is_dir():
        raise FileNotFoundError(
            f"weighted model directory not found: {resolved_model_path}"
        )
    if history_db is not None:
        initialize_history_database(history_db)

    spark = (
        SparkSession.builder.appName("CICIoT2023Phase7RealtimeAnalytics")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    queries: tuple[StreamingQuery, ...] = ()
    stop_requested = threading.Event()
    previous_sigint_handler = signal.getsignal(signal.SIGINT)

    def request_stop(_signum: int, _frame: object) -> None:
        stop_requested.set()

    signal.signal(signal.SIGINT, request_stop)
    interrupted = False
    clean_shutdown = False
    try:
        model = load_realtime_model(resolved_model_path)
        predictions, invalid_events = build_prediction_stream(
            spark,
            model,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            starting_offsets=args.starting_offsets,
        )
        prediction_metrics = build_realtime_metrics(predictions)
        alerts = build_alert_stream(predictions)

        print("=== PHASE 7 REALTIME ANALYTICS AND ALERTS ===")
        print(f"Spark version: {spark.version}")
        print(f"Kafka bootstrap servers: {args.bootstrap_servers}")
        print(f"Topic: {args.topic}")
        print(f"Starting offsets: {args.starting_offsets}")
        print(f"Model path: {resolved_model_path}")
        print("Inference reused from Phase 6: YES")
        print(f"Event-time column: {EVENT_TIME_COLUMN}")
        print(f"Window duration: {WINDOW_DURATION}")
        print(f"Slide duration: {SLIDE_DURATION}")
        print(f"Watermark: {WATERMARK_DELAY}")
        print("Demo status thresholds: <0.20 Normal, <0.80 Warning, >=0.80 Critical")
        print("Status source: predicted attack_rate only")
        print("Alert condition: prediction == 1.0")
        print("Training inside job: NO")
        if history_db is None:
            print("Persistence: NO")
        else:
            print("Persistence: SQLite")
            print(f"History database: {history_db}")
            print("History reset at startup: NO")
        if history_db is not None:
            if dashboard_output_dir is not None:
                print(f"Dashboard snapshot directory: {dashboard_output_dir}")
                print(f"Maximum metric windows: {MAX_METRIC_WINDOWS}")
                print(f"Maximum alerts: {MAX_ALERTS}")
            queries = start_history_queries(
                predictions,
                prediction_metrics,
                alerts,
                invalid_events,
                history_db,
                dashboard_output_dir,
            )
            print(
                "Prediction, metric, and alert history queries plus invalid "
                "console query started; press Ctrl+C to stop."
            )
        elif dashboard_output_dir is None:
            queries = start_debug_queries(prediction_metrics, alerts, invalid_events)
            print(
                "Metrics, alert, and invalid console queries started; "
                "press Ctrl+C to stop."
            )
        else:
            print(f"Dashboard snapshot directory: {dashboard_output_dir}")
            print(f"Maximum metric windows: {MAX_METRIC_WINDOWS}")
            print(f"Maximum alerts: {MAX_ALERTS}")
            queries = start_dashboard_queries(
                prediction_metrics,
                alerts,
                invalid_events,
                dashboard_output_dir,
            )
            print(
                "Metrics and alert snapshots plus invalid console query started; "
                "press Ctrl+C to stop."
            )
        while not stop_requested.wait(0.5):
            stopped_queries = [query for query in queries if not query.isActive]
            if stopped_queries:
                exception = stopped_queries[0].exception()
                raise RuntimeError(
                    "streaming query stopped unexpectedly"
                    + (f": {exception}" if exception is not None else "")
                )
        interrupted = True
        logging.getLogger().setLevel(logging.CRITICAL)
        print("Interrupt received; stopping streaming queries...")
    except Py4JError:
        if not stop_requested.is_set():
            raise
        interrupted = True
        logging.getLogger().setLevel(logging.CRITICAL)
        print("Interrupt received while Spark was stopping...")
    finally:
        query_stop_results: list[bool] = []
        for query in queries:
            try:
                if query.isActive:
                    query.stop()
                query_stop_results.append(not query.isActive)
            except Py4JError:
                query_stop_results.append(stop_requested.is_set())
        clean_shutdown = bool(queries) and all(query_stop_results)
        try:
            spark.stop()
        except Py4JError:
            clean_shutdown = clean_shutdown and stop_requested.is_set()
        signal.signal(signal.SIGINT, previous_sigint_handler)
        print("=== FINAL ANALYTICS REPORT ===")
        print(f"interrupted: {str(interrupted).lower()}")
        print(f"clean_shutdown: {str(clean_shutdown).lower()}")

    return 0 if clean_shutdown else 1


def main() -> int:
    try:
        return run()
    except (FileNotFoundError, OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
