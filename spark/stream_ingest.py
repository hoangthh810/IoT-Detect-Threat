#!/usr/bin/env python3
"""Phase 5 Kafka ingestion, JSON parsing, and event validation."""

from __future__ import annotations

import argparse
import logging
import signal
import threading
from functools import reduce

from py4j.protocol import Py4JError
from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DoubleType, MapType, StringType, StructField, StructType

from preprocess import SELECTED_FEATURES


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "iot-network-events"
STARTING_OFFSETS = ("latest", "earliest")
METADATA_FIELDS = ("event_time", "scenario", "ground_truth", "source_label")
REQUIRED_FIELDS = (*METADATA_FIELDS, *SELECTED_FEATURES)
EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

EVENT_SCHEMA = StructType(
    [StructField(field, StringType(), True) for field in METADATA_FIELDS]
    + [StructField(feature, DoubleType(), True) for feature in SELECTED_FEATURES]
)

VALID_OUTPUT_COLUMNS = (
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "event_time",
    "event_timestamp",
    "scenario",
    "ground_truth",
    "source_label",
    *SELECTED_FEATURES,
)
INVALID_OUTPUT_COLUMNS = (
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "raw_json",
    "invalid_reason",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read, parse, and validate CICIoT2023 events from Kafka."
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
    return parser.parse_args()


def build_event_schema() -> StructType:
    """Return the explicit four-metadata plus 27-double event schema."""
    return EVENT_SCHEMA


def read_kafka_stream(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str,
) -> DataFrame:
    """Subscribe to one Kafka topic while leaving offset management to Spark."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def _any_condition(conditions: list[Column]) -> Column:
    return reduce(lambda left, right: left | right, conditions, F.lit(False))


def _raw_field(name: str) -> Column:
    return F.element_at(F.col("_raw_fields"), F.lit(name))


def parse_and_validate_events(kafka_events: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a Kafka-shaped DataFrame into reusable valid and observable invalid rows."""
    kafka_columns = kafka_events.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast(StringType()).alias("raw_json"),
    )

    parsed = (
        kafka_columns.withColumn(
            "_raw_fields",
            F.from_json(
                F.col("raw_json"),
                MapType(StringType(), StringType()),
                {"mode": "PERMISSIVE"},
            ),
        )
        .withColumn(
            "_event",
            F.from_json(F.col("raw_json"), EVENT_SCHEMA, {"mode": "PERMISSIVE"}),
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "raw_json",
            "_raw_fields",
            *[
                F.col(f"_event.`{field}`").alias(field)
                for field in REQUIRED_FIELDS
            ],
        )
        .withColumn(
            "event_timestamp",
            F.try_to_timestamp(F.col("event_time"), F.lit(EVENT_TIME_FORMAT)),
        )
    )

    malformed_json = F.col("_raw_fields").isNull()
    missing_required = (~malformed_json) & _any_condition(
        [_raw_field(field).isNull() for field in REQUIRED_FIELDS]
    )
    invalid_event_time = (
        F.col("event_timestamp").isNull()
        | (~F.col("event_time").rlike(r"^\d{4}-\d{2}-\d{2}T.*Z$"))
    )
    invalid_numeric = _any_condition(
        [
            F.col(f"`{feature}`").isNull()
            | F.isnan(F.col(f"`{feature}`"))
            | (F.col(f"`{feature}`") == float("inf"))
            | (F.col(f"`{feature}`") == float("-inf"))
            for feature in SELECTED_FEATURES
        ]
    )
    invalid_scenario = ~F.col("scenario").isin("NORMAL", "DDOS_BURST", "MIXED")
    invalid_ground_truth = ~F.col("ground_truth").isin("Normal", "Attack")
    invalid_source_label = F.length(F.trim(F.col("source_label"))) == 0

    normal_metadata = (
        (F.col("scenario") == "NORMAL")
        & (F.col("ground_truth") == "Normal")
        & (F.col("source_label") == "BENIGN")
    )
    ddos_metadata = (
        (F.col("scenario") == "DDOS_BURST")
        & (F.col("ground_truth") == "Attack")
        & F.col("source_label").startswith("DDOS-")
    )
    mixed_metadata = (F.col("scenario") == "MIXED") & (
        (
            (F.col("ground_truth") == "Normal")
            & (F.col("source_label") == "BENIGN")
        )
        | (
            (F.col("ground_truth") == "Attack")
            & F.col("source_label").startswith("DDOS-")
        )
    )
    scenario_metadata_mismatch = ~(
        normal_metadata | ddos_metadata | mixed_metadata
    )

    classified = parsed.withColumn(
        "invalid_reason",
        F.when(malformed_json, F.lit("malformed_json"))
        .when(missing_required, F.lit("missing_required_field"))
        .when(invalid_event_time, F.lit("invalid_event_time"))
        .when(invalid_numeric, F.lit("invalid_numeric_value"))
        .when(invalid_scenario, F.lit("invalid_scenario"))
        .when(invalid_ground_truth, F.lit("invalid_ground_truth"))
        .when(invalid_source_label, F.lit("invalid_source_label"))
        .when(
            scenario_metadata_mismatch,
            F.lit("scenario_metadata_mismatch"),
        ),
    )

    valid_events = classified.filter(F.col("invalid_reason").isNull()).select(
        *VALID_OUTPUT_COLUMNS
    )
    invalid_events = classified.filter(F.col("invalid_reason").isNotNull()).select(
        *INVALID_OUTPUT_COLUMNS
    )
    return valid_events, invalid_events


def build_validated_stream(
    spark: SparkSession,
    *,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    starting_offsets: str = "latest",
) -> tuple[DataFrame, DataFrame]:
    """Build Phase 5's reusable valid and invalid streaming DataFrames."""
    kafka_events = read_kafka_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        starting_offsets=starting_offsets,
    )
    return parse_and_validate_events(kafka_events)


def start_debug_queries(
    valid_events: DataFrame, invalid_events: DataFrame
) -> tuple[StreamingQuery, StreamingQuery]:
    """Start compact console sinks for valid rows and invalid diagnostics."""
    valid_query = (
        valid_events.select(
            "kafka_timestamp",
            "partition",
            "offset",
            "event_time",
            "scenario",
            "ground_truth",
            "source_label",
            "Header_Length",
            "Rate",
        )
        .writeStream.queryName("phase5_valid_events")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    invalid_query = (
        invalid_events.writeStream.queryName("phase5_invalid_events")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    return valid_query, invalid_query


def run() -> int:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("CICIoT2023Phase5Ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    valid_events, invalid_events = build_validated_stream(
        spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
    )

    print("=== PHASE 5 STRUCTURED STREAMING INGESTION ===")
    print(f"Spark version: {spark.version}")
    print(f"Kafka bootstrap servers: {args.bootstrap_servers}")
    print(f"Topic: {args.topic}")
    print(f"Starting offsets: {args.starting_offsets}")
    print(f"Event fields: {len(METADATA_FIELDS)} metadata + {len(SELECTED_FEATURES)} doubles")
    print(f"Valid output columns: {', '.join(VALID_OUTPUT_COLUMNS)}")
    print(f"Invalid output columns: {', '.join(INVALID_OUTPUT_COLUMNS)}")

    queries: tuple[StreamingQuery, ...] = ()
    stop_requested = threading.Event()
    previous_sigint_handler = signal.getsignal(signal.SIGINT)

    def request_stop(_signum: int, _frame: object) -> None:
        stop_requested.set()

    signal.signal(signal.SIGINT, request_stop)
    interrupted = False
    clean_shutdown = False
    try:
        queries = start_debug_queries(valid_events, invalid_events)
        print("Valid and invalid console queries started; press Ctrl+C to stop.")
        while not stop_requested.wait(0.5):
            failures = [query.exception() for query in queries if not query.isActive]
            if failures:
                raise RuntimeError(f"streaming query failed: {failures[0]}")
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
        print("=== FINAL STREAM REPORT ===")
        print(f"interrupted: {str(interrupted).lower()}")
        print(f"clean_shutdown: {str(clean_shutdown).lower()}")

    return 0 if clean_shutdown else 1


if __name__ == "__main__":
    raise SystemExit(run())
