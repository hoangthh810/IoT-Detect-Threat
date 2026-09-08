#!/usr/bin/env python3
"""Phase 6 realtime inference for validated CICIoT2023 Kafka events."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from pathlib import Path

from py4j.protocol import Py4JError
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery

from preprocess import FEATURES_COLUMN, SELECTED_FEATURES, preprocess_dataframe
from stream_ingest import (
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_TOPIC,
    STARTING_OFFSETS,
    build_validated_stream,
)


DEFAULT_MODEL_PATH = Path("models/random_forest_full_weighted")
NORMAL_PREDICTION = 0.0
ATTACK_PREDICTION = 1.0
PREDICTION_OUTPUT_COLUMNS = (
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "event_time",
    "event_timestamp",
    "scenario",
    "ground_truth",
    "source_label",
    "prediction",
    "prediction_label",
    "probability",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply the frozen weighted Random Forest to valid Kafka events."
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
    return parser.parse_args()


def load_realtime_model(model_path: Path) -> RandomForestClassificationModel:
    """Load and validate the one frozen binary model used for realtime inference."""
    resolved_path = model_path.expanduser().resolve()
    if not resolved_path.is_dir():
        raise FileNotFoundError(f"weighted model directory not found: {resolved_path}")

    try:
        model = RandomForestClassificationModel.load(str(resolved_path))
    except Exception as exc:
        raise RuntimeError(
            f"failed to load weighted Random Forest from {resolved_path}: {exc}"
        ) from exc

    if model.numFeatures != len(SELECTED_FEATURES):
        raise RuntimeError(
            "model feature contract mismatch: "
            f"artifact expects {model.numFeatures}, code requires "
            f"{len(SELECTED_FEATURES)}"
        )
    if model.numClasses != 2:
        raise RuntimeError(
            f"model class contract mismatch: expected 2, got {model.numClasses}"
        )
    if model.getFeaturesCol() != FEATURES_COLUMN:
        raise RuntimeError(
            "model features column mismatch: "
            f"expected {FEATURES_COLUMN}, got {model.getFeaturesCol()}"
        )
    if model.getPredictionCol() != "prediction":
        raise RuntimeError(
            "model prediction column mismatch: "
            f"expected prediction, got {model.getPredictionCol()}"
        )
    if model.getProbabilityCol() != "probability":
        raise RuntimeError(
            "model probability column mismatch: "
            f"expected probability, got {model.getProbabilityCol()}"
        )
    return model


def add_prediction_label(predictions: DataFrame) -> DataFrame:
    """Map the frozen model's binary numeric prediction to its display label."""
    unexpected_prediction = F.raise_error(
        F.concat(
            F.lit("binary prediction contract violation: "),
            F.col("prediction").cast("string"),
        )
    )
    return predictions.withColumn(
        "prediction_label",
        F.when(
            F.col("prediction") == F.lit(NORMAL_PREDICTION), F.lit("Normal")
        )
        .when(
            F.col("prediction") == F.lit(ATTACK_PREDICTION), F.lit("Attack")
        )
        .otherwise(unexpected_prediction),
    )


def infer_valid_events(
    valid_events: DataFrame,
    model: RandomForestClassificationModel,
) -> DataFrame:
    """Apply shared unlabeled preprocessing and the frozen model to valid events."""
    prepared_events = preprocess_dataframe(valid_events, require_label=False)
    if FEATURES_COLUMN not in prepared_events.columns:
        raise RuntimeError("shared preprocessing did not create the features vector")
    return add_prediction_label(model.transform(prepared_events))


def build_prediction_stream(
    spark: SparkSession,
    model: RandomForestClassificationModel,
    *,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    starting_offsets: str = "latest",
) -> tuple[DataFrame, DataFrame]:
    """Return prediction and invalid streams without duplicating Phase 5 parsing."""
    valid_events, invalid_events = build_validated_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        starting_offsets=starting_offsets,
    )
    return infer_valid_events(valid_events, model), invalid_events


def start_debug_queries(
    predictions: DataFrame, invalid_events: DataFrame
) -> tuple[StreamingQuery, StreamingQuery]:
    """Start inspectable prediction and invalid-event console sinks."""
    prediction_query = (
        predictions.select(*PREDICTION_OUTPUT_COLUMNS)
        .writeStream.queryName("phase6_predictions")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    invalid_query = (
        invalid_events.writeStream.queryName("phase6_invalid_events")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    return prediction_query, invalid_query


def run() -> int:
    args = parse_args()
    resolved_model_path = args.model.expanduser().resolve()
    if not resolved_model_path.is_dir():
        raise FileNotFoundError(
            f"weighted model directory not found: {resolved_model_path}"
        )

    spark = (
        SparkSession.builder.appName("CICIoT2023Phase6RealtimeInference")
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

        print("=== PHASE 6 REALTIME INFERENCE ===")
        print(f"Spark version: {spark.version}")
        print(f"Kafka bootstrap servers: {args.bootstrap_servers}")
        print(f"Topic: {args.topic}")
        print(f"Starting offsets: {args.starting_offsets}")
        print(f"Model path: {resolved_model_path}")
        print("Model type: RandomForestClassificationModel")
        print(f"Feature vector dimension: {model.numFeatures}")
        print(f"Model classes: {model.numClasses}")
        print("Prediction contract: 0.0=Normal, 1.0=Attack")
        print("Training inside job: NO")
        print("Batch deduplication: NO")
        print("Class weight input: NO")
        print(f"Prediction output columns: {', '.join(PREDICTION_OUTPUT_COLUMNS)}")

        queries = start_debug_queries(predictions, invalid_events)
        print("Prediction and invalid console queries started; press Ctrl+C to stop.")
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
        print("=== FINAL INFERENCE REPORT ===")
        print(f"interrupted: {str(interrupted).lower()}")
        print(f"clean_shutdown: {str(clean_shutdown).lower()}")

    return 0 if clean_shutdown else 1


def main() -> int:
    try:
        return run()
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
