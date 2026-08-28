"""Small-sample data understanding for CICIoT2023 MERGED_CSV.

This script intentionally does not clean data, assemble features, or train a model.
It can stream a bounded number of rows from a few CSV partitions into a development
sample, then inspect that sample with a local Spark DataFrame.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
from pathlib import Path
from typing import Sequence


NORMAL_LIKE_LABELS = {"benign", "benigntraffic", "normal", "normaltraffic"}


def _normalized_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def create_sample(
    source_paths: Sequence[Path], output_path: Path, rows_per_file: int
) -> None:
    """Copy only the first bounded rows of schema-compatible CSV files."""
    if rows_per_file < 1:
        raise ValueError("rows_per_file must be positive")
    if not source_paths:
        raise ValueError("at least one source CSV is required")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    expected_header: list[str] | None = None
    total_rows = 0
    per_source: list[tuple[Path, int]] = []

    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file, lineterminator="\n")

        for source_path in source_paths:
            copied = 0
            with source_path.open("r", encoding="utf-8-sig", newline="") as source_file:
                reader = csv.reader(source_file)
                try:
                    header = next(reader)
                except StopIteration as exc:
                    raise ValueError(f"empty CSV: {source_path}") from exc

                if expected_header is None:
                    expected_header = header
                    writer.writerow(header)
                elif header != expected_header:
                    raise ValueError(f"schema mismatch in {source_path}")

                for row in reader:
                    if copied >= rows_per_file:
                        break
                    if len(row) != len(expected_header):
                        raise ValueError(
                            f"malformed row in {source_path}: expected "
                            f"{len(expected_header)} fields, found {len(row)}"
                        )
                    writer.writerow(row)
                    copied += 1
                    total_rows += 1

            per_source.append((source_path, copied))

    print(f"Sample written: {output_path.resolve()}")
    print(f"Columns: {len(expected_header or [])}")
    print(f"Data rows: {total_rows}")
    for source_path, copied in per_source:
        print(f"  {source_path}: {copied} rows")


def _resolve_label_column(columns: Sequence[str], requested: str | None) -> str:
    if requested:
        matches = [column for column in columns if column.casefold() == requested.casefold()]
        if len(matches) == 1:
            return matches[0]
        raise ValueError(f"label column not found: {requested}")

    conventional_names = {"label", "class", "target"}
    matches = [column for column in columns if column.casefold() in conventional_names]
    if len(matches) != 1:
        raise ValueError(
            "could not identify exactly one label column; pass --label-column explicitly"
        )
    return matches[0]


def analyze_sample(input_path: Path, requested_label: str | None) -> None:
    """Run bounded EDA with Spark and print concise evidence to the console."""
    try:
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import NumericType
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "PySpark is not installed. Install requirements.txt in an isolated environment."
        ) from exc

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("CICIoT2023Phase1EDA")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        dataframe = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .option("mode", "FAILFAST")
            .csv(str(input_path.resolve()))
            .cache()
        )
        row_count = dataframe.count()
        label_column = _resolve_label_column(dataframe.columns, requested_label)
        numeric_columns = [
            field.name
            for field in dataframe.schema.fields
            if isinstance(field.dataType, NumericType)
        ]
        non_numeric_columns = [
            field.name
            for field in dataframe.schema.fields
            if not isinstance(field.dataType, NumericType)
        ]

        print("\n=== DATASET OVERVIEW ===")
        print(f"Input: {input_path.resolve()}")
        print(f"Rows: {row_count}")
        print(f"Columns: {len(dataframe.columns)}")
        print(f"Label column: {label_column}")
        print(f"Numeric columns ({len(numeric_columns)}): {', '.join(numeric_columns)}")
        print(
            f"String/categorical columns ({len(non_numeric_columns)}): "
            f"{', '.join(non_numeric_columns)}"
        )

        print("\n=== SPARK SCHEMA ===")
        dataframe.printSchema()

        print("\n=== LABEL DISTRIBUTION ===")
        label_rows = (
            dataframe.groupBy(label_column)
            .count()
            .orderBy(F.desc("count"), F.asc(label_column))
            .collect()
        )
        observed_labels: list[tuple[str, int]] = []
        for row in label_rows:
            label_value = "<NULL>" if row[label_column] is None else str(row[label_column])
            count = int(row["count"])
            observed_labels.append((label_value, count))
            print(f"{label_value}: {count} ({count / row_count:.4%})")

        normal_labels = [
            label
            for label, _ in observed_labels
            if label != "<NULL>" and _normalized_label(label) in NORMAL_LIKE_LABELS
        ]
        print("\n=== BINARY MAPPING PROPOSAL ===")
        if len(normal_labels) == 1:
            normal_label = normal_labels[0]
            normal_count = sum(
                count for label, count in observed_labels if label == normal_label
            )
            attack_count = row_count - normal_count
            print(f"Observed normal-like label: {normal_label!r} -> Normal")
            print("All other observed non-null labels -> Attack")
            print(f"Normal: {normal_count} ({normal_count / row_count:.4%})")
            print(f"Attack: {attack_count} ({attack_count / row_count:.4%})")
        else:
            print(
                "No unique normal-like label was verified; do not implement a binary mapping yet."
            )

        null_row = dataframe.agg(
            *[
                F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
                for column in dataframe.columns
            ]
        ).first()
        nan_row = dataframe.agg(
            *[
                F.sum(
                    F.when(F.isnan(F.col(column).cast("double")), 1).otherwise(0)
                ).alias(column)
                for column in numeric_columns
            ]
        ).first()
        positive_inf_row = dataframe.agg(
            *[
                F.sum(
                    F.when(F.col(column).cast("double") == math.inf, 1).otherwise(0)
                ).alias(column)
                for column in numeric_columns
            ]
        ).first()
        negative_inf_row = dataframe.agg(
            *[
                F.sum(
                    F.when(F.col(column).cast("double") == -math.inf, 1).otherwise(0)
                ).alias(column)
                for column in numeric_columns
            ]
        ).first()

        null_counts = null_row.asDict() if null_row else {}
        nan_counts = nan_row.asDict() if nan_row else {}
        positive_inf_counts = positive_inf_row.asDict() if positive_inf_row else {}
        negative_inf_counts = negative_inf_row.asDict() if negative_inf_row else {}

        numeric_like_string_quality: dict[str, dict[str, int]] = {}
        for column in non_numeric_columns:
            if column == label_column:
                continue
            cast_value = F.col(column).cast("double")
            quality_row = dataframe.agg(
                F.sum(
                    F.when(F.col(column).isNotNull() & cast_value.isNull(), 1).otherwise(0)
                ).alias("invalid_numeric"),
                F.sum(F.when(F.isnan(cast_value), 1).otherwise(0)).alias("nan"),
                F.sum(F.when(cast_value == math.inf, 1).otherwise(0)).alias("positive_inf"),
                F.sum(F.when(cast_value == -math.inf, 1).otherwise(0)).alias("negative_inf"),
            ).first()
            if quality_row:
                numeric_like_string_quality[column] = {
                    key: int(value or 0) for key, value in quality_row.asDict().items()
                }

        duplicate_count = row_count - dataframe.dropDuplicates().count()
        distinct_row = dataframe.agg(
            *[
                F.approx_count_distinct(F.col(column)).alias(column)
                for column in dataframe.columns
            ]
        ).first()
        approximate_distinct = distinct_row.asDict() if distinct_row else {}
        constant_columns = [
            column
            for column, count in approximate_distinct.items()
            if count == 1 and null_counts.get(column, 0) < row_count
        ]
        dominance_columns = [
            column
            for column, count in approximate_distinct.items()
            if count <= 10 and column != label_column
        ]
        if "Number" in dataframe.columns and "Number" not in dominance_columns:
            dominance_columns.append("Number")
        dominant_values: dict[str, tuple[object, int, float]] = {}
        for column in dominance_columns:
            top_row = (
                dataframe.groupBy(column)
                .count()
                .orderBy(F.desc("count"))
                .first()
            )
            if top_row:
                count = int(top_row["count"])
                dominant_values[column] = (
                    top_row[column],
                    count,
                    count / row_count,
                )

        suspected_redundant_pairs = [
            ("AVG", "Tot size"),
            ("IPv", "LLC"),
        ]
        pair_equality: dict[str, tuple[int, float]] = {}
        for left, right in suspected_redundant_pairs:
            if left in dataframe.columns and right in dataframe.columns:
                equal_count = dataframe.filter(
                    F.col(left).eqNullSafe(F.col(right))
                ).count()
                pair_equality[f"{left} == {right}"] = (
                    equal_count,
                    equal_count / row_count,
                )

        print("\n=== DATA QUALITY ===")
        print(f"Exact duplicate rows: {duplicate_count}")
        print(
            "Columns with nulls: "
            + (str({k: v for k, v in null_counts.items() if v}) or "none")
        )
        print(
            "Numeric columns with NaN: "
            + (str({k: v for k, v in nan_counts.items() if v}) or "none")
        )
        print(
            "Numeric columns with +Inf: "
            + (str({k: v for k, v in positive_inf_counts.items() if v}) or "none")
        )
        print(
            "Numeric columns with -Inf: "
            + (str({k: v for k, v in negative_inf_counts.items() if v}) or "none")
        )
        print(
            "String columns castable to numeric (quality after cast): "
            + (str(numeric_like_string_quality) or "none")
        )
        print(
            "Constant columns (approximate distinct count = 1): "
            + (", ".join(constant_columns) or "none")
        )
        print("Dominant values for low-cardinality/suspect columns:")
        for column, (value, count, ratio) in dominant_values.items():
            marker = " (near-constant)" if ratio >= 0.95 else ""
            print(f"  {column}: {value!r} -> {count} ({ratio:.4%}){marker}")
        print("Selected pair equality checks:")
        for pair, (count, ratio) in pair_equality.items():
            print(f"  {pair}: {count}/{row_count} rows ({ratio:.4%})")

        preferred_statistics = [
            "Header_Length",
            "Protocol Type",
            "Time_To_Live",
            "Rate",
            "Tot sum",
            "Min",
            "Max",
            "AVG",
            "Std",
            "Tot size",
            "IAT",
            "Number",
            "Variance",
        ]
        statistics_columns = [
            column for column in preferred_statistics if column in numeric_columns
        ]
        statistics_row = dataframe.agg(
            *[
                expression.alias(f"{column}__{metric}")
                for column in statistics_columns
                for metric, expression in (
                    ("min", F.min(F.col(column))),
                    ("mean", F.avg(F.col(column))),
                    ("max", F.max(F.col(column))),
                )
            ]
        ).first()
        statistics = statistics_row.asDict() if statistics_row else {}

        print("\n=== SELECTED NUMERIC STATISTICS ===")
        for column in statistics_columns:
            print(
                f"{column}: min={statistics[f'{column}__min']}, "
                f"mean={statistics[f'{column}__mean']}, "
                f"max={statistics[f'{column}__max']}"
            )

        print("\n=== LOW-CARDINALITY / INVESTIGATION HINTS ===")
        for column in dataframe.columns:
            count = approximate_distinct.get(column)
            if count is not None and count <= 10:
                print(f"{column}: approximately {count} distinct values")

        print("\n=== PHASE 1 FEATURE GROUPING ===")
        print(
            "Candidate numeric features: all numeric columns require validation; "
            "none are removed in Phase 1."
        )
        if "Rate" in numeric_like_string_quality:
            print(
                "Numeric-looking feature requiring cast/Inf handling in Phase 2: Rate"
            )
        print("Categorical-like numeric feature: Protocol Type")
        print("Potential metadata: none observed in the 40-column CSV schema")
        print(f"Label / direct leakage: {label_column}")
        print(
            "Needs further investigation: Number and correlated aggregate fields "
            "(Tot sum, Min, Max, AVG, Std, Tot size, Variance)."
        )
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample_parser = subparsers.add_parser(
        "sample", help="create a bounded sample without loading full CSV files"
    )
    sample_parser.add_argument(
        "--source", type=Path, action="append", required=True, help="source CSV"
    )
    sample_parser.add_argument("--output", type=Path, required=True)
    sample_parser.add_argument("--rows-per-file", type=int, default=10_000)

    analyze_parser = subparsers.add_parser(
        "analyze", help="run Spark DataFrame EDA on the bounded sample"
    )
    analyze_parser.add_argument("--input", type=Path, required=True)
    analyze_parser.add_argument("--label-column")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "sample":
        create_sample(args.source, args.output, args.rows_per_file)
    elif args.command == "analyze":
        analyze_sample(args.input, args.label_column)


if __name__ == "__main__":
    main()
