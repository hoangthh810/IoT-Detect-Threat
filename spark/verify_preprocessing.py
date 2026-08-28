"""Focused pre-Phase-3 verification for labels and duplicate split leakage."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from preprocess import (
    BINARY_LABEL_COLUMN,
    KNOWN_LABELS,
    LABEL_COLUMN,
    RAW_SCHEMA,
    SELECTED_FEATURES,
    SPLIT_SEED,
    SPLIT_WEIGHTS,
    preprocess_dataframe,
    read_raw_csv,
    split_dataset,
)


FINGERPRINT_COLUMN = "_example_fingerprint"


def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def verify_label_coverage(dataset_dir: Path) -> None:
    csv_files = sorted(dataset_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"no CSV files found in {dataset_dir}")

    spark = create_spark("CICIoT2023FullLabelVerification")
    try:
        # Pass explicit file paths to avoid Hadoop's native directory-listing call on
        # Windows. Selecting Label before the action lets Spark's CSV column pruning
        # avoid materializing feature columns.
        labels = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(RAW_SCHEMA)
            .csv([str(path.resolve()) for path in csv_files])
            .select(
                F.trim(F.col(LABEL_COLUMN)).alias(LABEL_COLUMN)
            )
        )
        counts = (
            labels.groupBy(LABEL_COLUMN)
            .count()
            .orderBy(F.desc("count"), F.asc_nulls_first(LABEL_COLUMN))
            .collect()
        )

        observed_counts: dict[str, int] = {}
        null_rows = 0
        empty_rows = 0
        for row in counts:
            value = row[LABEL_COLUMN]
            count = int(row["count"])
            if value is None:
                null_rows += count
            elif value == "":
                empty_rows += count
            else:
                observed_counts[str(value)] = count

        normalized_observed = {label.upper() for label in observed_counts}
        labels_missing_from_known = sorted(normalized_observed - KNOWN_LABELS)
        known_not_observed = sorted(KNOWN_LABELS - normalized_observed)
        unexpected_rows = null_rows + empty_rows
        coverage_verified = not labels_missing_from_known

        normal_rows = sum(
            count for label, count in observed_counts.items() if label.upper() == "BENIGN"
        )
        attack_rows = sum(
            count
            for label, count in observed_counts.items()
            if label.upper() in KNOWN_LABELS and label.upper() != "BENIGN"
        )

        print("\n=== FULL-DATASET LABEL VERIFICATION ===")
        print(f"CSV partitions scanned: {len(csv_files)}")
        print(f"Distinct non-empty labels: {len(observed_counts)}")
        print(f"BENIGN rows: {normal_rows}")
        print(f"Known attack rows: {attack_rows}")
        print(f"Null label rows: {null_rows}")
        print(f"Empty label rows: {empty_rows}")
        print("Observed labels and counts:")
        for label, count in sorted(
            observed_counts.items(), key=lambda item: (-item[1], item[0])
        ):
            print(f"  {label}: {count}")
        print(f"Current KNOWN_LABELS ({len(KNOWN_LABELS)}):")
        for label in sorted(KNOWN_LABELS):
            print(f"  {label}")
        print(
            "Labels present in dataset but missing from KNOWN_LABELS: "
            + (", ".join(labels_missing_from_known) or "none")
        )
        print(
            "KNOWN_LABELS not observed in full dataset: "
            + (", ".join(known_not_observed) or "none")
        )
        print(
            "Known label coverage VERIFIED"
            if coverage_verified
            else "Known label coverage NOT VERIFIED"
        )
        print(
            "All label values valid"
            if unexpected_rows == 0
            else f"Invalid/null label rows to be filtered: {unexpected_rows}"
        )
    finally:
        spark.stop()


def _with_fingerprint(dataframe: DataFrame) -> DataFrame:
    fields = [*SELECTED_FEATURES, BINARY_LABEL_COLUMN]
    payload = F.to_json(F.struct(*[F.col(column).alias(column) for column in fields]))
    return dataframe.withColumn(FINGERPRINT_COLUMN, F.sha2(payload, 256))


def _overlap(left: DataFrame, right: DataFrame) -> tuple[int, int, int]:
    overlap_fingerprints = (
        left.select(FINGERPRINT_COLUMN)
        .distinct()
        .join(right.select(FINGERPRINT_COLUMN).distinct(), FINGERPRINT_COLUMN, "inner")
        .cache()
    )
    unique_fingerprints = overlap_fingerprints.count()
    affected_left = left.join(overlap_fingerprints, FINGERPRINT_COLUMN, "inner").count()
    affected_right = right.join(overlap_fingerprints, FINGERPRINT_COLUMN, "inner").count()
    return unique_fingerprints, affected_left, affected_right


def verify_sample_duplicates(sample_path: Path) -> None:
    spark = create_spark("CICIoT2023DuplicateLeakageVerification")
    try:
        raw = read_raw_csv(spark, sample_path).cache()
        total_rows = raw.count()
        distinct_exact_rows = raw.dropDuplicates(raw.columns).count()
        duplicate_rows = total_rows - distinct_exact_rows

        prepared = preprocess_dataframe(raw, require_label=True).cache()
        train, validation, test = [
            _with_fingerprint(split).cache()
            for split in split_dataset(prepared, seed=SPLIT_SEED)
        ]
        train_rows = train.count()
        validation_rows = validation.count()
        test_rows = test.count()

        train_validation = _overlap(train, validation)
        train_test = _overlap(train, test)
        validation_test = _overlap(validation, test)

        print("\n=== DEVELOPMENT-SAMPLE EXACT DUPLICATES ===")
        print(f"Sample rows: {total_rows}")
        print(f"Distinct exact source rows: {distinct_exact_rows}")
        print(f"Duplicate rows: {duplicate_rows}")
        print(f"Duplicate percentage: {duplicate_rows / total_rows:.4%}")

        print(
            f"\n=== SPLIT LEAKAGE (seed={SPLIT_SEED}, "
            f"weights={SPLIT_WEIGHTS}) ==="
        )
        print(f"Train rows: {train_rows}")
        print(f"Validation rows: {validation_rows}")
        print(f"Test rows: {test_rows}")
        print(
            "Train <-> Validation: "
            f"unique_overlap={train_validation[0]}, "
            f"affected_train_rows={train_validation[1]}, "
            f"affected_validation_rows={train_validation[2]}"
        )
        print(
            "Train <-> Test: "
            f"unique_overlap={train_test[0]}, "
            f"affected_train_rows={train_test[1]}, "
            f"affected_test_rows={train_test[2]}"
        )
        print(
            "Validation <-> Test: "
            f"unique_overlap={validation_test[0]}, "
            f"affected_validation_rows={validation_test[1]}, "
            f"affected_test_rows={validation_test[2]}"
        )
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    labels_parser = subparsers.add_parser(
        "labels", help="scan only Label across all local CSV partitions"
    )
    labels_parser.add_argument(
        "--input",
        type=Path,
        default=Path("MERGED_CSV/MERGED_CSV"),
    )

    duplicates_parser = subparsers.add_parser(
        "duplicates", help="check exact duplicates and split fingerprint overlap"
    )
    duplicates_parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/sample/ciciot2023_phase1_sample.csv"),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "labels":
        verify_label_coverage(args.input)
    elif args.command == "duplicates":
        verify_sample_duplicates(args.input)


if __name__ == "__main__":
    main()
