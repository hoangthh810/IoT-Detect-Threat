"""Shared CICIoT2023 preprocessing for offline and streaming DataFrames.

Phase 2 intentionally stops at ML-ready rows. This module does not fit a model,
calculate model metrics, resample classes, or scale features.
"""

from __future__ import annotations

import argparse
import math
from functools import reduce
from pathlib import Path
from typing import Sequence

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


LABEL_COLUMN = "Label"
BINARY_LABEL_COLUMN = "binary_label"
FEATURES_COLUMN = "features"
SPLIT_SEED = 42
SPLIT_WEIGHTS = (0.70, 0.15, 0.15)

NUMERIC_SOURCE_COLUMNS = [
    "Header_Length",
    "Protocol Type",
    "Time_To_Live",
    "Rate",
    "fin_flag_number",
    "syn_flag_number",
    "rst_flag_number",
    "psh_flag_number",
    "ack_flag_number",
    "ece_flag_number",
    "cwr_flag_number",
    "ack_count",
    "syn_count",
    "fin_count",
    "rst_count",
    "HTTP",
    "HTTPS",
    "DNS",
    "Telnet",
    "SMTP",
    "SSH",
    "IRC",
    "TCP",
    "UDP",
    "DHCP",
    "ARP",
    "ICMP",
    "IGMP",
    "IPv",
    "LLC",
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

SELECTED_FEATURES = [
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
]

EXCLUDED_FEATURES = {
    "Protocol Type": (
        "categorical protocol code with ordinal ambiguity; selected protocol "
        "indicator columns already preserve protocol information"
    ),
    "ece_flag_number": "zero in 99.9333% of Phase 1 sample rows",
    "cwr_flag_number": "zero in 99.9667% of Phase 1 sample rows",
    "Telnet": "zero in 99.9167% of Phase 1 sample rows",
    "SMTP": "zero in 99.9200% of Phase 1 sample rows",
    "SSH": "zero in 99.7100% of Phase 1 sample rows",
    "IRC": "zero in 99.9233% of Phase 1 sample rows",
    "DHCP": "zero in 99.4933% of Phase 1 sample rows",
    "IGMP": "zero in 99.9300% of Phase 1 sample rows",
    "LLC": "identical to IPv in all 30,000 Phase 1 sample rows",
    "Tot size": "identical to AVG in all 30,000 Phase 1 sample rows",
    "Number": "equals 100 in 94.99% of Phase 1 sample rows",
}

KNOWN_ATTACK_LABELS = {
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
KNOWN_LABELS = {"BENIGN", *KNOWN_ATTACK_LABELS}

# Read raw values as strings first so invalid numeric tokens are observable before cast.
RAW_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in NUMERIC_SOURCE_COLUMNS]
    + [StructField(LABEL_COLUMN, StringType(), True)]
)


def read_raw_csv(spark: SparkSession, input_path: str | Path) -> DataFrame:
    """Read CICIoT2023 CSV with an explicit raw schema and validated header order."""
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .csv(str(Path(input_path).resolve()))
    )


def _require_columns(dataframe: DataFrame, required: Sequence[str]) -> None:
    missing = [column for column in required if column not in dataframe.columns]
    if missing:
        raise ValueError(f"missing required columns: {', '.join(missing)}")


def _numeric_problem_conditions(column: str) -> dict[str, Column]:
    source = F.col(column)
    text = F.trim(source.cast("string"))
    cast_value = source.cast(DoubleType())
    missing = source.isNull() | (text == "")
    invalid_cast = (~missing) & cast_value.isNull()
    nan = cast_value.isNotNull() & F.isnan(cast_value)
    positive_inf = cast_value == math.inf
    negative_inf = cast_value == -math.inf
    return {
        "missing": missing,
        "invalid_cast": invalid_cast,
        "nan": nan,
        "positive_inf": positive_inf,
        "negative_inf": negative_inf,
    }


def _any_problem(columns: Sequence[str], problem: str | None = None) -> Column:
    conditions: list[Column] = []
    for column in columns:
        column_conditions = _numeric_problem_conditions(column)
        if problem is None:
            conditions.extend(column_conditions.values())
        else:
            conditions.append(column_conditions[problem])
    return reduce(lambda left, right: left | right, conditions, F.lit(False))


def _invalid_label_condition(dataframe: DataFrame) -> Column:
    _require_columns(dataframe, [LABEL_COLUMN])
    label = F.upper(F.trim(F.col(LABEL_COLUMN).cast("string")))
    return F.col(LABEL_COLUMN).isNull() | (label == "") | (~label.isin(sorted(KNOWN_LABELS)))


def cast_numeric_columns(
    dataframe: DataFrame,
    *,
    required_columns: Sequence[str] = NUMERIC_SOURCE_COLUMNS,
) -> DataFrame:
    """Cast available source features after validating the required contract."""
    _require_columns(dataframe, required_columns)
    numeric_columns = set(NUMERIC_SOURCE_COLUMNS) & set(dataframe.columns)
    return dataframe.select(
        *[
            F.col(column).cast(DoubleType()).alias(column)
            if column in numeric_columns
            else F.col(column)
            for column in dataframe.columns
        ]
    )


def map_binary_label(dataframe: DataFrame) -> DataFrame:
    """Preserve Label and add 0.0=Normal, 1.0=Attack for verified labels only."""
    _require_columns(dataframe, [LABEL_COLUMN])
    normalized_label = F.upper(F.trim(F.col(LABEL_COLUMN).cast("string")))
    return (
        dataframe.withColumn(LABEL_COLUMN, normalized_label)
        .withColumn(
            BINARY_LABEL_COLUMN,
            F.when(F.col(LABEL_COLUMN) == "BENIGN", F.lit(0.0))
            .when(F.col(LABEL_COLUMN).isin(sorted(KNOWN_ATTACK_LABELS)), F.lit(1.0))
            .otherwise(F.lit(None).cast(DoubleType())),
        )
    )


def assemble_features(dataframe: DataFrame) -> DataFrame:
    """Create the fixed-size model vector from the explicit baseline feature list."""
    _require_columns(dataframe, SELECTED_FEATURES)
    assembler = VectorAssembler(
        inputCols=SELECTED_FEATURES,
        outputCol=FEATURES_COLUMN,
        handleInvalid="error",
    )
    return assembler.transform(dataframe)


def preprocess_dataframe(
    dataframe: DataFrame, *, require_label: bool, include_features_vector: bool = True
) -> DataFrame:
    """Apply the shared row-wise preprocessing path to batch or streaming data.

    Set require_label=True for offline training/evaluation rows. Set it to False for
    unlabeled streaming inference. The feature validation/casting/selection logic is
    identical in both cases, and extra metadata columns remain separate from the
    VectorAssembler input list.
    """
    required_numeric_columns = (
        NUMERIC_SOURCE_COLUMNS if require_label else SELECTED_FEATURES
    )
    _require_columns(dataframe, required_numeric_columns)
    valid_rows = ~_any_problem(SELECTED_FEATURES)
    if require_label:
        valid_rows = valid_rows & (~_invalid_label_condition(dataframe))

    result = cast_numeric_columns(
        dataframe.filter(valid_rows),
        required_columns=required_numeric_columns,
    )
    if require_label:
        result = map_binary_label(result)
    if include_features_vector:
        result = assemble_features(result)
    return result


def deduplicate_model_examples(dataframe: DataFrame) -> DataFrame:
    """Remove repeated offline model examples using the verified identity policy."""
    deduplication_columns = [*SELECTED_FEATURES, BINARY_LABEL_COLUMN]
    _require_columns(dataframe, deduplication_columns)
    return dataframe.dropDuplicates(deduplication_columns)


def split_dataset(
    dataframe: DataFrame,
    *,
    seed: int = SPLIT_SEED,
    model_examples_are_deduplicated: bool = False,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Create reproducible 70/15/15 splits after model-example deduplication."""
    model_examples = (
        dataframe
        if model_examples_are_deduplicated
        else deduplicate_model_examples(dataframe)
    )
    train, validation, test = model_examples.randomSplit(SPLIT_WEIGHTS, seed=seed)
    return train, validation, test


def _count_condition(dataframe: DataFrame, condition: Column) -> int:
    row = dataframe.agg(
        F.sum(F.when(condition, 1).otherwise(0)).alias("count")
    ).first()
    return int((row["count"] if row else 0) or 0)


def _numeric_issue_profile(
    dataframe: DataFrame, columns: Sequence[str]
) -> dict[str, dict[str, int]]:
    expressions: list[Column] = []
    for column in columns:
        for problem, condition in _numeric_problem_conditions(column).items():
            expressions.append(
                F.sum(F.when(condition, 1).otherwise(0)).alias(
                    f"{column}__{problem}"
                )
            )
    row = dataframe.agg(*expressions).first()
    values = row.asDict() if row else {}
    profile: dict[str, dict[str, int]] = {}
    for column in columns:
        counts = {
            problem: int(values.get(f"{column}__{problem}") or 0)
            for problem in (
                "missing",
                "invalid_cast",
                "nan",
                "positive_inf",
                "negative_inf",
            )
        }
        if any(counts.values()):
            profile[column] = counts
    return profile


def _binary_counts(dataframe: DataFrame) -> tuple[int, int]:
    rows = {
        float(row[BINARY_LABEL_COLUMN]): int(row["count"])
        for row in dataframe.groupBy(BINARY_LABEL_COLUMN).count().collect()
    }
    return rows.get(0.0, 0), rows.get(1.0, 0)


def _print_split(name: str, dataframe: DataFrame) -> None:
    rows = dataframe.count()
    normal, attack = _binary_counts(dataframe)
    print(
        f"{name}: rows={rows}, Normal={normal} ({normal / rows:.4%}), "
        f"Attack={attack} ({attack / rows:.4%})"
    )


def validate_preprocessing(input_path: Path) -> None:
    """Run and report Phase 2 validation on a bounded CSV sample."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("CICIoT2023Phase2Preprocessing")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = read_raw_csv(spark, input_path).cache()
        input_rows = raw.count()
        numeric_issues = _numeric_issue_profile(raw, NUMERIC_SOURCE_COLUMNS)
        selected_issues = _numeric_issue_profile(raw, SELECTED_FEATURES)
        invalid_label_rows = _count_condition(raw, _invalid_label_condition(raw))

        row_issue_counts = {
            problem: _count_condition(raw, _any_problem(SELECTED_FEATURES, problem))
            for problem in (
                "missing",
                "invalid_cast",
                "nan",
                "positive_inf",
                "negative_inf",
            )
        }

        prepared = preprocess_dataframe(raw, require_label=True).cache()
        output_rows = prepared.count()
        removed_rows = input_rows - output_rows
        post_clean_issues = _numeric_issue_profile(prepared, SELECTED_FEATURES)
        null_binary_labels = prepared.filter(F.col(BINARY_LABEL_COLUMN).isNull()).count()

        before_normal = raw.filter(F.upper(F.trim(F.col(LABEL_COLUMN))) == "BENIGN").count()
        before_attack = raw.filter(
            F.upper(F.trim(F.col(LABEL_COLUMN))).isin(sorted(KNOWN_ATTACK_LABELS))
        ).count()
        after_normal, after_attack = _binary_counts(prepared)

        first_vector_row = prepared.select(FEATURES_COLUMN).first()
        dimensions = (
            [int(first_vector_row[FEATURES_COLUMN].size)] if first_vector_row else []
        )

        unlabeled_prepared = preprocess_dataframe(
            raw.select(*SELECTED_FEATURES), require_label=False
        ).cache()
        unlabeled_rows = unlabeled_prepared.count()
        unlabeled_first_vector = unlabeled_prepared.select(FEATURES_COLUMN).first()
        unlabeled_dimension = (
            int(unlabeled_first_vector[FEATURES_COLUMN].size)
            if unlabeled_first_vector
            else 0
        )

        train, validation, test = [
            split.cache() for split in split_dataset(prepared, seed=SPLIT_SEED)
        ]

        print("\n=== PHASE 2 INPUT ===")
        print(f"Input: {input_path.resolve()}")
        print(f"Rows: {input_rows}")
        print(f"Columns: {len(raw.columns)}")
        print("Raw schema: 39 feature strings + 1 Label string")
        print("Final feature datatype: double")

        print("\n=== PRE-CLEAN DATA PROBLEMS ===")
        if numeric_issues:
            for column, counts in numeric_issues.items():
                print(f"{column}: {counts}")
        else:
            print("No numeric issues found")
        print(f"Invalid/null/empty/unexpected label rows: {invalid_label_rows}")
        print(f"Rows affected by selected-feature problem type: {row_issue_counts}")

        print("\n=== CLEANING RESULT ===")
        print(f"Input rows: {input_rows}")
        print(f"Removed rows: {removed_rows} ({removed_rows / input_rows:.4%})")
        print(f"Output rows: {output_rows}")
        print("Policy: drop rows with an invalid selected feature or invalid label")
        print(
            "Shared row-wise output retains duplicates; model examples are "
            "deduplicated before splitting; no imputation or resampling is applied"
        )
        print(f"Selected-feature issues after cleaning: {post_clean_issues or 'none'}")
        print(f"Null binary labels after cleaning: {null_binary_labels}")

        print("\n=== LABEL MAPPING ===")
        print(f"Before: BENIGN={before_normal}, verified attacks={before_attack}")
        print("BENIGN -> 0.0 (Normal); verified attack labels -> 1.0 (Attack)")
        print(f"After: Normal={after_normal}, Attack={after_attack}")

        print("\n=== FEATURE SELECTION ===")
        print(f"Source columns: {len(raw.columns)}")
        print(f"Selected features: {len(SELECTED_FEATURES)}")
        print(", ".join(SELECTED_FEATURES))
        print(f"Excluded feature columns: {len(EXCLUDED_FEATURES)}")
        for column, reason in EXCLUDED_FEATURES.items():
            print(f"  {column}: {reason}")
        print(f"Label excluded from assembler: {LABEL_COLUMN}")
        print("Scaling: not applied; defer to the Phase 3 baseline choice")

        print("\n=== VECTOR ASSEMBLER ===")
        print(f"Assembler inputs: {len(SELECTED_FEATURES)}")
        print(f"Observed vector dimensions: {dimensions}")
        print(f"Post-clean invalid profile: {post_clean_issues or 'none'}")

        print("\n=== SHARED UNLABELED PATH SMOKE CHECK ===")
        print(f"Raw input fields: {len(SELECTED_FEATURES)} selected model features")
        print(f"Rows from input without Label: {unlabeled_rows}")
        print(f"Feature vector dimension: {unlabeled_dimension}")
        print(
            f"Binary label column present: {BINARY_LABEL_COLUMN in unlabeled_prepared.columns}"
        )

        print("\n=== REPRODUCIBLE SPLIT (seed=42, weights=70/15/15) ===")
        _print_split("Train", train)
        _print_split("Validation", validation)
        _print_split("Test", test)

        if selected_issues and removed_rows == 0:
            raise RuntimeError("selected feature issues were found but no rows were removed")
        if post_clean_issues or null_binary_labels:
            raise RuntimeError("preprocessing left invalid ML-ready values")
        if dimensions != [len(SELECTED_FEATURES)]:
            raise RuntimeError("unexpected feature vector dimension")
        if unlabeled_rows != output_rows or unlabeled_dimension != len(SELECTED_FEATURES):
            raise RuntimeError("shared unlabeled preprocessing path failed")
        if BINARY_LABEL_COLUMN in unlabeled_prepared.columns:
            raise RuntimeError("unlabeled preprocessing unexpectedly created a label")
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/sample/ciciot2023_phase1_sample.csv"),
        help="bounded CSV sample to validate",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    validate_preprocessing(args.input)


if __name__ == "__main__":
    main()
