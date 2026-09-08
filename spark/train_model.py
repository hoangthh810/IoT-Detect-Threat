"""Train/compare and verify Random Forest models on CICIoT2023 CSV input.

The training command reuses the Phase 2 preprocessing and leakage-free split
policy. Balanced class weighting is an opt-in, Validation-gated experiment;
the original baseline behavior remains the default. The verify-load command is
intentionally separate so reload and inference run in a fresh Spark process.
"""

from __future__ import annotations

import argparse
import math
import time
from pathlib import Path

from pyspark.ml.classification import (
    RandomForestClassificationModel,
    RandomForestClassifier,
)
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.storagelevel import StorageLevel

from preprocess import (
    BINARY_LABEL_COLUMN,
    FEATURES_COLUMN,
    SELECTED_FEATURES,
    SPLIT_SEED,
    deduplicate_model_examples,
    preprocess_dataframe,
    read_raw_csv,
    split_dataset,
)


DEFAULT_SAMPLE_PATH = Path("data/sample/ciciot2023_phase1_sample.csv")
DEFAULT_MODEL_PATH = Path("models/random_forest_baseline")
DEFAULT_FULL_BASELINE_MODEL_PATH = Path("models/random_forest_full_baseline")
NORMAL_LABEL = 0.0
ATTACK_LABEL = 1.0
CLASS_WEIGHT_COLUMN = "class_weight"

WEIGHTED_SELECTED = "WEIGHTED MODEL IS A CLEAR IMPROVEMENT"
BASELINE_RETAINED = "BASELINE REMAINS BETTER"
TRADE_OFF = "MODEL TRADE-OFF — DECISION REQUIRED"

EXPECTED_FINALIZATION_COUNTS = {
    "raw_rows": 45_019_243,
    "prepared_rows": 45_018_243,
    "deduplicated_rows": 20_362_785,
    "train_rows": 14_253_082,
    "validation_rows": 3_053_542,
    "test_rows": 3_056_161,
    "train_normal": 733_138,
    "train_attack": 13_519_944,
}
EXPECTED_NORMAL_WEIGHT = 9.720599668821
EXPECTED_ATTACK_WEIGHT = 0.527113204019
EXPECTED_VALIDATION_METRICS = {
    "baseline_normal_specificity": 0.745212,
    "baseline_attack_recall": 0.987539,
    "weighted_normal_specificity": 0.999045,
    "weighted_attack_recall": 0.951662,
}
REPRODUCIBILITY_TOLERANCE = 0.000001

PHASE_3B_BASELINE_TEST_METRICS: dict[str, float | int] = {
    "accuracy": 0.975085,
    "attack_precision": 0.986085,
    "attack_recall": 0.987672,
    "attack_f1": 0.986878,
    "normal_specificity": 0.742810,
    "false_positive_rate": 0.257190,
    "balanced_accuracy": (0.987672 + 0.742810) / 2,
    "tn": 116_694,
    "fp": 40_404,
    "fn": 35_741,
    "tp": 2_863_322,
}


def create_spark(
    app_name: str,
    *,
    master: str = "local[*]",
    driver_memory: str | None = None,
    local_dir: Path | None = None,
) -> SparkSession:
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
    )
    if driver_memory:
        builder = builder.config("spark.driver.memory", driver_memory)
    if local_dir:
        local_dir.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", str(local_dir.resolve()))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def csv_files_for_input(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        csv_files = sorted(path for path in input_path.glob("*.csv") if path.is_file())
        if csv_files:
            return csv_files
        raise ValueError(f"no CSV files found in input directory: {input_path}")
    raise FileNotFoundError(f"CSV input not found: {input_path}")


def class_distribution(dataframe: DataFrame, split_name: str) -> dict[str, float | int]:
    counts = {
        float(row[BINARY_LABEL_COLUMN]): int(row["count"])
        for row in dataframe.groupBy(BINARY_LABEL_COLUMN).count().collect()
    }
    unexpected_labels = sorted(set(counts) - {NORMAL_LABEL, ATTACK_LABEL})
    if unexpected_labels:
        raise RuntimeError(
            f"{split_name} contains unexpected binary labels: {unexpected_labels}"
        )

    normal = counts.get(NORMAL_LABEL, 0)
    attack = counts.get(ATTACK_LABEL, 0)
    rows = normal + attack
    if rows == 0:
        raise RuntimeError(f"{split_name} is empty")
    if normal == 0 or attack == 0:
        raise RuntimeError(
            f"{split_name} must contain both classes before training: "
            f"Normal={normal}, Attack={attack}"
        )

    return {
        "rows": rows,
        "normal": normal,
        "attack": attack,
        "normal_share": normal / rows,
        "attack_share": attack / rows,
    }


def print_distribution(split_name: str, distribution: dict[str, float | int]) -> None:
    print(f"\n{split_name}:")
    print(f"rows: {distribution['rows']}")
    print(f"Normal: {distribution['normal']}")
    print(f"Attack: {distribution['attack']}")
    print(f"Normal %: {distribution['normal_share']:.4%}")
    print(f"Attack %: {distribution['attack_share']:.4%}")


def evaluate(predictions: DataFrame) -> dict[str, float | int]:
    cells = {
        (float(row[BINARY_LABEL_COLUMN]), float(row["prediction"])): int(
            row["count"]
        )
        for row in predictions.groupBy(BINARY_LABEL_COLUMN, "prediction")
        .count()
        .collect()
    }
    expected_values = {NORMAL_LABEL, ATTACK_LABEL}
    unexpected_cells = [
        cell
        for cell in cells
        if cell[0] not in expected_values or cell[1] not in expected_values
    ]
    if unexpected_cells:
        raise RuntimeError(f"predictions contain non-binary values: {unexpected_cells}")

    tn = cells.get((NORMAL_LABEL, NORMAL_LABEL), 0)
    fp = cells.get((NORMAL_LABEL, ATTACK_LABEL), 0)
    fn = cells.get((ATTACK_LABEL, NORMAL_LABEL), 0)
    tp = cells.get((ATTACK_LABEL, ATTACK_LABEL), 0)
    total = tn + fp + fn + tp
    if total == 0:
        raise RuntimeError("cannot evaluate an empty prediction DataFrame")

    precision_denominator = tp + fp
    recall_denominator = tp + fn
    normal_denominator = tn + fp
    attack_precision = tp / precision_denominator if precision_denominator else 0.0
    attack_recall = tp / recall_denominator if recall_denominator else 0.0
    normal_specificity = tn / normal_denominator if normal_denominator else 0.0
    false_positive_rate = fp / normal_denominator if normal_denominator else 0.0
    f1_denominator = attack_precision + attack_recall
    attack_f1 = (
        2 * attack_precision * attack_recall / f1_denominator
        if f1_denominator
        else 0.0
    )
    balanced_accuracy = (attack_recall + normal_specificity) / 2

    return {
        "accuracy": (tn + tp) / total,
        "attack_precision": attack_precision,
        "attack_recall": attack_recall,
        "attack_f1": attack_f1,
        "normal_specificity": normal_specificity,
        "false_positive_rate": false_positive_rate,
        "balanced_accuracy": balanced_accuracy,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "tp": tp,
    }


def print_metrics(dataset_name: str, metrics: dict[str, float | int]) -> None:
    print(f"\n=== {dataset_name.upper()} METRICS ===")
    print(f"Accuracy: {metrics['accuracy']:.6f}")
    print(f"Attack Precision: {metrics['attack_precision']:.6f}")
    print(f"Attack Recall: {metrics['attack_recall']:.6f}")
    print(f"Attack F1: {metrics['attack_f1']:.6f}")
    print(f"Normal Specificity: {metrics['normal_specificity']:.6f}")
    print(f"False Positive Rate: {metrics['false_positive_rate']:.6f}")
    print(f"Balanced Accuracy: {metrics['balanced_accuracy']:.6f}")
    print("Confusion matrix (Attack=positive class):")
    print("                 Predicted Normal    Predicted Attack")
    print(f"Actual Normal    {metrics['tn']:>16}    {metrics['fp']:>16}")
    print(f"Actual Attack    {metrics['fn']:>16}    {metrics['tp']:>16}")
    print(f"TN: {metrics['tn']}")
    print(f"FP: {metrics['fp']} (Normal reported as Attack)")
    print(f"FN: {metrics['fn']} (Attack missed as Normal)")
    print(f"TP: {metrics['tp']}")


def print_model_configuration(
    classifier: RandomForestClassifier, *, balanced_class_weights: bool
) -> None:
    print("\n=== MODEL CONFIGURATION ===")
    print("Algorithm: RandomForestClassifier")
    print(f"labelCol: {classifier.getLabelCol()}")
    print(f"featuresCol: {classifier.getFeaturesCol()}")
    print(f"seed: {classifier.getSeed()}")
    print(f"numTrees: {classifier.getNumTrees()} (default)")
    print(f"maxDepth: {classifier.getMaxDepth()} (default)")
    print(f"maxBins: {classifier.getMaxBins()} (default)")
    print(
        "featureSubsetStrategy: "
        f"{classifier.getFeatureSubsetStrategy()} (default)"
    )
    if balanced_class_weights:
        print(f"weightCol: {classifier.getWeightCol()}")
        print("Only experimental change: weightCol uses Train-only balanced weights")
        print("All other baseline parameters: unchanged from Phase 3A/3B")
    else:
        print("Other non-default parameters: none")
        print("Baseline configuration: unchanged from Phase 3A")


def print_metric_comparison(
    dataset_name: str,
    baseline_metrics: dict[str, float | int],
    weighted_metrics: dict[str, float | int],
) -> None:
    metric_labels = (
        ("Accuracy", "accuracy"),
        ("Attack Precision", "attack_precision"),
        ("Attack Recall", "attack_recall"),
        ("Attack F1", "attack_f1"),
        ("Normal Specificity", "normal_specificity"),
        ("False Positive Rate", "false_positive_rate"),
        ("Balanced Accuracy", "balanced_accuracy"),
    )
    print(f"\n=== {dataset_name.upper()} COMPARISON (WEIGHTED - BASELINE) ===")
    print(f"{'Metric':<24} {'Baseline':>12} {'Weighted':>12} {'Delta':>12}")
    for label, key in metric_labels:
        baseline_value = float(baseline_metrics[key])
        weighted_value = float(weighted_metrics[key])
        print(
            f"{label:<24} {baseline_value:>12.6f} "
            f"{weighted_value:>12.6f} "
            f"{weighted_value - baseline_value:>+12.6f}"
        )


def weighted_validation_decision(
    baseline_metrics: dict[str, float | int],
    weighted_metrics: dict[str, float | int],
) -> str:
    baseline_specificity = float(baseline_metrics["normal_specificity"])
    weighted_specificity = float(weighted_metrics["normal_specificity"])
    baseline_recall = float(baseline_metrics["attack_recall"])
    weighted_recall = float(weighted_metrics["attack_recall"])

    if (
        weighted_specificity > baseline_specificity
        and weighted_recall >= baseline_recall
    ):
        return WEIGHTED_SELECTED
    if (
        weighted_specificity <= baseline_specificity
        and weighted_recall <= baseline_recall
    ):
        return BASELINE_RETAINED
    return TRADE_OFF


def require_finalization_reproducibility(
    *,
    raw_rows: int,
    prepared_rows: int,
    deduplicated_rows: int,
    distributions: dict[str, dict[str, float | int]],
    normal_weight: float,
    attack_weight: float,
) -> None:
    actual_counts = {
        "raw_rows": raw_rows,
        "prepared_rows": prepared_rows,
        "deduplicated_rows": deduplicated_rows,
        "train_rows": int(distributions["Train"]["rows"]),
        "validation_rows": int(distributions["Validation"]["rows"]),
        "test_rows": int(distributions["Test"]["rows"]),
        "train_normal": int(distributions["Train"]["normal"]),
        "train_attack": int(distributions["Train"]["attack"]),
    }
    count_mismatches = {
        key: (EXPECTED_FINALIZATION_COUNTS[key], actual)
        for key, actual in actual_counts.items()
        if actual != EXPECTED_FINALIZATION_COUNTS[key]
    }
    weights_match = math.isclose(
        normal_weight,
        EXPECTED_NORMAL_WEIGHT,
        rel_tol=0.0,
        abs_tol=REPRODUCIBILITY_TOLERANCE,
    ) and math.isclose(
        attack_weight,
        EXPECTED_ATTACK_WEIGHT,
        rel_tol=0.0,
        abs_tol=REPRODUCIBILITY_TOLERANCE,
    )
    if count_mismatches or not weights_match:
        raise RuntimeError(
            "Mini Phase 3C finalization reproducibility check failed; "
            "Test will not be evaluated. "
            f"count mismatches={count_mismatches or 'none'}, "
            f"expected weights=({EXPECTED_NORMAL_WEIGHT:.12f}, "
            f"{EXPECTED_ATTACK_WEIGHT:.12f}), actual weights="
            f"({normal_weight:.12f}, {attack_weight:.12f})"
        )

    print("\n=== FINALIZATION REPRODUCIBILITY CHECK ===")
    print("Full population and split counts match Mini Phase 3C: YES")
    print("Train-only class weights match Mini Phase 3C: YES")


def require_finalization_validation_reproduction(
    baseline_metrics: dict[str, float | int],
    weighted_metrics: dict[str, float | int],
    decision: str,
) -> None:
    actual_metrics = {
        "baseline_normal_specificity": float(
            baseline_metrics["normal_specificity"]
        ),
        "baseline_attack_recall": float(baseline_metrics["attack_recall"]),
        "weighted_normal_specificity": float(
            weighted_metrics["normal_specificity"]
        ),
        "weighted_attack_recall": float(weighted_metrics["attack_recall"]),
    }
    metric_mismatches = {
        key: (EXPECTED_VALIDATION_METRICS[key], actual)
        for key, actual in actual_metrics.items()
        if not math.isclose(
            actual,
            EXPECTED_VALIDATION_METRICS[key],
            rel_tol=0.0,
            abs_tol=REPRODUCIBILITY_TOLERANCE,
        )
    }
    if decision != TRADE_OFF or metric_mismatches:
        raise RuntimeError(
            "Mini Phase 3C Validation trade-off did not reproduce; "
            "Test will not be evaluated. "
            f"decision={decision}, metric mismatches="
            f"{metric_mismatches or 'none'}"
        )
    print("Validation trade-off matches Mini Phase 3C: YES")


def print_feature_importances(model: RandomForestClassificationModel) -> None:
    ranked = sorted(
        zip(SELECTED_FEATURES, model.featureImportances.toArray()),
        key=lambda item: (-float(item[1]), item[0]),
    )
    print("\n=== TOP 10 FEATURE IMPORTANCES ===")
    for rank, (feature, importance) in enumerate(ranked[:10], start=1):
        print(f"{rank}. {feature}: {float(importance):.8f}")


def train_baseline(
    input_path: Path,
    model_path: Path,
    *,
    master: str,
    driver_memory: str | None,
    local_dir: Path | None,
    balanced_class_weights: bool,
    accept_weighted_tradeoff: bool,
    baseline_model_path: Path,
) -> None:
    csv_files = csv_files_for_input(input_path)
    if model_path.exists():
        raise FileExistsError(
            f"model output already exists: {model_path}. "
            "Choose a different --model-output or remove the old artifact explicitly."
        )
    if balanced_class_weights and not baseline_model_path.is_dir():
        raise FileNotFoundError(
            f"baseline model not found: {baseline_model_path}. "
            "The weighted experiment must compare against an existing baseline."
        )

    full_dataset_input = input_path.is_dir()
    total_started = time.perf_counter()
    spark = create_spark(
        (
            "CICIoT2023MiniPhase3CWeightedRandomForest"
            if balanced_class_weights
            else "CICIoT2023Phase3BFullRandomForest"
        )
        if full_dataset_input
        else (
            "CICIoT2023MiniPhase3CWeightedSmoke"
            if balanced_class_weights
            else "CICIoT2023Phase3ARandomForest"
        ),
        master=master,
        driver_memory=driver_memory,
        local_dir=local_dir,
    )
    model_examples: DataFrame | None = None
    try:
        dataset_size = sum(path.stat().st_size for path in csv_files)
        print("\n=== SPARK LOCAL RESOURCES ===")
        print(f"Master: {spark.sparkContext.master}")
        print(
            "Driver memory: "
            f"{spark.sparkContext.getConf().get('spark.driver.memory', 'Spark default')}"
        )
        print(
            "Local shuffle/spill directory: "
            f"{spark.sparkContext.getConf().get('spark.local.dir', 'Spark default')}"
        )
        print("\nCounting raw CSV rows...", flush=True)
        raw = read_raw_csv(spark, input_path)
        raw_rows = raw.count()

        print("Applying shared preprocessing and counting ML-ready rows...", flush=True)
        prepared = preprocess_dataframe(raw, require_label=True).select(
            *SELECTED_FEATURES,
            BINARY_LABEL_COLUMN,
            FEATURES_COLUMN,
        )
        prepared_rows = prepared.count()
        first_feature_row = prepared.select(FEATURES_COLUMN).first()
        if first_feature_row is None:
            raise RuntimeError("shared preprocessing produced no ML-ready rows")
        feature_dimension = int(first_feature_row[FEATURES_COLUMN].size)
        if feature_dimension != len(SELECTED_FEATURES):
            raise RuntimeError(
                f"expected {len(SELECTED_FEATURES)} features, got {feature_dimension}"
            )

        print(
            "Deduplicating model examples and persisting only the deduplicated "
            "training population...",
            flush=True,
        )
        model_examples = (
            deduplicate_model_examples(prepared)
            .select(BINARY_LABEL_COLUMN, FEATURES_COLUMN)
            .persist(StorageLevel.DISK_ONLY)
        )
        model_distribution = class_distribution(
            model_examples, "Model examples after cleaning and deduplication"
        )
        deduplicated_rows = int(model_distribution["rows"])

        train, validation, test = split_dataset(
            model_examples,
            seed=SPLIT_SEED,
            model_examples_are_deduplicated=True,
        )
        print("Computing reproducible split distributions...", flush=True)
        distributions = {
            "Train": class_distribution(train, "Train"),
            "Validation": class_distribution(validation, "Validation"),
            "Test": class_distribution(test, "Test"),
        }
        split_rows = sum(
            int(distribution["rows"]) for distribution in distributions.values()
        )
        if split_rows != deduplicated_rows:
            raise RuntimeError(
                "split row counts do not cover the deduplicated model examples: "
                f"splits={split_rows}, deduplicated={deduplicated_rows}"
            )

        cleaned_rows = raw_rows - prepared_rows
        deduplicated_removed_rows = prepared_rows - deduplicated_rows

        print("\n=== TRAINING INPUT ===")
        print(f"Input: {input_path.resolve()}")
        print(f"CSV partitions: {len(csv_files)}")
        print(f"Dataset size: {dataset_size} bytes")
        print(f"Raw rows: {raw_rows}")
        print(f"ML-ready rows: {prepared_rows}")
        print(
            f"Rows removed during cleaning: {cleaned_rows} "
            f"({cleaned_rows / raw_rows:.6%})"
        )
        print(f"Rows before model-example deduplication: {prepared_rows}")
        print(f"Rows after model-example deduplication: {deduplicated_rows}")
        print(
            f"Rows removed by model-example deduplication: "
            f"{deduplicated_removed_rows} "
            f"({deduplicated_removed_rows / prepared_rows:.6%})"
        )
        print(f"Feature count: {len(SELECTED_FEATURES)}")
        print(f"Feature vector dimension: {feature_dimension}")

        print("\n=== MODEL-EXAMPLE CLASS DISTRIBUTION ===")
        print_distribution("After cleaning and deduplication", model_distribution)
        print(
            "Leakage prevention: model-example deduplication performed before split"
        )

        print("\n=== SPLIT DISTRIBUTION ===")
        for split_name, distribution in distributions.items():
            print_distribution(split_name, distribution)

        baseline_validation_metrics: dict[str, float | int] | None = None
        if balanced_class_weights:
            print(
                "\nLoading the existing baseline before candidate training...",
                flush=True,
            )
            baseline_model = RandomForestClassificationModel.load(
                str(baseline_model_path.resolve())
            )
            print(
                "Evaluating existing baseline on the same Validation split...",
                flush=True,
            )
            baseline_validation_metrics = evaluate(
                baseline_model.transform(validation)
            )
            print_metrics("Baseline Validation", baseline_validation_metrics)

            train_rows = int(distributions["Train"]["rows"])
            train_normal = int(distributions["Train"]["normal"])
            train_attack = int(distributions["Train"]["attack"])
            normal_weight = train_rows / (2 * train_normal)
            attack_weight = train_rows / (2 * train_attack)
            print("\n=== TRAIN-ONLY BALANCED CLASS WEIGHTS ===")
            print(f"N_train: {train_rows}")
            print(f"N_normal_train: {train_normal}")
            print(f"N_attack_train: {train_attack}")
            print("Formula: weight_class = N_train / (2 * N_class_train)")
            print(f"Normal weight: {normal_weight:.12f}")
            print(f"Attack weight: {attack_weight:.12f}")
            print("Rows added or removed by weighting: 0")
            print("Weight column scope: Train only; not part of the feature vector")
            if accept_weighted_tradeoff:
                require_finalization_reproducibility(
                    raw_rows=raw_rows,
                    prepared_rows=prepared_rows,
                    deduplicated_rows=deduplicated_rows,
                    distributions=distributions,
                    normal_weight=normal_weight,
                    attack_weight=attack_weight,
                )
            training_frame = train.withColumn(
                CLASS_WEIGHT_COLUMN,
                F.when(
                    F.col(BINARY_LABEL_COLUMN) == F.lit(NORMAL_LABEL),
                    F.lit(normal_weight),
                ).otherwise(F.lit(attack_weight)),
            )
            classifier = RandomForestClassifier(
                labelCol=BINARY_LABEL_COLUMN,
                featuresCol=FEATURES_COLUMN,
                seed=SPLIT_SEED,
                weightCol=CLASS_WEIGHT_COLUMN,
            )
        else:
            training_frame = train
            classifier = RandomForestClassifier(
                labelCol=BINARY_LABEL_COLUMN,
                featuresCol=FEATURES_COLUMN,
                seed=SPLIT_SEED,
            )
        print_model_configuration(
            classifier, balanced_class_weights=balanced_class_weights
        )

        print("\nFitting Random Forest on Train only...", flush=True)
        fit_started = time.perf_counter()
        model = classifier.fit(training_frame)
        fit_seconds = time.perf_counter() - fit_started

        print(
            "Evaluating weighted Validation..."
            if balanced_class_weights
            else "Evaluating Validation...",
            flush=True,
        )
        validation_predictions = model.transform(validation)
        validation_metrics = evaluate(validation_predictions)
        print_metrics(
            "Weighted Validation" if balanced_class_weights else "Validation",
            validation_metrics,
        )

        save_model = True
        if balanced_class_weights:
            if baseline_validation_metrics is None:
                raise RuntimeError("baseline Validation metrics were not computed")
            print_metric_comparison(
                "Validation", baseline_validation_metrics, validation_metrics
            )
            decision = weighted_validation_decision(
                baseline_validation_metrics, validation_metrics
            )
            print("\n=== VALIDATION DECISION ===")
            print(decision)
            if accept_weighted_tradeoff:
                require_finalization_validation_reproduction(
                    baseline_validation_metrics,
                    validation_metrics,
                    decision,
                )
                print("\n=== USER DECISION ===")
                print("WEIGHTED TRADE-OFF ACCEPTED BEFORE TEST")
                print("WEIGHTED MODEL WAS SELECTED BEFORE TEST INSPECTION.")
                print("TEST IS USED ONLY FOR FINAL UNBIASED EVALUATION.")
                save_model = True
            else:
                save_model = decision == WEIGHTED_SELECTED
            if save_model:
                print("Validation gate passed: Test evaluation is now unlocked.")
                print("Evaluating weighted candidate on Test...", flush=True)
                test_metrics = evaluate(model.transform(test))
                print_metrics("Weighted Candidate Test", test_metrics)
                print(
                    "Attack Miss Rate: "
                    f"{1.0 - float(test_metrics['attack_recall']):.6f}"
                )
                print(
                    "Normal False-Positive Rate: "
                    f"{float(test_metrics['false_positive_rate']):.6f}"
                )
                if accept_weighted_tradeoff:
                    baseline_test_metrics = PHASE_3B_BASELINE_TEST_METRICS
                    baseline_test_source = "verified Phase 3B metrics"
                else:
                    print(
                        "Evaluating baseline on Test after the Validation "
                        "decision...",
                        flush=True,
                    )
                    baseline_test_metrics = evaluate(
                        baseline_model.transform(test)
                    )
                    print_metrics("Baseline Test", baseline_test_metrics)
                    baseline_test_source = "same-run baseline evaluation"
                print_metric_comparison(
                    "Test", baseline_test_metrics, test_metrics
                )
                print(f"Baseline Test comparison source: {baseline_test_source}")
                print("MODEL SELECTION WAS FROZEN BEFORE TEST.")
            else:
                print("Test evaluation: NOT RUN (blocked by Validation gate)")
                print("Candidate promotion: NO")
                print("Candidate artifact save: NO")
        else:
            print("Evaluating Test...", flush=True)
            test_predictions = model.transform(test)
            test_metrics = evaluate(test_predictions)
            print_metrics(
                "Final full-dataset Test"
                if full_dataset_input
                else "Development-sample Test",
                test_metrics,
            )

        print_feature_importances(model)

        model_examples.unpersist()
        model_examples = None

        if save_model:
            model.save(str(model_path.resolve()))
            print("\n=== MODEL SAVE ===")
            print(f"Model saved to: {model_path.resolve()}")
            print(
                "Run the separate verify-load command in a fresh process to "
                "verify reload."
            )
        print("\n=== RUNTIME ===")
        print(f"Random Forest fit runtime: {fit_seconds:.1f} seconds")
        print(
            "End-to-end training command runtime: "
            f"{time.perf_counter() - total_started:.1f} seconds"
        )
    finally:
        if model_examples is not None:
            model_examples.unpersist()
        spark.stop()


def verify_loaded_model(input_path: Path, model_path: Path, rows: int) -> None:
    csv_files_for_input(input_path)
    if not model_path.is_dir():
        raise FileNotFoundError(f"saved model not found: {model_path}")
    if rows < 1:
        raise ValueError("--rows must be at least 1")

    spark = create_spark("CICIoT2023VerifyLoadedRandomForest")
    try:
        loaded_model = RandomForestClassificationModel.load(
            str(model_path.resolve())
        )
        raw_subset = read_raw_csv(spark, input_path).limit(max(100, rows * 10))
        prepared_subset = preprocess_dataframe(
            raw_subset, require_label=True
        ).limit(rows)
        predictions = (
            loaded_model.transform(prepared_subset)
            .select(BINARY_LABEL_COLUMN, "prediction", "probability")
            .collect()
        )
        if len(predictions) < rows:
            raise RuntimeError(
                f"requested {rows} sample predictions but only produced "
                f"{len(predictions)}"
            )

        print("\n=== SAVE / RELOAD VERIFICATION ===")
        print(f"Model loaded from: {model_path.resolve()}")
        print("Reload successful: YES")
        print("Prediction contract: 0.0=Normal, 1.0=Attack")
        print("Sample predictions from the loaded model:")
        for index, row in enumerate(predictions, start=1):
            actual = float(row[BINARY_LABEL_COLUMN])
            prediction = float(row["prediction"])
            if actual not in {NORMAL_LABEL, ATTACK_LABEL}:
                raise RuntimeError(f"unexpected actual binary label: {actual}")
            if prediction not in {NORMAL_LABEL, ATTACK_LABEL}:
                raise RuntimeError(f"unexpected binary prediction: {prediction}")
            actual_name = "Attack" if actual == ATTACK_LABEL else "Normal"
            prediction_name = (
                "Attack" if prediction == ATTACK_LABEL else "Normal"
            )
            probability = [float(value) for value in row["probability"]]
            print(
                f"{index}. binary_label={actual:.1f} ({actual_name}), "
                f"prediction={prediction:.1f} ({prediction_name}), "
                f"probability=[Normal={probability[0]:.6f}, "
                f"Attack={probability[1]:.6f}]"
            )
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    train_parser = subparsers.add_parser(
        "train", help="train, evaluate, and save one Random Forest baseline"
    )
    train_parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_SAMPLE_PATH,
        help="a CICIoT2023 CSV file or directory containing CSV partitions",
    )
    train_parser.add_argument(
        "--model-output",
        type=Path,
        default=DEFAULT_MODEL_PATH,
        help="new directory for the native Spark MLlib model",
    )
    train_parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master used for the local training run",
    )
    train_parser.add_argument(
        "--driver-memory",
        default=None,
        help="optional Spark driver heap, for example 6g",
    )
    train_parser.add_argument(
        "--spark-local-dir",
        type=Path,
        default=None,
        help="optional local directory for Spark shuffle and spill files",
    )
    train_parser.add_argument(
        "--balanced-class-weights",
        action="store_true",
        help=(
            "run the single Mini Phase 3C balanced-weight candidate and apply "
            "the Validation gate before Test/save"
        ),
    )
    train_parser.add_argument(
        "--baseline-model",
        type=Path,
        default=DEFAULT_FULL_BASELINE_MODEL_PATH,
        help="existing baseline model used for same-Validation comparison",
    )
    train_parser.add_argument(
        "--accept-weighted-tradeoff",
        action="store_true",
        help=(
            "finalize the weighted model selected before Test; requires "
            "--balanced-class-weights and exact Phase 3C reproduction"
        ),
    )

    verify_parser = subparsers.add_parser(
        "verify-load", help="load the saved model and run sample predictions"
    )
    verify_parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_SAMPLE_PATH,
        help="a CICIoT2023 CSV file or directory containing CSV partitions",
    )
    verify_parser.add_argument(
        "--model",
        type=Path,
        default=DEFAULT_MODEL_PATH,
        help="saved native Spark MLlib model directory",
    )
    verify_parser.add_argument(
        "--rows",
        type=int,
        default=5,
        help="number of sample predictions to display",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "train":
        if args.accept_weighted_tradeoff and not args.balanced_class_weights:
            parser.error(
                "--accept-weighted-tradeoff requires "
                "--balanced-class-weights"
            )
        train_baseline(
            args.input,
            args.model_output,
            master=args.master,
            driver_memory=args.driver_memory,
            local_dir=args.spark_local_dir,
            balanced_class_weights=args.balanced_class_weights,
            accept_weighted_tradeoff=args.accept_weighted_tradeoff,
            baseline_model_path=args.baseline_model,
        )
    elif args.command == "verify-load":
        verify_loaded_model(args.input, args.model, args.rows)


if __name__ == "__main__":
    main()
