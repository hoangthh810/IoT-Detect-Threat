# TASKS.md

This roadmap is ordered by dependency. A checkbox may be marked complete only when the repository contains verifiable implementation or output evidence.

## Next recommended task

Train one simple Spark MLlib Random Forest baseline on the reproducible Phase 2 splits and report Accuracy, Precision, Recall, F1, and a confusion matrix without tuning.

## Phase 1 - Data understanding

**Objective:** Make the dataset available to Spark and establish its real schema and data quality before designing preprocessing.

- [x] Confirm how CICIoT2023 `MERGED_CSV` is made available locally without committing the full dataset unnecessarily.
- [x] Read a manageable sample or partition with a Spark DataFrame.
- [x] Identify the actual label column and observed label values.
- [x] Inspect schema, row counts, class distribution, missing values, invalid values, and positive/negative infinity.
- [x] Identify candidate numeric features and columns that must not be used for training.
- [x] Produce basic reproducible Spark EDA with key summaries.

**Expected Output:** A reproducible Spark EDA entry point and concise findings based on the real dataset.

**Definition of Done:** Spark reads the dataset successfully, and the repository records the schema, label mapping, class distribution, missing/invalid-value findings, and main descriptive statistics.

**Dependencies:** Access to at least one valid CICIoT2023 `MERGED_CSV` file or representative partition.

**Evidence:** `spark/eda.py`, `data/sample/ciciot2023_phase1_sample.csv`, and `data/README.md`. The EDA completed successfully on the 30,000-row sample.

## Phase 2 - Preprocessing

**Objective:** Build one transformation path that can be reused by offline training and online inference.

- [x] Cast input columns to the validated data types.
- [x] Handle null, invalid, NaN, and infinite values consistently.
- [x] Select validated model features and exclude leakage-prone metadata.
- [x] Map the source label to binary `Normal`/`Attack` targets.
- [x] Assemble features with Spark ML `VectorAssembler`.
- [x] Defer scaling until the Phase 3 baseline choice demonstrates a need; no scaler is added in Phase 2.
- [x] Split train, validation, and test data without leakage.
- [x] Package the transformations so training and streaming inference use the same logic.

**Expected Output:** A shared Spark preprocessing pipeline or module plus reproducible data splits.

**Definition of Done:** The same transformation logic can prepare both historical training rows and schema-compatible streaming events.

**Dependencies:** Phase 1 completed with a validated schema, label, and feature set.

**Evidence:** `spark/preprocess.py` completed successfully on the 30,000-row sample. It produced 29,999 cleaned labeled and unlabeled rows and a 27-dimensional feature vector. The pre-Phase-3 check in `spark/verify_preprocessing.py` scanned `Label` across all 63 partitions, verified all 34 valid labels after adding `UPLOADING_ATTACK`, and found 9 null-label rows that remain invalid. On the sample, 771 exact source duplicates produced cross-split leakage under the original policy, so model examples are now deduplicated before the seeded 70/15/15 split. The resulting 28,959 examples split into 20,153/4,364/4,442 rows with zero fingerprint overlap across every split pair; see `data/README.md`.

## Phase 3 - Baseline ML

**Objective:** Train and persist one simple Spark MLlib binary classifier.

- [ ] Train either Logistic Regression or Random Forest as the first baseline.
- [ ] Report Accuracy, Precision, Recall, and F1 for the binary task.
- [ ] Produce and explain a confusion matrix, emphasizing Attack Recall/F1.
- [ ] Save the fitted preprocessing/model pipeline or equivalent reusable artifacts.
- [ ] Reload the saved artifact in a fresh process and run sample predictions.

**Expected Output:** A saved Spark MLlib baseline and an evaluation summary using measured results.

**Definition of Done:** The saved model reloads successfully and predicts only `Normal` or `Attack` on transformed test rows.

**Dependencies:** Phase 2 completed.

## Phase 4 - Kafka producer

**Objective:** Replay historical CICIoT2023 rows as controllable JSON event streams.

- [ ] Define and document the minimal JSON event schema required by streaming inference.
- [ ] Keep model features separate from metadata such as event time, scenario, optional device ID, and optional ground truth.
- [ ] Create or configure one Kafka topic for simulated IoT network events.
- [ ] Replay dataset records continuously at a configurable `events_per_second`.
- [ ] Implement the `NORMAL` scenario.
- [ ] Implement a DDoS burst scenario.
- [ ] Implement the `MIXED` scenario.
- [ ] Add other simulation parameters only when a concrete demo task needs them.

**Expected Output:** A Python dataset-replay producer and documented event contract.

**Definition of Done:** Kafka receives a continuous sequence of valid JSON events at the configured rate for all three MVP scenarios.

**Dependencies:** Phase 1 schema findings and the Phase 2 feature contract; Kafka runtime availability.

## Phase 5 - Spark Structured Streaming

**Objective:** Reliably ingest and validate simulated events from Kafka.

- [ ] Connect Spark Structured Streaming to the configured Kafka topic.
- [ ] Deserialize JSON with an explicit schema.
- [ ] Validate required fields and handle malformed records visibly.
- [ ] Confirm stable continuous ingestion on a small event rate before increasing load.

**Expected Output:** A streaming ingestion job that exposes validated event rows for inference.

**Definition of Done:** Spark continuously reads valid events from Kafka without unhandled errors and makes invalid-record behavior observable.

**Dependencies:** Phase 4 producer/topic and a compatible Spark-Kafka runtime.

## Phase 6 - Realtime inference

**Objective:** Apply the saved offline model to each validated streaming event.

- [ ] Load the saved MLlib model without training inside the streaming job.
- [ ] Apply the shared preprocessing transformations from Phase 2.
- [ ] Produce a binary `Normal`/`Attack` prediction.
- [ ] Preserve a clear separation between prediction and `scenario`/`ground_truth` metadata.
- [ ] Write a simple inspectable prediction output for debugging.

**Expected Output:** A continuous stream of binary predictions with relevant event metadata.

**Definition of Done:** Events replayed through Kafka continuously receive predictions from the reloaded model, with no simulator label presented as model output.

**Dependencies:** Phases 2, 3, and 5 completed.

## Phase 7 - Realtime analytics and alerts

**Objective:** Calculate only the metrics and alerts required to make scenario changes obvious during the demo.

- [ ] Calculate windowed event count or event rate.
- [ ] Calculate predicted Attack count, attack rate, and prediction distribution.
- [ ] Define simple, documented `Normal`/`Warning`/`Critical` status logic using prediction-derived metrics.
- [ ] Produce a basic alert feed with timestamp, prediction, and available event metadata.
- [ ] Count by device/source only if `device_id` exists in the simulated event schema.
- [ ] Expose throughput or processing latency only when Spark provides it without additional infrastructure.

**Expected Output:** Simple realtime metric and alert outputs suitable for persistence and dashboard display.

**Definition of Done:** Metrics and alerts visibly distinguish `NORMAL`, DDoS burst, and `MIXED` runs without treating ground truth as prediction.

**Dependencies:** Phase 6 completed.

## Phase 8 - Dashboard MVP

**Objective:** Let a non-technical viewer understand the system state within a few seconds.

- [ ] Build one small Streamlit dashboard; do not add a second dashboard framework.
- [ ] Show a prominent `Normal`/`Warning`/`Critical` status indicator.
- [ ] Show realtime event-rate and predicted attack-rate trends.
- [ ] Show an auto-updating alert feed.
- [ ] Show scenario or ground-truth attack type only when available and label it explicitly as simulation/evaluation metadata.
- [ ] Show device/source distribution only when valid `device_id` data exists.
- [ ] Use the simplest direct output-reading or refresh mechanism that satisfies the demo.

**Expected Output:** A compact auto-updating Streamlit dashboard backed by the pipeline's metric/alert output.

**Definition of Done:** A viewer can quickly identify normal traffic versus an active attack and see the key supporting indicators without reading console logs or manually refreshing.

**Dependencies:** Phase 7 completed and a simple readable metric/alert output available.

## Phase 9 - Persistence and final demo

**Objective:** Preserve useful history and make the full MVP reproducible for another student.

- [ ] Persist prediction, metric, alert, and required event history using the simplest compatible storage.
- [ ] Use Delta Lake only if it has been explicitly adopted and materially simplifies the current stack; otherwise avoid full Bronze/Silver/Gold layering.
- [ ] Verify that saved history can be queried or read after streaming stops.
- [ ] Document real installation and run commands only after verifying them.
- [ ] Prepare the demo sequence `NORMAL -> DDoS burst -> NORMAL -> MIXED`.
- [ ] Summarize measured ML metrics and available throughput/latency results.
- [ ] Test the documented workflow from a clean or equivalent environment.

**Expected Output:** Queryable history, verified run instructions, and a repeatable end-to-end demo flow.

**Definition of Done:** Another student can follow `README.md`, run the main pipeline, observe the dashboard through the full scenario sequence, and inspect saved history.

**Dependencies:** Phases 1-8 completed.

## Optional backlog - do not start before the MVP is stable

- [ ] Multiclass attack classification.
- [ ] Full Delta Lake Bronze/Silver/Gold architecture.
- [ ] Airflow retraining or reporting workflows.
- [ ] Redis alert caching.
- [ ] Advanced dashboard drill-down, complex filters, or report export.
- [ ] Advanced model comparison or hyperparameter tuning.
- [ ] Large-scale load testing.
- [ ] Cloud deployment, Kubernetes, or microservices.
- [ ] Complex CI/CD or enterprise observability.
