# AGENTS.md

## Project goal

Build a small, understandable, reproducible Big Data pipeline for **real-time IoT network intrusion detection** using CICIoT2023. The project must demonstrate Spark-based offline processing and ML training, Kafka-based dataset replay, Spark Structured Streaming inference, persisted outputs, realtime analytics, and a simple dashboard.

The priority is a working university-level MVP, not an enterprise platform.

## Repository state

### Existing

- `giới thiệu đồ án.docx`: the project brief and original planning reference.
- `MERGED_CSV(1)/MERGED_CSV/`: the current local extraction containing all 63 CICIoT2023 CSV partitions; ignored by Git because the full download is about 8.66 GiB.
- `data/sample/ciciot2023_phase1_sample.csv`: a 30,000-row development sample from three representative partitions.
- `data/README.md`: verified Phase 1 data findings, Phase 2 preprocessing decisions/results, Phase 3A/3B baseline metrics, and finalized Mini Phase 3C weighted-model evidence.
- `spark/eda.py`: bounded sample creation and Spark DataFrame EDA; it does not clean data or train a model.
- `spark/preprocess.py`: the shared Phase 2 casting, cleaning, binary-label, feature assembly, and reproducible leakage-free split logic for labeled and unlabeled DataFrames.
- `spark/verify_preprocessing.py`: focused pre-Phase-3 checks for full-dataset label coverage and sample duplicate/split leakage; it does not train a model.
- `spark/train_model.py`: the shared Random Forest training, evaluation, save, and fresh-process reload entry point. Its opt-in `--balanced-class-weights` path implements the Mini Phase 3C validation gate, and `--accept-weighted-tradeoff` finalizes the user decision only after exact Phase 3C reproduction; unweighted behavior remains the default.
- `models/random_forest_baseline/`: the native Spark MLlib Random Forest artifact trained only on the bounded development sample.
- `models/random_forest_full_baseline/`: the native Spark MLlib Random Forest trained on all 63 local CSV partitions; it is retained as the Phase 3B unweighted reference baseline.
- `models/random_forest_full_weighted/`: the finalized balanced-weight Random Forest. It was selected from the Validation trade-off before Test inspection, then evaluated once on Test, saved, and reloaded successfully. This is the model intended for realtime inference.
- `kafka/producer.py`: the Phase 4 standard-library CSV replay producer for `NORMAL`, `DDOS_BURST`, and deterministic 1:1 `MIXED`; it publishes the exact 27-feature JSON contract without loading Spark or a model.
- `kafka/README.md`: the verified Kafka 4.3.1 KRaft runtime commands, topic/event contract, producer usage, invalid-row policy, and Phase 4 delivery evidence.
- `spark/stream_ingest.py`: the Phase 5 Spark Structured Streaming entry point. It reads the Kafka event contract with an explicit schema and exposes separate `valid_events` and `invalid_events` DataFrames without loading a model or assembling a feature vector.
- `spark/stream_infer.py`: the Phase 6 realtime inference entry point. It reuses Phase 5 ingestion/validation, applies shared unlabeled preprocessing only to valid rows, loads the frozen weighted model once, and writes separate prediction and invalid-event console streams.
- `spark/stream_analytics.py`: the Phase 7-9 analytics entry point. It reuses Phase 6 predictions to expose event-time metrics/status, one alert per predicted Attack, and the existing invalid-event stream. Optional flags independently enable Phase 8 bounded JSON snapshots and Phase 9 durable SQLite history; the original console mode remains the default.
- `dashboard/app.py`: the Phase 8 Streamlit presentation layer. It reads only transient Phase 7 snapshots, refreshes every two seconds, and does not create Spark/Kafka/model objects or recalculate analytics.
- `scripts/inspect_history.py`: the Phase 9 standard-library SQLite inspection CLI. It prints table counts plus recent metric, prediction, and alert rows without starting Spark, Kafka, or the dashboard.
- `data/history/iot_ids_history.sqlite3`: the local ignored Phase 9 durable-history artifact. It is never reset automatically and is not the dashboard handoff.
- `requirements.txt`: verified local PySpark/ML dependencies, `confluent-kafka==2.15.0`, and `streamlit==1.63.0`.
- No committed test suite exists yet; Phase 9 was verified with focused temporary checks and the live end-to-end workflow documented in `README.md` and `TASKS.md`.
- This directory is initialized as a Git repository.

### Completed MVP

- Read and explore CICIoT2023 `MERGED_CSV` with Spark DataFrames.
- Clean data and reuse one preprocessing path for offline training and online inference.
- Train, evaluate, save, and reload one binary Spark MLlib baseline model.
- Replay historical dataset records as JSON events through Kafka.
- Consume events and perform realtime inference with Spark Structured Streaming.
- Calculate basic realtime metrics and alerts, then persist prediction/history output.
- Support `NORMAL`, DDoS burst, and `MIXED` scenarios.
- Provide a small Streamlit dashboard with status, event/attack-rate charts, and an alert feed.
- Document a reproducible end-to-end demo flow.

### Optional after the MVP works

- Multiclass attack classification.
- Full Delta Lake Bronze/Silver/Gold architecture.
- Airflow, Redis, advanced dashboard features, model tuning, and large-scale load testing.
- Kubernetes, cloud deployment, microservices, complex CI/CD, or enterprise observability.

## Core architecture

### Offline flow

```text
CICIoT2023 / MERGED_CSV
-> Spark DataFrame
-> EDA and cleaning
-> shared preprocessing and feature engineering
-> Spark MLlib binary model (Normal vs Attack)
-> evaluation and saved model
```

Evaluate Accuracy, Precision, Recall, F1, and a confusion matrix. Prioritize Attack Recall/F1 over Accuracy alone. Do not invent a target score before measuring the real dataset.

### Online flow

```text
CICIoT2023 historical records
-> Python replay producer
-> Kafka
-> Spark Structured Streaming
-> shared preprocessing
-> saved MLlib model
-> Normal/Attack prediction
-> metrics, alerts, history
-> Streamlit dashboard
```

The producer simulates network events by replaying dataset rows; it does not generate or capture real packets. The final demo sequence is `NORMAL -> DDoS burst -> NORMAL -> MIXED`.

## Component responsibilities

- `MERGED_CSV/` holds the local full dataset and must not be committed.
- `data/sample/` holds the bounded development sample; `data/README.md` records its provenance and Phase 1 findings.
- `spark/eda.py` owns Phase 1 sample creation and read-only Spark EDA.
- `spark/preprocess.py` owns the single reusable preprocessing path. Training and future streaming inference must call it rather than reimplementing casts, filtering, feature selection, or assembly.
- Offline model preparation deduplicates on the selected feature values plus `binary_label` before splitting. Do not apply this batch split policy blindly to the future event stream.
- `kafka/producer.py` owns historical CSV iteration, scenario filtering, replay pacing, JSON serialization, and Kafka delivery. It must not load the model, run Spark, synthesize feature values, or emit predictions.
- `spark/stream_infer.py` owns only online feature preparation, frozen-model inference, binary display-label mapping, and inspectable console output. It must reuse `build_validated_stream(...)` and must not train or select a model.
- `spark/stream_analytics.py` owns only prediction-derived windows, rates, demo status, alerts, and their console/snapshot/history sinks. It must reuse Phase 6 helpers and must not use simulator metadata for operational decisions.

Keep responsibilities separated by capability: dataset replay, shared Spark preprocessing, offline training, streaming inference, persistence/analytics, dashboard, and focused tests. Do not create placeholder directories or speculative layers before a task needs them.

## Technical constraints

- MVP prediction is strictly binary: `Normal` or `Attack`.
- `scenario`, `ground_truth`, and simulator attack labels are evaluation/demo metadata, not model predictions.
- Offline training and streaming inference must use the same preprocessing logic.
- Balanced class weights are training-only. Kafka events and streaming input must not contain `class_weight`; inference still uses the fixed 27-feature contract.
- Never train a model inside the streaming inference job.
- Use one Kafka topic unless a demonstrated requirement needs more.
- The verified Phase 4 topic is `iot-network-events` with one partition and replication factor one. Its events contain only four metadata fields plus the exact 27 numeric model inputs; they do not contain `class_weight`, a feature vector, `prediction`, or `device_id`.
- Phase 4 scenarios are fixed to `NORMAL`, `DDOS_BURST`, and deterministic 1:1 `MIXED`. NORMAL replays only BENIGN rows; DDOS_BURST replays only verified labels beginning with `DDOS-`.
- Phase 5 uses Spark 3.5.7 with `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`. Its valid stream preserves Kafka metadata, parsed simulator metadata, `event_timestamp`, and the 27 doubles. Its invalid stream preserves Kafka metadata, `raw_json`, and the first validation reason.
- Phase 6 calls `preprocess_dataframe(valid_events, require_label=False)` and therefore reuses Phase 2's one 27-feature `VectorAssembler` path. It loads only `models/random_forest_full_weighted/` by default and maps `prediction` as `0.0=Normal`, `1.0=Attack`; `scenario`, `ground_truth`, and `source_label` stay separate simulator metadata. Invalid rows do not reach the model.
- Phase 7 uses `event_timestamp`, a 10-second window, 5-second slide, and 30-second watermark. Its fixed demo-only status thresholds are `<0.20 Normal`, `<0.80 Warning`, and `>=0.80 Critical`; status derives from predicted attack rate and alerts derive only from `prediction == 1.0`.
- The event contract has no `device_id`, so Phase 7 has no device/source distribution and must not invent an equivalent field.
- Do not replace an invalid or missing `event_time` with Kafka's ingestion timestamp. The source event timestamp and `kafka_timestamp` have distinct meanings.
- Use Streamlit as the planned dashboard framework; do not add Plotly Dash alongside it.
- The dashboard should consume outputs through the simplest suitable path. Do not add a REST API, WebSocket service, or database solely for the dashboard.
- Phase 8's optional handoff is `.runtime/dashboard/metrics.json` plus `alerts.json`. It is startup-reset, bounded to 60 metric windows and 100 alerts, atomically replaced, and deliberately not durable Phase 9 persistence.
- The dashboard must treat the latest snapshot `status` as authoritative, show a waiting state before metrics arrive, keep alerts newest first, and identify `scenario`/`source_label` as simulator/evaluation metadata.
- Phase 9's optional durable store defaults to `data/history/iot_ids_history.sqlite3` when its path is supplied. It contains exactly `predictions`, `metrics`, and `alerts`; startup creates missing schema but never resets history.
- Prediction and alert identity is Kafka `(topic, partition, offset)` with idempotent insert. Metric identity is `(window_start, window_end)` with UPSERT because update-mode windows are refined across micro-batches.
- SQLite connections and DataFrame collection stay inside bounded micro-batch callbacks. Do not collect the unbounded streaming DataFrame or add an ORM/Delta layer for this MVP.
- `--history-db` and `--dashboard-output-dir` are independent. Without the former, Phase 7 console and Phase 8 dashboard modes retain their prior behavior. The dashboard continues to read only transient JSON; durable history is inspected with `scripts/inspect_history.py`.
- Full Delta Lake layering remains optional unless explicitly adopted later.
- Develop against a small sample or partition before scaling up.
- Keep `KNOWN_LABELS` aligned with the 34 valid labels verified across all 63 local partitions; null/empty/unknown labels remain invalid rather than defaulting to Attack.
- Do not add commands, ports, environment variables, dependencies, or services to documentation until they really exist and have been verified.

## Current workflow

Verified Phase 1 through Phase 9 commands are documented in `README.md`,
`data/README.md`, and `kafka/README.md`. Phase 3B's
`models/random_forest_full_baseline/` remains the unweighted reference, while
`models/random_forest_full_weighted/` is the frozen inference model. The final
Phase 9 run used Python 3.12.14, Java 17.0.20, Kafka 4.3.1, Spark/PySpark 3.5.7,
Scala 2.12.18, and Streamlit 1.63.0. One Spark process wrote both the unchanged
Phase 8 handoff and SQLite while one browser tab automatically displayed the
4x60-event `NORMAL -> DDOS_BURST -> NORMAL -> MIXED` sequence as
`Normal -> Critical -> Normal -> Warning`. The durable result remained queryable
after shutdown with 280 predictions, 22 metric windows, 110 alerts, and zero
duplicate primary-key identities; 240 predictions and 90 alerts came from the
final sequence. Invalid offset 1239 stayed outside all three tables. The required
MVP is complete; only the optional backlog remains. Do not treat the ignored
Phase 8 JSON handoff as history storage and do not start optional scope without
an explicit request.

Read `RULES.md` before changing the project and use `TASKS.md` as the ordered roadmap.
