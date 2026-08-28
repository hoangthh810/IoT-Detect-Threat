# AGENTS.md

## Project goal

Build a small, understandable, reproducible Big Data pipeline for **real-time IoT network intrusion detection** using CICIoT2023. The project must demonstrate Spark-based offline processing and ML training, Kafka-based dataset replay, Spark Structured Streaming inference, persisted outputs, realtime analytics, and a simple dashboard.

The priority is a working university-level MVP, not an enterprise platform.

## Repository state

### Existing

- `giới thiệu đồ án.docx`: the project brief and original planning reference.
- `MERGED_CSV/MERGED_CSV/`: 63 local CICIoT2023 CSV partitions; ignored by Git because the full download is about 8.66 GiB.
- `data/sample/ciciot2023_phase1_sample.csv`: a 30,000-row development sample from three representative partitions.
- `data/README.md`: verified Phase 1 data findings and Phase 2 preprocessing decisions/results.
- `spark/eda.py`: bounded sample creation and Spark DataFrame EDA; it does not clean data or train a model.
- `spark/preprocess.py`: the shared Phase 2 casting, cleaning, binary-label, feature assembly, and reproducible leakage-free split logic for labeled and unlabeled DataFrames.
- `spark/verify_preprocessing.py`: focused pre-Phase-3 checks for full-dataset label coverage and sample duplicate/split leakage; it does not train a model.
- `requirements.txt`: verified local PySpark/ML runtime dependencies.
- No trained model artifact, Kafka/streaming job, persistence layer, dashboard, or test suite exists yet.
- This directory is not currently initialized as a Git repository.

### Planned MVP

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

As later implementation begins, keep responsibilities separated by capability: dataset replay, shared Spark preprocessing, offline training, streaming inference, persistence/analytics, dashboard, and focused tests. Do not create placeholder directories or speculative layers before a task needs them.

## Technical constraints

- MVP prediction is strictly binary: `Normal` or `Attack`.
- `scenario`, `ground_truth`, and simulator attack labels are evaluation/demo metadata, not model predictions.
- Offline training and streaming inference must use the same preprocessing logic.
- Never train a model inside the streaming inference job.
- Use one Kafka topic unless a demonstrated requirement needs more.
- Use Streamlit as the planned dashboard framework; do not add Plotly Dash alongside it.
- The dashboard should consume outputs through the simplest suitable path. Do not add a REST API, WebSocket service, or database solely for the dashboard.
- Choose the simplest persistence that satisfies history/query needs. Full Delta Lake layering remains optional unless explicitly adopted later.
- Develop against a small sample or partition before scaling up.
- Keep `KNOWN_LABELS` aligned with the 34 valid labels verified across all 63 local partitions; null/empty/unknown labels remain invalid rather than defaulting to Attack.
- Do not add commands, ports, environment variables, dependencies, or services to documentation until they really exist and have been verified.

## Current workflow

Verified Phase 1 and Phase 2 commands are documented in `README.md` and `data/README.md`. Feature/data-quality and duplicate verification use the 30,000-row sample; the focused label-coverage command scans only `Label` across all 63 partitions. There is no verified test, training, streaming, or dashboard command yet; add one only after its implementation exists and has run successfully.

Read `RULES.md` before changing the project and use `TASKS.md` as the ordered roadmap.
