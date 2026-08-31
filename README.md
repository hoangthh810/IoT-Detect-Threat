# Real-Time Big Data Analytics for IoT Network Intrusion Detection

University Big Data project for detecting IoT network attacks with an end-to-end offline and realtime pipeline based on CICIoT2023, Apache Spark, Spark MLlib, Kafka, and a small Streamlit dashboard.

## Current status

**Phase 2 is verified and ready for Phase 3 on the bounded development sample.** A label-only scan of all 63 local partitions verified 34 valid labels after adding the observed `UPLOADING_ATTACK` label. The shared preprocessing module provides explicit casting, invalid-row filtering, binary label mapping, a fixed 27-feature vector, and reproducible leakage-free train/validation/test splits after model-example deduplication. See [data/README.md](./data/README.md) for the evidence.

Model training, Kafka, streaming inference, persistence, and the dashboard have not been implemented.

The existing source brief is [giới thiệu đồ án.docx](<./giới thiệu đồ án.docx>). Where that brief conflicts with the current repository charter, the scope decisions below take precedence.

## MVP goal

Deliver a small, clear, reproducible pipeline that demonstrates:

- Spark-based CICIoT2023 data exploration, cleaning, and feature preparation.
- Offline binary `Normal`/`Attack` training with a simple Spark MLlib model.
- Model evaluation with Accuracy, Precision, Recall, F1, and a confusion matrix.
- Historical dataset replay through a Python Kafka producer.
- Spark Structured Streaming ingestion and realtime inference with the saved model.
- Basic realtime metrics, alerts, and prediction/history persistence.
- `NORMAL`, DDoS burst, and `MIXED` scenarios.
- A simple Streamlit dashboard that communicates system status at a glance.

## Intended architecture

### Offline

```text
CICIoT2023 / MERGED_CSV
-> Spark DataFrame
-> EDA and cleaning
-> shared preprocessing
-> Spark MLlib binary classifier
-> evaluation and saved model
```

### Online

```text
CICIoT2023 record replay
-> Kafka
-> Spark Structured Streaming
-> shared preprocessing
-> saved model inference
-> metrics, alerts, and history
-> Streamlit dashboard
```

The producer is a replay simulator for historical rows, not a real packet generator.

## Prediction contract

The MVP model produces only:

```text
Normal
Attack
```

Fields such as `scenario`, `ground_truth`, or simulator attack type may support demonstrations and evaluation. They must be labeled as simulation/ground-truth metadata and must never be displayed as a multiclass model prediction.

## Scope decisions

- The basic dashboard is required for the MVP; only advanced dashboard features are optional.
- Streamlit is the planned single dashboard framework.
- Multiclass classification is optional and starts only after the binary pipeline is stable.
- Full Delta Lake Bronze/Silver/Gold, Airflow, Redis, Kubernetes, cloud deployment, microservices, and enterprise infrastructure are not current requirements.
- Persistence will use the simplest solution that supports demo history and queries.
- The verified Phase 2 mapping is `BENIGN -> 0.0 (Normal)` and the 33 full-dataset attack labels -> `1.0 (Attack)`; unknown, null, or empty labels are invalid.
- The baseline feature contract contains 27 double-valued inputs. `Label` and all non-selected columns remain outside the `VectorAssembler` input list.
- Shared row-wise preprocessing retains repeated events, while offline model preparation deduplicates the 27 selected features plus `binary_label` before splitting to prevent cross-split leakage.

## Repository guidance

- [AGENTS.md](./AGENTS.md) gives coding agents the current project context and architecture constraints.
- [RULES.md](./RULES.md) defines implementation rules and anti-over-engineering guardrails.
- [TASKS.md](./TASKS.md) is the ordered roadmap and source of Definition of Done criteria.

## Verified Phase 1 workflow

The following commands were verified on Windows from the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
python spark/eda.py sample --source MERGED_CSV/MERGED_CSV/Merged01.csv --source MERGED_CSV/MERGED_CSV/Merged32.csv --source MERGED_CSV/MERGED_CSV/Merged63.csv --output data/sample/ciciot2023_phase1_sample.csv --rows-per-file 10000
.\.venv\Scripts\python.exe spark/eda.py analyze --input data/sample/ciciot2023_phase1_sample.csv
```

The full application still has no run command because later phases are not implemented.

## Verified Phase 2 workflow

Run the shared preprocessing validation on the bounded sample:

```powershell
.\.venv\Scripts\python.exe spark/preprocess.py --input data/sample/ciciot2023_phase1_sample.csv
.\.venv\Scripts\python.exe spark/verify_preprocessing.py labels --input MERGED_CSV/MERGED_CSV
.\.venv\Scripts\python.exe spark/verify_preprocessing.py duplicates --input data/sample/ciciot2023_phase1_sample.csv
```

These commands do not train a classifier. They validate the labeled and unlabeled preprocessing paths, full-dataset label coverage, exact duplicates, and split-fingerprint isolation.

## Next task

Train one simple Spark MLlib Random Forest baseline on the reproducible Phase 2 splits and report Accuracy, Precision, Recall, F1, and a confusion matrix without tuning.

## Project base on CCIOT2023
