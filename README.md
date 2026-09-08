# Real-Time Big Data Analytics for IoT Network Intrusion Detection

University Big Data project for detecting IoT network attacks with an end-to-end offline and realtime pipeline based on CICIoT2023, Apache Spark, Spark MLlib, Kafka, and a small Streamlit dashboard.

## Current status

**Phase 9 and the university MVP are complete.** The project now runs the full
dataset-replay path from Kafka through Spark validation, frozen-model inference,
realtime analytics, durable SQLite history, and the auto-updating Streamlit
dashboard.

`data/history/iot_ids_history.sqlite3` is the durable prediction, metric, and
alert store. The Phase 8 `.runtime/dashboard/` JSON handoff remains intentionally
bounded, startup-reset, local, and transient; the dashboard still reads only
that handoff rather than querying the database.

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
-> prediction-derived metrics and alerts
-> SQLite durable history
-> bounded JSON snapshots
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
- Persistence uses standard-library SQLite, the simplest solution that supports
  the demonstrated local history and query needs.
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

The complete final run sequence is documented under **Verified Phase 9 workflow**.

## Verified Phase 2 workflow

Run the shared preprocessing validation on the bounded sample:

```powershell
.\.venv\Scripts\python.exe spark/preprocess.py --input data/sample/ciciot2023_phase1_sample.csv
.\.venv\Scripts\python.exe spark/verify_preprocessing.py labels --input MERGED_CSV/MERGED_CSV
.\.venv\Scripts\python.exe spark/verify_preprocessing.py duplicates --input data/sample/ciciot2023_phase1_sample.csv
```

These commands do not train a classifier. They validate the labeled and unlabeled preprocessing paths, full-dataset label coverage, exact duplicates, and split-fingerprint isolation.

## Verified Phase 3A workflow

<<<<<<< HEAD
Train one simple Spark MLlib Random Forest baseline on the reproducible Phase 2 splits and report Accuracy, Precision, Recall, F1, and a confusion matrix without tuning.

## Project base on CCIOT2023
=======
With the dependency environment activated, train one Random Forest on the bounded development sample:

```text
python spark/train_model.py train
```

The command refuses to overwrite an existing model directory. Use a new `--model-output` path for an intentional rerun when `models/random_forest_baseline/` already exists.

In a separate process, reload the saved model and show five predictions:

```text
python spark/train_model.py verify-load
```

The native Spark MLlib artifact is `models/random_forest_baseline/`. Shared preprocessing remains code-based because it has no fitted state; both training and reload verification call `spark/preprocess.py` directly.

## Verified Phase 3B workflow

The local full dataset is currently extracted under `MERGED_CSV(1)/MERGED_CSV/`. This verified Linux command used all 63 CSV partitions, 8 local Spark worker threads, a 6 GiB driver heap, and workspace-backed shuffle/spill storage:

```bash
.venv/bin/python -u spark/train_model.py train --input "MERGED_CSV(1)/MERGED_CSV" --model-output models/random_forest_full_baseline --master "local[8]" --driver-memory 6g --spark-local-dir .spark-local
```

Reload the final model in a separate process:

```bash
.venv/bin/python -u spark/train_model.py verify-load --input "MERGED_CSV(1)/MERGED_CSV" --model models/random_forest_full_baseline --rows 5
```

Model roles are intentionally separate:

```text
models/random_forest_baseline/       Phase 3A development-sample model
models/random_forest_full_baseline/  Phase 3B unweighted reference baseline
models/random_forest_full_weighted/  Phase 3C selected realtime model
```

## Verified Mini Phase 3C workflow

The only candidate change was `weightCol=class_weight`. The two class weights were computed from Train only; preprocessing, deduplication, splits, seed, features, and all other Random Forest parameters remained unchanged. A bounded-sample smoke test succeeded before this full-data command:

```bash
.venv/bin/python -u spark/train_model.py train --input "MERGED_CSV(1)/MERGED_CSV" --model-output models/random_forest_full_weighted --baseline-model models/random_forest_full_baseline --balanced-class-weights --master "local[8]" --driver-memory 6g --spark-local-dir .spark-local
```

The initial command evaluated the existing baseline on Validation and then trained/evaluated the weighted candidate on the same split. Because specificity improved but Attack Recall declined, its validation gate correctly reported `MODEL TRADE-OFF — DECISION REQUIRED` without using Test or saving the candidate.

The user then selected the weighted trade-off for the realtime MVP before Test inspection. This verified finalization command reproduced the same population, weights, and Validation evidence before evaluating Test exactly once and saving the selected model:

```bash
.venv/bin/python -u spark/train_model.py train --input "MERGED_CSV(1)/MERGED_CSV" --model-output models/random_forest_full_weighted --baseline-model models/random_forest_full_baseline --balanced-class-weights --accept-weighted-tradeoff --master "local[8]" --driver-memory 6g --spark-local-dir .spark-local
```

Reload the selected model in a separate process:

```bash
.venv/bin/python -u spark/train_model.py verify-load --input "MERGED_CSV(1)/MERGED_CSV" --model models/random_forest_full_weighted --rows 5
```

Class weights are training-only metadata. Future Kafka events and the streaming schema still require only the 27 model inputs used by shared preprocessing; they must not contain `class_weight`.

## Verified Phase 4 workflow

Phase 4 used Apache Kafka 4.3.1 with Java 17 as one local combined KRaft broker/controller. Docker was not used because its daemon was inaccessible in this environment. From the extracted Kafka distribution root, start the formatted broker and create the one application topic:

```bash
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --topic iot-network-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

From the repository root, the three verified bounded producer runs were:

```bash
.venv/bin/python -u kafka/producer.py --scenario NORMAL --events-per-second 5 --max-events 20
.venv/bin/python -u kafka/producer.py --scenario DDOS_BURST --events-per-second 20 --max-events 20
.venv/bin/python -u kafka/producer.py --scenario MIXED --events-per-second 20 --max-events 20
```

The Kafka console consumer read all 20 events from each isolated batch. NORMAL contained only BENIGN/Normal events, DDOS_BURST contained only `DDOS-*`/Attack events, and MIXED contained 10 of each in deterministic alternating order. The 5 events/s run took 3.802 seconds for 20 sends. A separate continuous MIXED run was interrupted manually after 45 successful deliveries; it flushed and exited cleanly.

The producer defaults to the bounded Phase 1 sample, `localhost:9092`, and `iot-network-events`. It accepts a single CSV or a directory of sequential CSV partitions, validates every header, streams rows without pandas or Spark, skips invalid values without imputation, and loops the historical source in continuous mode. The exact 4-metadata-plus-27-numeric-field contract and one-time KRaft formatting command are documented in [kafka/README.md](./kafka/README.md).

## Verified Phase 5 workflow

Phase 5 was verified with PySpark/Spark 3.5.7, Scala 2.12.18, Java 17, Kafka 4.3.1, and the matching Spark Kafka connector `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`. The connector was first loaded and connected to the broker through `spark-submit --packages`; no Python dependency was added for it. For the interactive local run, this verified command gives the Python process ownership of `Ctrl+C` while passing the same JVM package to Spark:

```bash
source .venv/bin/activate
PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell' \
python -u spark/stream_ingest.py --starting-offsets latest
```

The defaults are `localhost:9092`, `iot-network-events`, and `latest`. Use `--starting-offsets earliest` only when intentionally replaying the retained topic history. The job starts separate console sinks for valid and invalid events. Valid rows retain Kafka topic, partition, offset, and timestamp; parsed event metadata; an independently parsed UTC `event_timestamp`; and all 27 double-valued model inputs. Invalid rows retain Kafka metadata, the original `raw_json`, and one first-failure `invalid_reason`.

After both streaming queries were ready, these bounded producer runs were verified:

```bash
.venv/bin/python -u kafka/producer.py --scenario NORMAL --events-per-second 5 --max-events 20
.venv/bin/python -u kafka/producer.py --scenario DDOS_BURST --events-per-second 10 --max-events 20
.venv/bin/python -u kafka/producer.py --scenario MIXED --events-per-second 10 --max-events 20
```

The final fresh-broker rerun classified offsets 0-19 as 20 valid NORMAL events, offsets 20-39 as 20 valid DDOS_BURST events, and offsets 40-59 as 20 valid MIXED events containing exactly 10 Normal and 10 Attack rows. Three deliberately injected records at offsets 60-62 were classified respectively as `malformed_json`, `missing_required_field`, and `invalid_scenario`; both streaming queries remained active. The validation path also rejected invalid timestamps, non-finite numerics, invalid ground truth/source labels, and scenario/metadata mismatches in a bounded Spark check.

A final continuous MIXED replay at 5 events/s produced 23 valid events at offsets 63-85 across multiple micro-batches before manual interruption. The producer flushed with zero delivery failures, the streaming queries stopped cleanly, and the application exited with status 0. A separate `spark-submit --packages` batch read then reconciled all 86 records by exact offset: 83 valid and 3 invalid, with no missing or duplicate continuous offsets. The broker end offset was 86. No model, inference, feature-vector assembly, aggregation, persistence, or dashboard logic is present in this phase.

## Verified Phase 6 workflow

Phase 6 uses the same Spark 3.5.7/Scala 2.12.18 runtime and matching Kafka connector as Phase 5. Start the realtime job after the local broker and topic are ready:

```bash
source .venv/bin/activate
PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell' \
python -u spark/stream_infer.py --starting-offsets latest
```

The defaults are `localhost:9092`, `iot-network-events`, `latest`, and `models/random_forest_full_weighted/`. `--starting-offsets earliest` is available for an intentional replay. Startup fails clearly if the selected weighted-model directory is missing, corrupt, or incompatible; there is no fallback model and no training, threshold tuning, class-weight input, or stream deduplication.

The job calls Phase 5's `build_validated_stream(...)`, sends only `valid_events` through `preprocess_dataframe(..., require_label=False)`, and therefore reuses the single Phase 2 `VectorAssembler` path to create the 27-dimensional `features` vector. It loads one `RandomForestClassificationModel` and maps its numeric output as `0.0 -> Normal` and `1.0 -> Attack`. The prediction console includes `topic`, `partition`, `offset`, `kafka_timestamp`, `event_time`, `event_timestamp`, `scenario`, `ground_truth`, `source_label`, `prediction`, `prediction_label`, and `probability`. The simulator fields are retained evaluation metadata, not model inputs or predictions. A second console query keeps malformed/invalid input observable without applying the model.

The verified live run began after the Phase 5 end offset of 86. NORMAL at 5 events/s produced and inferred 20 events at offsets 86-105, all predicted Normal. DDOS_BURST at 10 events/s produced and inferred 20 events at offsets 106-125, all predicted Attack. MIXED at 10 events/s produced and inferred 20 events at offsets 126-145: ground truth and predictions were both exactly 10 Normal/10 Attack, with 20 matches and no mismatch. A malformed JSON event at offset 146 appeared only in the invalid query with reason `malformed_json`, while both queries stayed active. A continuous MIXED replay at 5 events/s then produced 30 inferred events across multiple micro-batches at offsets 147-176, with 30 distinct offsets and a 15/15 prediction split. Both producer and inference job stopped cleanly on `Ctrl+C`; the final broker end offset observed for this run was 177.

Phase 6 intentionally writes only inspectable console output. It contains no realtime aggregation, alerts, persistence, dashboard, model retraining, or model tuning.

## Verified Phase 7 workflow

Phase 7 uses the same Spark 3.5.7/Scala 2.12.18 runtime, Kafka 4.3.1 broker, and `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7` connector as the verified ingestion/inference path. Start its three console queries with:

```bash
source .venv/bin/activate
PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell' \
python -u spark/stream_analytics.py --starting-offsets latest
```

The CLI defaults remain `localhost:9092`, `iot-network-events`, `latest`, and `models/random_forest_full_weighted/`. The job loads the frozen model through Phase 6 and calls `build_prediction_stream(...)`; it does not duplicate ingestion, validation, preprocessing, inference, or prediction mapping.

Metrics use `event_timestamp` with a 10-second window, 5-second slide, and 30-second watermark. The watermark bounds aggregation state; an invalid event timestamp is still rejected by Phase 5 rather than replaced with Kafka time. Each metrics row contains `window_start`, `window_end`, `event_count`, `event_rate`, `normal_count`, `attack_count`, `normal_rate`, `attack_rate`, and `status`. `event_rate` is the actual window count divided by 10 seconds. Counts and rates come only from the binary model prediction.

The fixed status rules are **demo thresholds**, not validated production IDS thresholds: attack rate below 20% is `Normal`, 20% through below 80% is `Warning`, and 80% or higher is `Critical`. Each `prediction == 1.0` creates one `Predicted Attack` alert. Alert rows retain event/Kafka timestamps, topic/partition/offset, prediction, prediction label, probability, and explicitly identified simulator metadata. `scenario`, `ground_truth`, and `source_label` neither trigger alerts nor affect metrics/status.

Three isolated 60-event runs at 5 events/s were verified:

| Scenario | Representative window | Prediction result over all 60 | Alerts |
| --- | --- | --- | ---: |
| NORMAL | 50 events, 5.0 events/s, 50 Normal, 0 Attack, attack rate 0.0, `Normal` | 60 Normal / 0 Attack | 0 |
| DDOS_BURST | 45 events, 4.5 events/s, 0 Normal, 45 Attack, attack rate 1.0, `Critical` | 0 Normal / 60 Attack | 60 |
| MIXED | 46 events, 4.6 events/s, 23 Normal, 23 Attack, attack rate 0.5, `Warning` | 30 Normal / 30 Attack | 30 |

A malformed record at offset 357 appeared only in the invalid query; it produced no prediction, metric contribution, or alert, and all queries remained active. A second integrated run sent 30 events per segment in the sequence `NORMAL -> DDOS_BURST -> NORMAL -> MIXED`. Its actual window-status sequence was `Normal -> Warning -> Critical -> Warning -> Normal -> Warning`; intermediate Warning windows are the expected overlap from the 10-second/5-second sliding window. Exact-offset reconciliation covered 300 valid Phase 7 events plus that one invalid event, matched every predicted Attack to one alert, and ended at broker offset 478. All streaming runs stopped cleanly on `Ctrl+C`.

Phase 7 has no device/source distribution because the verified event contract has no `device_id`. It does not expose Spark progress-rate fields, persist output, add infrastructure, or implement a dashboard.

## Verified Phase 8 workflow

Phase 8 adds only a small local handoff and presentation layer. Start the Phase 7 analytics job in dashboard mode after Kafka is ready:

```bash
source .venv/bin/activate
PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell' \
python -u spark/stream_analytics.py \
  --starting-offsets latest \
  --dashboard-output-dir .runtime/dashboard
```

In a separate terminal, start the verified Streamlit 1.63.0 application:

```bash
source .venv/bin/activate
streamlit run dashboard/app.py \
  --server.address 127.0.0.1 \
  --server.port 8501
```

Open `http://127.0.0.1:8501`. The dashboard refreshes its realtime fragment every two seconds; manual browser refresh is not required. Before metrics arrive, it displays `Waiting for realtime analytics data...` rather than an inferred `Normal` state.

When `--dashboard-output-dir` is supplied, analytics resets `metrics.json` and `alerts.json` to empty lists at startup. Each micro-batch atomically replaces those files using a temporary file and `os.replace`: metrics retain at most 60 windows keyed by `(window_start, window_end)`, while alerts retain at most 100 records deduplicated by `(topic, partition, offset)`. This handoff collects only those small batches. It is not a database, REST/WebSocket service, Kafka output topic, or durable history. Omitting the option retains the verified Phase 7 console-query behavior.

The live dashboard verification replayed isolated 60-event runs at 5 events/s. The final displayed snapshots were NORMAL: 15 events, 1.5 events/s, 0.0% predicted Attack, `Normal`, no alerts; DDOS_BURST: 4 events, 0.4 events/s, 100.0% predicted Attack, `Critical`, 60 retained alerts; and MIXED: 9 events, 0.9 events/s, 55.6% predicted Attack, `Warning`, 30 retained alerts. Lower counts in final windows are expected at the trailing edge of a sliding event-time window.

An integrated no-reload browser run used one Spark query and one browser tab for `NORMAL -> DDOS_BURST -> NORMAL -> MIXED`. The displayed checkpoints were respectively `Normal`/0.0% at 1.5 events/s, `Critical`/100.0% at 2.3 events/s, recovered `Normal`/0.0% at 1.9 events/s, and `Warning`/50.0% at 2.4 events/s. The short verification pauses exceeded the 10-second window, so no additional overlap status appeared in this final run. Both trend charts and the alert feed updated automatically. Alert rows appeared newest first and labeled `scenario` and `source_label` as simulator/ground-truth metadata, not multiclass predictions. One earlier invalid record at offset 808 remained visible only in the invalid console stream and did not modify dashboard snapshots. After the final four-step run, the broker end offset was 959. The analytics process and producers shut down cleanly.

No device/source chart is shown because the verified event contract contains no `device_id`. The dashboard does not import Spark, Kafka, or the model, and it does not recalculate predictions, metrics, status thresholds, or alerts.

## Verified Phase 9 workflow

Phase 9 adds standard-library SQLite persistence to the existing analytics
process. It does not add an ORM, Delta Lake, a second output service, or a new
dashboard reader.

### Final architecture

```text
CICIoT2023 CSV rows
  -> Python dataset-replay producer
  -> Kafka: iot-network-events
  -> Spark explicit-schema validation
       |-> invalid rows -> inspectable console output
       `-> valid rows -> shared 27-feature preprocessing
                       -> frozen weighted Random Forest
                       -> binary Normal/Attack predictions
                       -> event-time metrics and predicted-Attack alerts
                            |-> SQLite (durable, unbounded MVP history)
                            `-> JSON snapshots (bounded, transient)
                                  -> Streamlit dashboard
```

Operational status and alerts come only from model predictions. `scenario`,
`ground_truth`, and `source_label` remain simulation/evaluation metadata.

### Prerequisites and installation

The final workflow was verified with Python 3.12.14, OpenJDK 17.0.20, Apache
Kafka 4.3.1, PySpark 3.5.7/Scala 2.12.18, the matching
`spark-sql-kafka-0-10_2.12:3.5.7` connector, and Streamlit 1.63.0.

From the repository root:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

The saved `models/random_forest_full_weighted/` artifact must be present. If it
is absent, reproduce it with the accepted Mini Phase 3C command above; training
is never performed by the streaming job. The bounded sample is enough for the
producer demo, so the full 8.66 GiB dataset is not required unless retraining.

From an extracted Kafka 4.3.1 directory, format a new KRaft log directory only
once, then start the broker and create the topic in a second shell:

```bash
bin/kafka-storage.sh random-uuid
bin/kafka-storage.sh format --standalone -t <UUID_FROM_PREVIOUS_COMMAND> -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

```bash
bin/kafka-topics.sh --create \
  --topic iot-network-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Do not reformat an existing broker log directory. If the topic already exists,
skip its creation.

### Reproducible final demo

With Kafka running, start analytics from the repository root. This single Spark
process writes both the durable database and the unchanged dashboard snapshots:

```bash
source .venv/bin/activate
PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python" \
PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 --conf spark.sql.shuffle.partitions=8 pyspark-shell' \
python -u spark/stream_analytics.py \
  --starting-offsets latest \
  --dashboard-output-dir .runtime/dashboard \
  --history-db data/history/iot_ids_history.sqlite3
```

Start the dashboard in another terminal and open `http://127.0.0.1:8501`:

```bash
source .venv/bin/activate
streamlit run dashboard/app.py \
  --server.address 127.0.0.1 \
  --server.port 8501
```

After Spark reports that its four queries have started, run these bounded
producer commands in order. Each segment lasts about 12 seconds at 5 events/s;
leave a short pause between segments if a visually isolated status is desired.

```bash
.venv/bin/python -u kafka/producer.py --scenario NORMAL --events-per-second 5 --max-events 60
.venv/bin/python -u kafka/producer.py --scenario DDOS_BURST --events-per-second 5 --max-events 60
.venv/bin/python -u kafka/producer.py --scenario NORMAL --events-per-second 5 --max-events 60
.venv/bin/python -u kafka/producer.py --scenario MIXED --events-per-second 5 --max-events 60
```

Stop Spark and Streamlit with `Ctrl+C`. Durable history remains queryable after
all writers and the dashboard have stopped:

```bash
.venv/bin/python scripts/inspect_history.py --limit 10
```

Use `--db <path>` for a non-default database. `--history-db` is optional: when
omitted, the old Phase 7 console mode and Phase 8 dashboard-only mode behave as
before. When enabled, startup creates missing tables but never deletes or resets
existing rows. Predictions and alerts use Kafka `(topic, partition, offset)` as
their idempotency key; metric windows use `(window_start, window_end)` and are
updated as Spark refines an event-time window. SQLite connections are opened and
committed per micro-batch, and only each micro-batch's output rows are collected.

The database contains exactly three tables:

- `predictions`: Kafka identity/timestamps, event timestamps, simulator metadata,
  binary prediction/label, and JSON-encoded probability vector.
- `metrics`: the exact Phase 7 window boundaries, counts, rates, and status.
- `alerts`: the exact Phase 7 predicted-Attack alert fields and Kafka identity.

Do not use `.runtime/dashboard/*.json` as history: those files are deliberately
reset on analytics startup and capped at 60 metric windows/100 alerts. Conversely,
the durable database is not cleared automatically; remove it only when an
intentional fresh history is required.

### Final measured evidence

The live final run used one Spark process and one browser tab without manual
reload. Four 60-event segments at 5 events/s produced 240 predictions and the
visible status sequence `Normal -> Critical -> Normal -> Warning`. Representative
10-second windows were:

| Segment | Events in representative window | Event rate | Predicted attack rate | Status | Alerts over all 60 events |
| --- | ---: | ---: | ---: | --- | ---: |
| `NORMAL` | 48 | 4.8 events/s | 0.0 | `Normal` | 0 |
| `DDOS_BURST` | 48 | 4.8 events/s | 1.0 | `Critical` | 60 |
| recovery `NORMAL` | 48 | 4.8 events/s | 0.0 | `Normal` | 0 |
| `MIXED` | 50 | 5.0 events/s | 0.5 | `Warning` | 30 |

The first persistence run wrote 20 predictions, stopped, and remained readable.
A restart against the same database retained those 20 rows and appended 20 new
predictions plus 20 alerts. Replaying identical synthetic micro-batches separately
verified `INSERT OR IGNORE` for predictions/alerts and metric-window UPSERTs.
After the restart check and final demo, the stopped database contained 280
predictions, 22 metric windows, and 110 alerts, with zero duplicate primary-key
identities. The final demo itself accounted for exactly 240 predictions and 90
alerts. A malformed Kafka event at offset 1239 appeared only in the invalid
console query and left all three database counts unchanged.

The selected weighted model's held-out Test metrics remain Accuracy 0.954128,
Attack Precision 0.999941, Attack Recall 0.951698, Attack F1 0.975223, Normal
Specificity 0.998962, False Positive Rate 0.001038, and Balanced Accuracy
0.975330 (TN=156,935, FP=163, FN=140,030, TP=2,759,033). The weighting greatly
reduced false positives relative to the unweighted baseline, with the documented
trade-off of lower Attack Recall.

`event_rate` above is an event-time window count divided by 10 seconds, not an
end-to-end throughput benchmark. Spark's `inputRowsPerSecond`,
`processedRowsPerSecond`, and processing latency were not captured, so no
throughput or latency result is claimed.

### Known MVP limitations

- Binary `Normal`/`Attack` inference only; attack-family multiclass prediction is
  not implemented.
- Local single-node Kafka, Spark, SQLite, and Streamlit; no high availability or
  concurrent-writer scaling claim.
- Historical dataset replay is a simulator, not live packet capture.
- Fixed status thresholds are demo rules, not calibrated production IDS policy.
- The dashboard shows bounded current/recent state and does not browse SQLite
  history; use `scripts/inspect_history.py` for durable inspection.
- No measured Spark processing throughput/latency, production monitoring, or
  large-scale load test.

## Project status

**MVP COMPLETE.** All required Phase 1 through Phase 9 work is implemented and
verified. Remaining items in `TASKS.md` are optional backlog only.
>>>>>>> backup-local
