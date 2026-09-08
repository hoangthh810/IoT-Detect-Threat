# TASKS.md

This roadmap is ordered by dependency. A checkbox may be marked complete only when the repository contains verifiable implementation or output evidence.

## Next recommended task

The required university MVP is complete through Phase 9. Start no further work
unless an item is deliberately selected from the optional backlog.

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

- [x] Train either Logistic Regression or Random Forest as the first baseline.
- [x] Report Accuracy, Precision, Recall, and F1 for the binary task.
- [x] Produce and explain a confusion matrix, emphasizing Attack Recall/F1.
- [x] Save the fitted preprocessing/model pipeline or equivalent reusable artifacts.
- [x] Reload the saved artifact in a fresh process and run sample predictions.

**Expected Output:** A saved Spark MLlib baseline and an evaluation summary using measured results.

**Definition of Done:** The saved model reloads successfully and predicts only `Normal` or `Attack` on transformed test rows.

**Dependencies:** Phase 2 completed.

**Phase 3A evidence:** `spark/train_model.py` trained one untuned Random Forest only on the bounded development-sample Train split. Test metrics were Accuracy 0.989644, Attack Precision 0.994238, Attack Recall 0.995155, and Attack F1 0.994696, with TN=83, FP=25, FN=21, and TP=4,313. `models/random_forest_baseline/` reloaded successfully in a fresh process and produced binary sample predictions. These are development baseline results, not final full-dataset results.

**Phase 3B evidence:** The same preprocessing, deduplication, 70/15/15 seeded split, and untuned Random Forest configuration ran on all 63 local CSV partitions. From 45,019,243 raw rows, preprocessing retained 45,018,243 and model-example deduplication retained 20,362,785. Final Test metrics were Accuracy 0.975085, Attack Precision 0.986085, Attack Recall 0.987672, Attack F1 0.986878, Normal Specificity 0.742810, and False Positive Rate 0.257190, with TN=116,694, FP=40,404, FN=35,741, and TP=2,863,322. `models/random_forest_full_baseline/` reloaded successfully in a fresh process and remains the unweighted full-data reference baseline.

**Mini Phase 3C evidence:** `spark/train_model.py` has an opt-in `--balanced-class-weights` path; unweighted behavior remains the default. On the unchanged full Train split, the computed weights were 9.720599668821 for Normal and 0.527113204019 for Attack. On the same Validation split, baseline versus weighted Normal Specificity was 0.745212 versus 0.999045, while Attack Recall was 0.987539 versus 0.951662. The initial validation gate correctly withheld Test and save because this was a trade-off. The user then accepted the weighted trade-off for the realtime MVP before Test inspection. `--accept-weighted-tradeoff` reproduced the counts, weights, and Validation result; it then evaluated the frozen candidate on Test exactly once. Weighted Test metrics were Accuracy 0.954128, Attack Precision 0.999941, Attack Recall 0.951698, Attack F1 0.975223, Normal Specificity 0.998962, False Positive Rate 0.001038, and Balanced Accuracy 0.975330, with TN=156,935, FP=163, FN=140,030, and TP=2,759,033. `models/random_forest_full_weighted/` saved and reloaded successfully in a fresh process and is the selected model for future realtime inference. Class weights remain training-only and are not Kafka/streaming input fields.

## Phase 4 - Kafka producer

**Objective:** Replay historical CICIoT2023 rows as controllable JSON event streams.

- [x] Define and document the minimal JSON event schema required by streaming inference.
- [x] Keep the 27 model inputs separate from `event_time`, `scenario`, `ground_truth`, and `source_label` metadata; do not invent `device_id`.
- [x] Create and verify one `iot-network-events` Kafka topic with one partition and replication factor one.
- [x] Replay dataset records continuously at a configurable `events_per_second`.
- [x] Implement and verify the `NORMAL` scenario.
- [x] Implement and verify the `DDOS_BURST` scenario.
- [x] Implement and verify the deterministic 1:1 `MIXED` scenario.
- [x] Keep configuration to the required input, Kafka, scenario, rate, and bounded-run arguments.

**Expected Output:** A Python dataset-replay producer and documented event contract.

**Definition of Done:** Kafka receives a continuous sequence of valid JSON events at the configured rate for all three MVP scenarios.

**Dependencies:** Phase 1 schema findings and the Phase 2 feature contract; Kafka runtime availability.

**Evidence:** `kafka/producer.py` and `kafka/README.md`. Apache Kafka 4.3.1 ran locally as one KRaft broker and reported `iot-network-events` with one partition, replication factor one, and an in-sync leader. The producer sent three isolated 20-event batches, and Kafka's console consumer read all 20 from each: NORMAL was entirely BENIGN/Normal, DDOS_BURST entirely `DDOS-*`/Attack with no BENIGN, and MIXED exactly alternated 10 Normal with 10 Attack events. All messages had the exact four metadata fields and 27 finite numeric feature fields. The measured 5 events/s run sent 20 events in 3.802 seconds. A continuous 10 events/s run delivered 45 messages before manual `Ctrl+C`, flushed successfully, and exited cleanly with no delivery failure.

## Phase 5 - Spark Structured Streaming

**Objective:** Reliably ingest and validate simulated events from Kafka.

- [x] Connect Spark Structured Streaming to the configured Kafka topic.
- [x] Deserialize JSON with an explicit schema.
- [x] Validate required fields and handle malformed records visibly.
- [x] Confirm stable continuous ingestion on a small event rate before increasing load.

**Expected Output:** A streaming ingestion job that exposes validated event rows for inference.

**Definition of Done:** Spark continuously reads valid events from Kafka without unhandled errors and makes invalid-record behavior observable.

**Dependencies:** Phase 4 producer/topic and a compatible Spark-Kafka runtime.

**Evidence:** `spark/stream_ingest.py` was rerun from a fresh topic with Spark 3.5.7/Scala 2.12.18 and `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7` against the live Kafka 4.3.1 broker. With the stream started from `latest`, Spark accepted 20/20 NORMAL events (offsets 0-19), 20/20 DDOS_BURST events (20-39), and 20/20 MIXED events (40-59, exactly 10 Normal and 10 Attack). It routed three deliberate failures at offsets 60-62 to `malformed_json`, `missing_required_field`, and `invalid_scenario` while retaining their raw JSON and Kafka metadata. A bounded Spark validation also covered every remaining reason and NaN/positive-infinity/negative-infinity inputs. A continuous 5 events/s run consumed 23 distinct valid events across multiple micro-batches at offsets 63-85; producer flush and stream shutdown both completed cleanly. A separate successful `spark-submit --packages` reconciliation accounted for all 83 valid and 3 invalid records, and the final broker end offset was 86. Phase 5 contains no model load, inference, feature-vector assembly, analytics, persistence, or dashboard behavior.

## Phase 6 - Realtime inference

**Objective:** Apply the saved offline model to each validated streaming event.

- [x] Load the saved MLlib model without training inside the streaming job.
- [x] Apply the shared preprocessing transformations from Phase 2.
- [x] Produce a binary `Normal`/`Attack` prediction.
- [x] Preserve a clear separation between prediction and `scenario`/`ground_truth` metadata.
- [x] Write a simple inspectable prediction output for debugging.

**Expected Output:** A continuous stream of binary predictions with relevant event metadata.

**Definition of Done:** Events replayed through Kafka continuously receive predictions from the reloaded model, with no simulator label presented as model output.

**Dependencies:** Phases 2, 3, and 5 completed.

**Evidence:** `spark/stream_infer.py` reuses Phase 5's `build_validated_stream(...)`, passes only `valid_events` through Phase 2's `preprocess_dataframe(..., require_label=False)`, and loads `models/random_forest_full_weighted/` once as a 27-feature, two-class `RandomForestClassificationModel`. It performs no training, fallback, class weighting, threshold tuning, or stream deduplication. With Spark 3.5.7/Scala 2.12.18 and `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`, the live run inferred 20/20 NORMAL events as Normal (offsets 86-105), 20/20 DDOS_BURST events as Attack (106-125), and all 20 MIXED events (126-145) as 10 Normal/10 Attack matching their 10/10 ground-truth split. A malformed JSON record at offset 146 was visible only in the invalid query. A continuous 5 events/s MIXED run inferred 30 distinct events across multiple micro-batches at offsets 147-176, and both producer and inference job stopped cleanly on `Ctrl+C`. The final observed broker end offset was 177. Output is console-only; Phase 6 adds no analytics, alerts, persistence, or dashboard behavior.

## Phase 7 - Realtime analytics and alerts

**Objective:** Calculate only the metrics and alerts required to make scenario changes obvious during the demo.

- [x] Calculate windowed event count and event rate.
- [x] Calculate predicted Attack count, attack rate, and prediction distribution.
- [x] Define simple, documented `Normal`/`Warning`/`Critical` status logic using prediction-derived metrics.
- [x] Produce a basic alert feed with timestamp, prediction, and available event metadata.
- [x] Device/source count is N/A because the verified event contract has no `device_id`; no replacement field was invented.
- [ ] Spark progress throughput/latency is not exposed in Phase 7; this optional item does not block the prediction-derived analytics Definition of Done.

**Expected Output:** Simple realtime metric and alert outputs suitable for persistence and dashboard display.

**Definition of Done:** Metrics and alerts visibly distinguish `NORMAL`, DDoS burst, and `MIXED` runs without treating ground truth as prediction.

**Dependencies:** Phase 6 completed.

**Evidence:** `spark/stream_analytics.py` loads the frozen model and obtains predictions through Phase 6 helpers, then applies a 10-second event-time window, 5-second slide, and 30-second watermark. It derives event count/rate, binary prediction counts/rates, and fixed demo status exclusively from `prediction`; alerts filter only `prediction == 1.0`. Isolated 60-event runs at 5 events/s produced representative windows of NORMAL: 50 events, 5.0 events/s, 50/0 Normal/Attack, attack rate 0.0, `Normal`; DDOS_BURST: 45 events, 4.5 events/s, 0/45, attack rate 1.0, `Critical`; MIXED: 46 events, 4.6 events/s, 23/23, attack rate 0.5, `Warning`. Exact range reconciliation found 0, 60, and 30 alerts respectively, exactly matching predicted Attack counts over each full 60-event run. A malformed record at offset 357 reached only the invalid stream, with no prediction, metric, or alert. The 30-event-per-segment integrated sequence produced `Normal -> Warning -> Critical -> Warning -> Normal -> Warning`, including expected sliding-window transition states. In total, offsets 177-477 contained 300 valid Phase 7 events and one invalid event; broker end offset was 478. Queries and producers stopped cleanly. Phase 7 adds no persistence or dashboard.

## Phase 8 - Dashboard MVP

**Objective:** Let a non-technical viewer understand the system state within a few seconds.

- [x] Build one small Streamlit dashboard; do not add a second dashboard framework.
- [x] Show a prominent `Normal`/`Warning`/`Critical` status indicator.
- [x] Show realtime event-rate and predicted attack-rate trends.
- [x] Show an auto-updating alert feed.
- [x] Show scenario or ground-truth attack type only when available and label it explicitly as simulation/evaluation metadata.
- [x] Device/source distribution is N/A because the verified event contract has no `device_id`; no replacement field was invented.
- [x] Use the simplest direct output-reading or refresh mechanism that satisfies the demo.

**Expected Output:** A compact auto-updating Streamlit dashboard backed by the pipeline's metric/alert output.

**Definition of Done:** A viewer can quickly identify normal traffic versus an active attack and see the key supporting indicators without reading console logs or manually refreshing.

**Dependencies:** Phase 7 completed and a simple readable metric/alert output available.

**Evidence:** `dashboard/app.py` runs on verified Streamlit 1.63.0 and reads only the bounded transient snapshots written by the optional `spark/stream_analytics.py --dashboard-output-dir .runtime/dashboard` mode. Analytics atomically replaces startup-reset JSON lists, retaining at most 60 metric windows by `(window_start, window_end)` and 100 alerts by Kafka identity. Without the option, Phase 7's three console queries remain the default. The dashboard refreshes every two seconds with `st.fragment`, displays a waiting state before data, shows the latest Phase 7 status unchanged, four KPIs, event-rate and predicted-attack-rate trends, and newest-first alerts with explicit simulator/ground-truth metadata labels. Isolated live 60-event runs at 5 events/s displayed NORMAL as `Normal`/0.0% with no alerts, DDOS_BURST as `Critical`/100.0% with 60 alerts, and MIXED as `Warning`/55.6% with 30 alerts. One Spark query and one browser tab then completed the required `NORMAL -> DDOS_BURST -> NORMAL -> MIXED` sequence without reload, displaying `Normal`/0.0% -> `Critical`/100.0% -> `Normal`/0.0% -> `Warning`/50.0%; deliberate verification pauses exceeded the 10-second window, so this final run had no additional overlap status. An invalid event at offset 808 stayed only in the invalid console stream and did not modify metrics or alerts; broker end offset after the final sequence was 959. Static compilation, bounded snapshot upsert/dedup/limit/reset/atomic-write checks, Streamlit AppTest empty/populated checks, browser route/UI checks, dependency validation, default Phase 7 console-mode regression, and clean shutdown all passed. Phase 8 adds no database, durable persistence, REST/WebSocket service, Kafka output topic, device chart, model change, threshold change, or duplicated analytics.

## Phase 9 - Persistence and final demo

**Objective:** Preserve useful history and make the full MVP reproducible for another student.

- [x] Persist prediction, metric, alert, and required event history using standard-library SQLite.
- [x] Keep Delta Lake and Bronze/Silver/Gold out of the MVP because SQLite satisfies the actual local history/query requirement with less infrastructure.
- [x] Verify that saved history can be queried after streaming stops and survives an analytics restart.
- [x] Document only verified installation, broker, pipeline, dashboard, replay, and history-inspection commands.
- [x] Run the demo sequence `NORMAL -> DDoS burst -> NORMAL -> MIXED` in one Spark process and one browser tab.
- [x] Summarize measured ML metrics and explicitly record that Spark processing throughput/latency was not captured.
- [x] Test the documented workflow in the verified local dependency environment, including dashboard/history together, persistence restart, invalid input, and clean shutdown.

**Expected Output:** Queryable history, verified run instructions, and a repeatable end-to-end demo flow.

**Definition of Done:** Another student can follow `README.md`, run the main pipeline, observe the dashboard through the full scenario sequence, and inspect saved history.

**Dependencies:** Phases 1-8 completed.

**Evidence:** `spark/stream_analytics.py` accepts the optional `--history-db`
path and creates exactly `predictions`, `metrics`, and `alerts` without resetting
existing data. Predictions/alerts use Kafka identity with `INSERT OR IGNORE`;
metric windows use their exact boundaries with UPSERT. Connections and collection
are micro-batch scoped. `scripts/inspect_history.py` uses only `sqlite3` to print
counts and recent rows after the stream stops. A 20-event first run remained
readable after shutdown; a restart retained it and appended another 20 predictions
and 20 alerts. The final 4x60-event browser demo displayed
`Normal -> Critical -> Normal -> Warning` without reload and added exactly 240
predictions and 90 alerts. Representative windows measured 4.8/0.0/`Normal`,
4.8/1.0/`Critical`, 4.8/0.0/`Normal`, and 5.0/0.5/`Warning` for event rate,
predicted attack rate, and status respectively. After all writers stopped, the
database remained queryable with 280 predictions, 22 metric windows, and 110
alerts and zero duplicate identities. A malformed event at offset 1239 stayed
outside all three tables. Static compilation, CLI, schema/idempotency/UPSERT,
dependency, live Kafka/Spark, Streamlit browser, restart, and clean-shutdown
checks passed. SQLite uses no new package; requirements remain unchanged.

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
