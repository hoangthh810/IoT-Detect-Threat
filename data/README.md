# CICIoT2023 data understanding and preprocessing

This report records feature/data-quality evidence from a bounded development sample, label-only verification across all 63 local partitions, the completed Phase 3B full-dataset baseline, and the finalized Mini Phase 3C balanced-class-weight model. Sample findings remain distinct from full-data metrics.

## Dataset structure

- Current local dataset path: `MERGED_CSV(1)/MERGED_CSV/`.
- CSV files: **63** (`Merged01.csv` through `Merged63.csv`).
- Total size from file metadata: **9,300,139,459 bytes** (approximately 8.66 GiB).
- All files are in one directory. Names are sequential partitions and do not identify an attack type.
- Representative files inspected by header plus their first five records:

| File | Size | Role in inspection |
| --- | ---: | --- |
| `Merged01.csv` | 147,115,614 bytes (140.30 MiB) | Early partition |
| `Merged32.csv` | 145,464,744 bytes (138.73 MiB) | Middle partition |
| `Merged63.csv` | 88,473,082 bytes (84.37 MiB) | Final partition |

The three representative files have identical 40-column headers.

## Development sample

- File: `data/sample/ciciot2023_phase1_sample.csv`.
- Size: 6,194,896 bytes (5.91 MiB).
- Rows: **30,000 data rows** plus one header.
- Sampling method: stream the first 10,000 rows from each of `Merged01.csv`, `Merged32.csv`, and `Merged63.csv` after verifying identical headers and row widths.
- The sample was not class-balanced. Creating and profiling it did not scan every source row; the later label verification projected only `Label` across all partitions.

## Schema

Spark inferred **40 columns**: 38 numeric columns and 2 string columns.

```text
Header_Length, Protocol Type, Time_To_Live, Rate,
fin_flag_number, syn_flag_number, rst_flag_number, psh_flag_number,
ack_flag_number, ece_flag_number, cwr_flag_number,
ack_count, syn_count, fin_count, rst_count,
HTTP, HTTPS, DNS, Telnet, SMTP, SSH, IRC,
TCP, UDP, DHCP, ARP, ICMP, IGMP, IPv, LLC,
Tot sum, Min, Max, AVG, Std, Tot size, IAT, Number, Variance,
Label
```

- `Label` is the verified label column and is inferred as string.
- `Rate` is inferred as string because the sample contains a positive-infinity value. All other observed `Rate` values cast to double without an invalid token.
- `Protocol Type`, packet/flag counts, `Tot sum`, `Min`, `Max`, and `Number` are inferred as integer.
- The other 30 feature columns are inferred as double.
- No timestamp, device ID, source/destination address, or other obvious metadata column exists in this 40-column schema.

## Labels

### Full-dataset label verification

The pre-Phase-3 verification projected only `Label` from all **63** CSV partitions. It found **34** valid distinct labels: one normal label and 33 attack labels. All valid labels are now present in `KNOWN_LABELS`. Nine rows have a null label and remain invalid; they are filtered rather than silently mapped to Attack.

| Label | Count |
| --- | ---: |
| `DDOS-ICMP_FLOOD` | 6,893,259 |
| `DDOS-UDP_FLOOD` | 5,181,027 |
| `DDOS-TCP_FLOOD` | 4,306,086 |
| `DDOS-PSHACK_FLOOD` | 3,920,372 |
| `DDOS-SYN_FLOOD` | 3,886,130 |
| `DDOS-RSTFINFLOOD` | 3,872,808 |
| `DDOS-SYNONYMOUSIP_FLOOD` | 3,445,659 |
| `DOS-UDP_FLOOD` | 3,177,323 |
| `DOS-TCP_FLOOD` | 2,558,256 |
| `DOS-SYN_FLOOD` | 1,942,176 |
| `BENIGN` | 1,051,373 |
| `MIRAI-GREETH_FLOOD` | 949,381 |
| `MIRAI-UDPPLAIN` | 852,695 |
| `MIRAI-GREIP_FLOOD` | 719,655 |
| `DDOS-ICMP_FRAGMENTATION` | 433,157 |
| `VULNERABILITYSCAN` | 357,583 |
| `MITM-ARPSPOOFING` | 294,469 |
| `DDOS-UDP_FRAGMENTATION` | 274,909 |
| `DDOS-ACK_FRAGMENTATION` | 272,793 |
| `DNS_SPOOFING` | 171,468 |
| `RECON-HOSTDISCOVERY` | 128,677 |
| `RECON-OSSCAN` | 93,970 |
| `RECON-PORTSCAN` | 78,730 |
| `DOS-HTTP_FLOOD` | 68,799 |
| `DDOS-HTTP_FLOOD` | 27,597 |
| `DDOS-SLOWLORIS` | 22,400 |
| `DICTIONARYBRUTEFORCE` | 12,522 |
| `BROWSERHIJACKING` | 5,630 |
| `COMMANDINJECTION` | 5,168 |
| `SQLINJECTION` | 5,022 |
| `XSS` | 3,705 |
| `BACKDOOR_MALWARE` | 3,078 |
| `RECON-PINGSWEEP` | 2,161 |
| `UPLOADING_ATTACK` | 1,196 |

Valid rows comprise **1,051,373 BENIGN** and **43,967,861 attack** records. The verification discovered `UPLOADING_ATTACK`, which was absent from the sample-derived mapping; it was added to `KNOWN_ATTACK_LABELS` and still maps only to binary Attack (`1.0`). No labels defined in the updated `KNOWN_LABELS` are absent from the full dataset.

### Development-sample labels

The sample contains 33 observed non-null labels:

| Label | Count | Share |
| --- | ---: | ---: |
| `DDOS-ICMP_FLOOD` | 4,591 | 15.3033% |
| `DDOS-UDP_FLOOD` | 3,527 | 11.7567% |
| `DDOS-TCP_FLOOD` | 2,964 | 9.8800% |
| `DDOS-RSTFINFLOOD` | 2,634 | 8.7800% |
| `DDOS-PSHACK_FLOOD` | 2,587 | 8.6233% |
| `DDOS-SYN_FLOOD` | 2,482 | 8.2733% |
| `DDOS-SYNONYMOUSIP_FLOOD` | 2,259 | 7.5300% |
| `DOS-UDP_FLOOD` | 2,060 | 6.8667% |
| `DOS-TCP_FLOOD` | 1,708 | 5.6933% |
| `DOS-SYN_FLOOD` | 1,318 | 4.3933% |
| `BENIGN` | 703 | 2.3433% |
| `MIRAI-GREETH_FLOOD` | 634 | 2.1133% |
| `MIRAI-UDPPLAIN` | 580 | 1.9333% |
| `MIRAI-GREIP_FLOOD` | 456 | 1.5200% |
| `DDOS-ICMP_FRAGMENTATION` | 295 | 0.9833% |
| `VULNERABILITYSCAN` | 233 | 0.7767% |
| `DDOS-ACK_FRAGMENTATION` | 198 | 0.6600% |
| `MITM-ARPSPOOFING` | 188 | 0.6267% |
| `DDOS-UDP_FRAGMENTATION` | 176 | 0.5867% |
| `RECON-HOSTDISCOVERY` | 103 | 0.3433% |
| `DNS_SPOOFING` | 101 | 0.3367% |
| `RECON-PORTSCAN` | 57 | 0.1900% |
| `RECON-OSSCAN` | 54 | 0.1800% |
| `DOS-HTTP_FLOOD` | 41 | 0.1367% |
| `DDOS-HTTP_FLOOD` | 16 | 0.0533% |
| `DDOS-SLOWLORIS` | 10 | 0.0333% |
| `DICTIONARYBRUTEFORCE` | 8 | 0.0267% |
| `BROWSERHIJACKING` | 4 | 0.0133% |
| `SQLINJECTION` | 4 | 0.0133% |
| `XSS` | 3 | 0.0100% |
| `BACKDOOR_MALWARE` | 2 | 0.0067% |
| `COMMANDINJECTION` | 2 | 0.0067% |
| `RECON-PINGSWEEP` | 2 | 0.0067% |

### Proposed binary mapping

The data provides one unambiguous normal-like value:

```text
BENIGN -> Normal
every other observed non-null label -> Attack
```

On this sample, the proposed mapping gives:

- Normal: **703 rows (2.3433%)**.
- Attack: **29,297 rows (97.6567%)**.

This is a Phase 1 proposal based on observed labels, not a completed preprocessing transformation.

## Data quality

- Nulls: `Std` has 1 and `Variance` has 1.
- NaN: none observed in Spark-inferred numeric columns or in `Rate` after casting.
- Positive infinity: 1 value in numeric-looking `Rate`.
- Negative infinity: none observed.
- Exact duplicate rows: **771 (2.57%)**.
- Constant columns: none on the sample.
- Near-constant columns (dominant zero value): `ece_flag_number`, `cwr_flag_number`, `Telnet`, `SMTP`, `SSH`, `IRC`, `DHCP`, and `IGMP` are zero in at least 99.49% of sample rows.
- `Number=100` in 28,497 rows (94.99%), so its usefulness/generalization needs investigation.
- `AVG == Tot size` for all 30,000 rows.
- `IPv == LLC` for all 30,000 rows.
- Large observed ranges include `Max` up to 14,546, `Std` up to 4,465.40, and `Variance` up to 19,939,783.29. These are observations only; no outlier removal is performed in Phase 1.

Local Spark emitted Windows native-Hadoop/`winutils.exe` warnings, but the CSV EDA completed successfully with exit code 0. No Hadoop service was added because it was not required for this local sample analysis.

## Candidate features

### Candidate model features

- Traffic/header/timing: `Header_Length`, `Time_To_Live`, `Rate`, `IAT`.
- TCP flags and counts: `fin_flag_number`, `syn_flag_number`, `rst_flag_number`, `psh_flag_number`, `ack_flag_number`, `ece_flag_number`, `cwr_flag_number`, `ack_count`, `syn_count`, `fin_count`, `rst_count`.
- Protocol/application indicators: `HTTP`, `HTTPS`, `DNS`, `Telnet`, `SMTP`, `SSH`, `IRC`, `TCP`, `UDP`, `DHCP`, `ARP`, `ICMP`, `IGMP`, `IPv`, `LLC`.
- Aggregate statistics: `Tot sum`, `Min`, `Max`, `AVG`, `Std`, `Tot size`, `Number`, `Variance`.

### Potential metadata

No obvious metadata column is present. `Protocol Type` is a numeric protocol code and should be treated as categorical-like rather than as a continuous measurement.

### Label and leakage risk

- `Label` is direct target leakage and must never be included in the feature vector.
- `AVG`/`Tot size` and `IPv`/`LLC` are exact duplicate pairs on the sample; retaining both may add redundant information.
- `Number` and the aggregate-statistic group require further investigation for collection artifacts and high correlation before the final feature set is chosen.
- Near-constant indicators may carry little information in this sample, but Phase 1 does not remove them.

## Reproduce Phase 1

From the repository root on Windows:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
python spark/eda.py sample --source MERGED_CSV/MERGED_CSV/Merged01.csv --source MERGED_CSV/MERGED_CSV/Merged32.csv --source MERGED_CSV/MERGED_CSV/Merged63.csv --output data/sample/ciciot2023_phase1_sample.csv --rows-per-file 10000
.\.venv\Scripts\python.exe spark/eda.py analyze --input data/sample/ciciot2023_phase1_sample.csv
```

The sample command validates header equality and row width while streaming only the bounded prefix from each source file.

## Phase 2 preprocessing result

Phase 2 uses `spark/preprocess.py` as the single row-wise preprocessing path for both labeled offline data and unlabeled inference data. It was validated only on the bounded 30,000-row sample.

### Input and explicit types

- Input: `data/sample/ciciot2023_phase1_sample.csv`.
- Input rows: 30,000.
- Input columns: 40.
- Raw CSV schema: 39 feature columns read as string plus `Label` as string. Reading raw tokens first makes cast failures and non-finite values observable.
- Final numeric policy: the labeled offline path validates and casts all 39 source feature columns, while the unlabeled inference path validates and casts the exact 27 selected model inputs. Both use one projection and the same selected-feature invalid-value rules.
- `Label` remains string; `binary_label` is double.
- Cast failures observed: 0.

### Problems and cleaning decisions

| Problem | Count | Percentage | Action | Reason |
| --- | ---: | ---: | --- | --- |
| `Rate=+Inf` | 1 | 0.0033% | Drop affected row | Non-finite values cannot enter `VectorAssembler`; prevalence is too small to justify imputation or clipping. |
| `Rate=-Inf` | 0 | 0% | No action | Not observed. |
| `Rate` invalid cast / NaN | 0 | 0% | No action | Not observed. |
| `Std` null | 1 | 0.0033% | Drop affected row | Same rare invalid row; simple row removal is clearer than adding an imputer. |
| `Variance` null | 1 | 0.0033% | Drop affected row | Same rare invalid row; simple row removal is clearer than adding an imputer. |
| Invalid/null/empty/unexpected `Label` | 0 | 0% | Reject if encountered | Unknown labels must not be silently mapped to Attack. |
| Exact duplicate rows | 771 in Phase 1 sample | 2.57% | Retain in shared row-wise output; deduplicate model examples before split | Streaming preprocessing must not deduplicate blindly, but allowing identical model examples across split boundaries creates evaluation leakage. |

The `Rate`, `Std`, and `Variance` problems occur on the same `BENIGN` row. Therefore preprocessing removes exactly 1 row, not 3.

```text
Input rows:       30,000
Removed rows:          1
Remaining rows:   29,999
Removed share:    0.0033%
```

No mean/median imputation, zero fill, clipping, resampling, or scaling is applied. After row-wise cleaning, offline splitting deduplicates on the 27 selected features plus `binary_label`; this reduces 29,999 prepared sample rows to **28,959 unique model examples**. Phase 3B later measured full-dataset deduplication directly; see the final section below.

### Binary label mapping

The original `Label` is preserved for debugging and evaluation. A separate double column is added:

```text
BENIGN                                  -> binary_label=0.0 (Normal)
33 verified full-dataset attack labels  -> binary_label=1.0 (Attack)
null/empty/unexpected label             -> invalid row
```

After cleaning:

- Normal: 702 rows (2.3401%).
- Attack: 29,297 rows (97.6599%).

### Baseline feature selection

- Original columns: 40.
- Numeric source features: 39.
- Selected features: **27**.
- Label: `Label`, excluded from model features.
- Metadata: none in the source schema. Extra future metadata columns remain outside the explicit assembler list.

Selected features:

```text
Header_Length, Time_To_Live, Rate,
fin_flag_number, syn_flag_number, rst_flag_number,
psh_flag_number, ack_flag_number,
ack_count, syn_count, fin_count, rst_count,
HTTP, HTTPS, DNS, TCP, UDP, ARP, ICMP, IPv,
Tot sum, Min, Max, AVG, Std, IAT, Variance
```

Excluded source features:

| Column | Reason |
| --- | --- |
| `Protocol Type` | Categorical code with ordinal ambiguity; selected protocol indicators retain protocol information. |
| `ece_flag_number`, `cwr_flag_number` | At least 99.93% zero in the sample. |
| `Telnet`, `SMTP`, `SSH`, `IRC`, `DHCP`, `IGMP` | At least 99.49% zero in the sample. |
| `LLC` | Identical to `IPv` in all 30,000 sample rows. |
| `Tot size` | Identical to `AVG` in all 30,000 sample rows. |
| `Number` | Equals 100 in 94.99% of sample rows. |

These exclusions define a simple baseline feature contract. Their usefulness should be revisited only if later full-data/model evidence warrants it; Phase 2 does not run automatic feature selection.

### ML-ready validation

- `VectorAssembler` inputs: 27 selected double columns.
- Observed vector dimension: 27.
- Selected-feature null after cleaning: 0.
- NaN after cleaning: 0.
- Positive/negative infinity after cleaning: 0.
- Null `binary_label`: 0.
- Scaling: not applied; Phase 3 may add it only if the chosen baseline requires it.

The same function was smoke-tested with an input containing exactly the 27 selected model fields and no `Label`: it produced 29,999 unlabeled rows with 27-dimensional vectors and did not create `binary_label`. This is the shared feature path intended for future streaming inference and matches the Phase 4 Kafka event feature contract.

### Reproducible split

Offline model preparation deduplicates on the 27 selected features plus `binary_label`, then Spark `randomSplit` uses weights 70/15/15 with seed 42:

| Dataset | Rows | Normal | Attack | Normal share | Attack share |
| --- | ---: | ---: | ---: | ---: | ---: |
| Train | 20,153 | 495 | 19,658 | 2.4562% | 97.5438% |
| Validation | 4,364 | 99 | 4,265 | 2.2686% | 97.7314% |
| Test | 4,442 | 108 | 4,334 | 2.4313% | 97.5687% |

All three splits retain both classes with similar imbalance. Fingerprint checks found zero overlap for train/validation, train/test, and validation/test after deduplication. Before this policy, the same seed produced 208, 209, and 53 overlapping fingerprints respectively. No over/undersampling is performed.

### Reproduce Phase 2

```powershell
.\.venv\Scripts\python.exe spark/preprocess.py --input data/sample/ciciot2023_phase1_sample.csv
.\.venv\Scripts\python.exe spark/verify_preprocessing.py labels --input MERGED_CSV/MERGED_CSV
.\.venv\Scripts\python.exe spark/verify_preprocessing.py duplicates --input data/sample/ciciot2023_phase1_sample.csv
```

Result: **Phase 2 VERIFIED — READY FOR PHASE 3** on the development sample. The shared preprocessing logic is ML-ready and reusable by later offline training and unlabeled inference paths.

## Phase 3A Random Forest baseline validation

Phase 3A reused `preprocess_dataframe()` and `split_dataset()` without changing the Phase 2 cleaning, label mapping, feature list, assembly, deduplication, or split policy. It trained one `RandomForestClassifier` on the bounded development-sample Train split only. No scaler, resampling, tuning, model comparison, or full-dataset training was performed.

### Training input and split

| Item | Result |
| --- | ---: |
| Raw sample rows | 30,000 |
| ML-ready rows | 29,999 |
| Rows after model-example deduplication | 28,959 |
| Selected features / vector dimension | 27 / 27 |
| Train rows (Normal / Attack) | 20,153 (495 / 19,658) |
| Validation rows (Normal / Attack) | 4,364 (99 / 4,265) |
| Test rows (Normal / Attack) | 4,442 (108 / 4,334) |

All splits retained both classes. The classifier used `labelCol=binary_label`, `featuresCol=features`, and `seed=42`. `numTrees=20`, `maxDepth=5`, `maxBins=32`, and `featureSubsetStrategy=auto` remained at Spark 3.5.7 defaults.

### Measured metrics

Attack (`binary_label=1.0`) is the positive class.

| Dataset | Accuracy | Attack Precision | Attack Recall | Attack F1 | TN | FP | FN | TP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Validation | 0.991063 | 0.995544 | 0.995311 | 0.995427 | 80 | 19 | 20 | 4,245 |
| Test | 0.989644 | 0.994238 | 0.995155 | 0.994696 | 83 | 25 | 21 | 4,313 |

On Test, the model missed 21 of 4,334 attacks (False Negatives, approximately 0.4845%). It also reported 25 of only 108 Normal examples as Attack, so Normal specificity was 83/108 (approximately 76.85%). The strong Accuracy and Attack metrics therefore do not mean the model is equally strong on the minority Normal class; false alerts are the clearest weakness in this development sample.

### Feature importance and persistence

The top impurity-based feature importances were `HTTPS` (0.18956471), `IAT` (0.13640716), `Rate` (0.12614218), `Header_Length` (0.12436275), `Tot sum` (0.10331583), `Max` (0.07793530), `ack_count` (0.03855428), `ack_flag_number` (0.03659262), `Std` (0.03521673), and `syn_count` (0.01783563). They are descriptive baseline information only; no feature was removed and the model was not retrained from these values.

The model was saved natively to `models/random_forest_baseline/`. A separate `verify-load` process loaded it with `RandomForestClassificationModel.load()`, called the shared preprocessing path on sample rows, and successfully produced five binary predictions with probability vectors.

```text
python spark/train_model.py train
python spark/train_model.py verify-load
```

Result: **PHASE 3A COMPLETE — READY TO SCALE**. All metrics above are development-sample baseline metrics, not final CICIoT2023 metrics.

## Phase 3B full-dataset Random Forest baseline

Phase 3B ran the same shared preprocessing, 27-feature contract, model-example deduplication, seeded 70/15/15 split, and Random Forest configuration on all 63 local CSV partitions. No scaling, resampling, class weighting, tuning, threshold adjustment, feature redesign, or model comparison was applied.

### Full input, cleaning, and deduplication

| Item | Result |
| --- | ---: |
| CSV partitions | 63 |
| Dataset size | 9,300,139,459 bytes (approximately 8.66 GiB) |
| Raw rows | 45,019,243 |
| ML-ready rows | 45,018,243 |
| Rows removed during cleaning | 1,000 (0.002221%) |
| Rows before model-example deduplication | 45,018,243 |
| Rows after model-example deduplication | 20,362,785 |
| Rows removed by deduplication | 24,655,458 (54.767704%) |
| Selected features / vector dimension | 27 / 27 |

The run did not add extra full-data scans to break the 1,000 removed rows into overlapping invalid-value categories. The cleaning count comes directly from the shared preprocessing output. Deduplication happened before `randomSplit`, so identical model examples cannot be assigned to different splits by construction.

### Model-example distribution and split

After cleaning and deduplication, model examples contained 1,047,308 Normal rows (5.1432%) and 19,315,477 Attack rows (94.8568%). This is still strongly imbalanced, although the Normal share is higher than in the raw label distribution because duplicate removal affected the classes differently.

| Dataset | Rows | Normal | Attack | Normal share | Attack share |
| --- | ---: | ---: | ---: | ---: | ---: |
| Train | 14,253,082 | 733,138 | 13,519,944 | 5.1437% | 94.8563% |
| Validation | 3,053,542 | 157,072 | 2,896,470 | 5.1439% | 94.8561% |
| Test | 3,056,161 | 157,098 | 2,899,063 | 5.1404% | 94.8596% |

All splits retained both classes. The model was fit on Train only. The configuration was unchanged from Phase 3A: `labelCol=binary_label`, `featuresCol=features`, `seed=42`, with Spark defaults `numTrees=20`, `maxDepth=5`, `maxBins=32`, and `featureSubsetStrategy=auto`.

### Final full-dataset metrics

Attack (`binary_label=1.0`) is the positive class.

| Dataset | Accuracy | Attack Precision | Attack Recall | Attack F1 | Normal Specificity | FPR | TN | FP | FN | TP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Validation | 0.975074 | 0.986202 | 0.987539 | 0.986870 | 0.745212 | 0.254788 | 117,052 | 40,020 | 36,093 | 2,860,377 |
| Test | 0.975085 | 0.986085 | 0.987672 | 0.986878 | 0.742810 | 0.257190 | 116,694 | 40,404 | 35,741 | 2,863,322 |

The final Test split contained 35,741 attacks predicted as Normal, approximately 1.2328% of actual attacks. It also contained 40,404 Normal rows predicted as Attack, or 25.7190% of actual Normal rows. Attack Recall/F1 remain high, but the Normal false-positive rate is a significant baseline weakness. Accuracy alone is especially misleading here because almost 94.86% of deduplicated model examples are Attack.

### Phase 3A versus Phase 3B Test

| Metric | Phase 3A sample | Phase 3B full |
| --- | ---: | ---: |
| Accuracy | 0.989644 | 0.975085 |
| Attack Precision | 0.994238 | 0.986085 |
| Attack Recall | 0.995155 | 0.987672 |
| Attack F1 | 0.994696 | 0.986878 |
| Normal Specificity | 0.768519 | 0.742810 |
| False Positive Rate | 0.231481 | 0.257190 |

Every reported classification metric weakened when the verified flow scaled from the bounded sample to the full dataset, and Normal false positives increased. The full result is more trustworthy for the project baseline because it covers all partitions and millions of held-out model examples; no tuning was performed in response.

### Feature importance, runtime, and artifact

The top impurity-based feature importances were `HTTPS` (0.23511776), `IAT` (0.13210193), `Header_Length` (0.13124571), `Tot sum` (0.10691966), `Rate` (0.08877284), `Max` (0.05271312), `ack_count` (0.05160815), `Std` (0.03470063), `Min` (0.03006315), and `syn_count` (0.02877536). They were not used for feature removal or retraining.

The verified local run used `local[8]`, a 6 GiB driver heap, and `.spark-local/` for shuffle/spill. End-to-end training-command runtime was 865.8 seconds; Random Forest fitting itself took 66.7 seconds. Spark warned that its internal Random Forest training RDD could not fit entirely in memory and spilled blocks to disk, but the job completed without OOM, executor/driver crash, or disk failure. Other observed warnings were the local hostname fallback, missing native-Hadoop library fallback, optimizer maximum-iteration notice, and truncated plan display.

The final native Spark MLlib model is `models/random_forest_full_baseline/`; `models/random_forest_baseline/` remains the separate Phase 3A development artifact. A fresh process loaded the final model and successfully produced five binary predictions with probability vectors from shared-preprocessed full-dataset raw rows.

```bash
.venv/bin/python -u spark/train_model.py train --input "MERGED_CSV(1)/MERGED_CSV" --model-output models/random_forest_full_baseline --master "local[8]" --driver-memory 6g --spark-local-dir .spark-local
.venv/bin/python -u spark/train_model.py verify-load --input "MERGED_CSV(1)/MERGED_CSV" --model models/random_forest_full_baseline --rows 5
```

Result: **PHASE 3B COMPLETE — FINAL BASELINE MODEL READY**.

## Mini Phase 3C class-imbalance validation

Mini Phase 3C tested exactly one balanced-class-weight Random Forest. It reused all 63 CSV partitions, the shared cleaning path, 27-feature vector, model-example deduplication, seed-42 70/15/15 splits, and the Phase 3B Random Forest defaults. The only candidate change was `weightCol=class_weight`; the weight column existed on Train only and was not part of the feature vector. A development-sample smoke test completed successfully before the full run, and its metrics were not used for model selection.

### Training population and weights

The full-data population and split counts reproduced Phase 3B exactly. Class weights were computed only from Train with `weight_class = N_train / (2 * N_class_train)`.

| Item | Result |
| --- | ---: |
| Train rows | 14,253,082 |
| Train Normal rows | 733,138 |
| Train Attack rows | 13,519,944 |
| Normal weight | 9.720599668821 |
| Attack weight | 0.527113204019 |
| Rows added or removed by weighting | 0 |

Validation and Test did not receive a weight column. No scaler, resampling, threshold change, feature change, or hyperparameter tuning was introduced.

### Validation-first comparison

The existing Phase 3B model was loaded and evaluated on the recreated Validation split before the weighted candidate was fit. Attack is the positive class; deltas are weighted minus baseline.

| Metric | Baseline | Weighted | Delta |
| --- | ---: | ---: | ---: |
| Accuracy | 0.975074 | 0.954100 | -0.020974 |
| Attack Precision | 0.986202 | 0.999946 | +0.013744 |
| Attack Recall | 0.987539 | 0.951662 | -0.035877 |
| Attack F1 | 0.986870 | 0.975207 | -0.011663 |
| Normal Specificity | 0.745212 | 0.999045 | +0.253833 |
| False Positive Rate | 0.254788 | 0.000955 | -0.253833 |
| Balanced Accuracy | 0.866376 | 0.975354 | +0.108978 |

| Validation model | TN | FP | FN | TP |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 117,052 | 40,020 | 36,093 | 2,860,377 |
| Weighted | 156,922 | 150 | 140,009 | 2,756,461 |

The weighted candidate reduced Normal false positives by 39,870 and increased true-Normal decisions by the same amount. It also increased missed attacks by 103,916 and reduced true-Attack decisions by the same amount. Specificity improved, but Attack Recall declined, so the required decision was **MODEL TRADE-OFF — DECISION REQUIRED**.

### Initial Test gate

During the initial Mini Phase 3C validation task, Test evaluation was **not run** because the candidate was not a Pareto improvement on Validation. The candidate was not promoted or saved at that point. This protected Test from model selection and left the explicit trade-off decision to the user.

The initial verified full run used `local[8]`, a 6 GiB driver heap, and `.spark-local/`. Random Forest fitting took 71.6 seconds and the end-to-end command took 881.5 seconds. Spark spilled internal Random Forest cache blocks to disk under memory pressure but completed successfully without OOM or process failure.

```bash
.venv/bin/python -u spark/train_model.py train --input "MERGED_CSV(1)/MERGED_CSV" --model-output models/random_forest_full_weighted --baseline-model models/random_forest_full_baseline --balanced-class-weights --master "local[8]" --driver-memory 6g --spark-local-dir .spark-local
```

What this experiment established:

1. Balanced class weights can almost eliminate Normal false positives for this default Random Forest.
2. That gain is not free: Attack Recall fell by about 3.59 percentage points.
3. Accuracy fell even though Balanced Accuracy rose substantially, demonstrating the effect of class imbalance.
4. Attack Precision rose because far fewer Normal rows were predicted as Attack.
5. Weighting changed neither the training population nor the 27-feature contract.
6. Train-only weight calculation avoided Validation/Test information leakage.
7. Re-evaluating the stored baseline on the same Validation split made the comparison directly controlled.
8. The validation gate protected Test from being used to resolve a model-policy trade-off.
9. The validation-only outcome required an explicit operational preference between missed attacks and false alerts.

Initial result: **Mini Phase 3C COMPLETE — MODEL TRADE-OFF REQUIRES DECISION**.

### Mini Phase 3C finalization

The user selected the weighted Random Forest for the realtime MVP from Validation evidence before Test inspection. The motivation was its much stronger Normal Specificity and suitability for a convincing `NORMAL` scenario with very few false alerts, while accepting lower but still high Attack Recall. Model selection was frozen before this finalization run; Test was used only for final unbiased evaluation.

The finalization reran the unchanged full-data flow and reproduced every expected count, both Train-only class weights, and the Validation trade-off within `1e-6`. No feature, threshold, class weight, split, seed, preprocessing rule, or Random Forest parameter changed.

#### Final weighted Test metrics

| Metric | Phase 3B baseline | Phase 3C weighted | Delta |
| --- | ---: | ---: | ---: |
| Accuracy | 0.975085 | 0.954128 | -0.020957 |
| Attack Precision | 0.986085 | 0.999941 | +0.013856 |
| Attack Recall | 0.987672 | 0.951698 | -0.035974 |
| Attack F1 | 0.986878 | 0.975223 | -0.011655 |
| Normal Specificity | 0.742810 | 0.998962 | +0.256152 |
| False Positive Rate | 0.257190 | 0.001038 | -0.256152 |
| Balanced Accuracy | 0.865241 | 0.975330 | +0.110089 |

Weighted Test confusion matrix:

| TN | FP | FN | TP |
| ---: | ---: | ---: | ---: |
| 156,935 | 163 | 140,030 | 2,759,033 |

The weighted model falsely alerted on 163 of 157,098 Normal rows, a Normal false-positive rate of 0.001038 (approximately 0.1038%). It missed 140,030 of 2,899,063 Attack rows, an attack miss rate of 0.048302 (approximately 4.8302%). Compared with the baseline, the accepted practical trade-off is dramatically quieter Normal traffic at the cost of more missed attacks. The selection was not reconsidered after seeing these Test results.

#### Artifact and fresh-process verification

The selected native Spark MLlib artifact is `models/random_forest_full_weighted/`; the existing `models/random_forest_full_baseline/` was not overwritten or removed and remains the unweighted reference. The finalization fit took 72.4 seconds and the full command took 867.6 seconds. A new Spark/Python process then loaded the weighted artifact and produced five valid binary predictions with probability vectors:

```text
1. binary_label=1.0 (Attack), prediction=1.0 (Attack), probability=[Normal=0.002761, Attack=0.997239]
2. binary_label=1.0 (Attack), prediction=1.0 (Attack), probability=[Normal=0.004888, Attack=0.995112]
3. binary_label=1.0 (Attack), prediction=1.0 (Attack), probability=[Normal=0.002761, Attack=0.997239]
4. binary_label=1.0 (Attack), prediction=1.0 (Attack), probability=[Normal=0.002761, Attack=0.997239]
5. binary_label=1.0 (Attack), prediction=1.0 (Attack), probability=[Normal=0.002761, Attack=0.997239]
```

```bash
.venv/bin/python -u spark/train_model.py train --input "MERGED_CSV(1)/MERGED_CSV" --model-output models/random_forest_full_weighted --baseline-model models/random_forest_full_baseline --balanced-class-weights --accept-weighted-tradeoff --master "local[8]" --driver-memory 6g --spark-local-dir .spark-local
.venv/bin/python -u spark/train_model.py verify-load --input "MERGED_CSV(1)/MERGED_CSV" --model models/random_forest_full_weighted --rows 5
```

`class_weight` is used only during fitting. Kafka events and the future streaming schema do not need it: inference still accepts the same 27 input features, applies shared preprocessing, and produces only `Normal` or `Attack`.

Result: **MINI PHASE 3C FINALIZATION COMPLETE — WEIGHTED RANDOM FOREST IS THE SELECTED REALTIME MODEL — READY FOR PHASE 4**.
