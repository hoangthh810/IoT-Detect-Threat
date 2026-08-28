# CICIoT2023 data understanding and preprocessing

This report records feature/data-quality evidence from a bounded development sample plus a label-only verification across all 63 local partitions. It is not a full-dataset feature profile and the sample must not be treated as statistically balanced.

## Dataset structure

- Local dataset path: `MERGED_CSV/MERGED_CSV/`.
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
- Final numeric policy: cast all 39 source feature columns to Spark `double` in one projection.
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

No mean/median imputation, zero fill, clipping, resampling, or scaling is applied. After row-wise cleaning, offline splitting deduplicates on the 27 selected features plus `binary_label`; this reduces 29,999 prepared sample rows to **28,959 unique model examples**. Full-dataset duplicate prevalence remains unverified and should be revisited before final large-scale training.

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

The same function was smoke-tested after dropping `Label`: it produced 29,999 unlabeled rows with 27-dimensional vectors and did not create `binary_label`. This is the shared feature path intended for future streaming inference.

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
