# CICIoT2023 Kafka dataset replay

`producer.py` streams historical CICIoT2023 CSV rows as JSON. It is a replay
simulator: it neither captures nor generates network packets, synthesizes feature
values, loads the trained model, runs Spark, nor performs inference.

## Verified local runtime

Phase 4 was verified with:

- OpenJDK 17.0.20.
- Apache Kafka 4.3.1, downloaded from Apache, SHA-512 verified, and extracted to
  `/tmp/ciciot-kafka-4.3.1/kafka_2.13-4.3.1`.
- One combined KRaft broker/controller at `localhost:9092` with no ZooKeeper.
- One topic, `iot-network-events`, with one partition and replication factor one.
- `confluent-kafka==2.15.0` as the only Python Kafka client.

Docker was not used because this environment could not access the Docker daemon.
No Kafka UI, Schema Registry, Connect service, or extra broker was added.

From the extracted Kafka distribution root, the verified one-time storage and
broker commands were:

```bash
bin/kafka-storage.sh random-uuid
bin/kafka-storage.sh format --standalone -t <UUID_FROM_PREVIOUS_COMMAND> -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

The default Kafka 4.3.1 `config/server.properties` used ports 9092 and 9093,
`/tmp/kraft-combined-logs`, and single-node replication settings. In another
terminal, create and inspect the only application topic:

```bash
bin/kafka-topics.sh --create --topic iot-network-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --describe --topic iot-network-events --bootstrap-server localhost:9092
```

KRaft formatting is a one-time action for a new log directory. Do not format an
existing broker log directory on every startup.

## Producer commands

Install the project dependencies from the repository root:

```bash
.venv/bin/python -m pip install -r requirements.txt
```

The default input is `data/sample/ciciot2023_phase1_sample.csv`; `--input` also
accepts a directory and reads its `.csv` files sequentially in filename order.
Each partition header is validated before the first message is sent. Example
bounded runs verified in Phase 4:

```bash
.venv/bin/python -u kafka/producer.py --scenario NORMAL --events-per-second 5 --max-events 20
.venv/bin/python -u kafka/producer.py --scenario DDOS_BURST --events-per-second 20 --max-events 20
.venv/bin/python -u kafka/producer.py --scenario MIXED --events-per-second 20 --max-events 20
```

`--bootstrap-servers` defaults to `localhost:9092`, and `--topic` defaults to
`iot-network-events`. Both are configurable. `--events-per-second` must be a
finite number greater than zero. `--max-events` must be a positive integer and
counts successful deliveries; omit it to replay continuously until `Ctrl+C`.
When the matching historical rows are exhausted, the producer reopens the CSV
source and continues without retaining all rows in memory.

The console consumer command used to inspect each isolated 20-message batch was:

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-network-events --partition 0 --offset <BATCH_START_OFFSET> --max-messages 20 --timeout-ms 15000
```

## Scenario rules

| Scenario | Historical rows | Output order |
| --- | --- | --- |
| `NORMAL` | Normalized `Label == BENIGN` only | Source order |
| `DDOS_BURST` | Normalized `Label` beginning with `DDOS-` only | Source order |
| `MIXED` | Separate BENIGN and `DDOS-*` iterators | Deterministic Normal, Attack, Normal, Attack, ... |

`DDOS_BURST` therefore excludes `DOS-*`, `MIRAI-*`, reconnaissance, and every
other non-DDoS attack family. `MIXED` is a fixed 1:1 replay, not random sampling.

## JSON event contract

Every event has exactly four metadata fields followed by 27 numeric model inputs:

| Metadata field | Meaning |
| --- | --- |
| `event_time` | Timezone-aware UTC ISO-8601 simulator send time ending in `Z`; not a dataset packet timestamp |
| `scenario` | Replay mode: `NORMAL`, `DDOS_BURST`, or `MIXED` |
| `ground_truth` | Dataset-derived binary evaluation metadata: `Normal` for BENIGN and `Attack` for DDoS |
| `source_label` | Normalized original CICIoT2023 `Label`; historical ground truth, not a prediction |

The exact numeric model inputs are:

```text
Header_Length, Time_To_Live, Rate,
fin_flag_number, syn_flag_number, rst_flag_number, psh_flag_number,
ack_flag_number, ack_count, syn_count, fin_count, rst_count,
HTTP, HTTPS, DNS, TCP, UDP, ARP, ICMP, IPv,
Tot sum, Min, Max, AVG, Std, IAT, Variance
```

The event does not contain `class_weight`, a `features` vector, `prediction`, or
`device_id`. Phase 5 deserializes and validates this contract in
`spark/stream_ingest.py`; later inference will assemble the feature vector and
produce the separate binary prediction.

If any required numeric value is missing, cannot be parsed, is NaN, or is positive
or negative infinity, the row is skipped and `skipped_invalid` increases. A null,
empty, or unknown label is also skipped; it never defaults to Attack. A missing
required header fails before Kafka delivery. No imputation is performed.

## Phase 4 verification evidence

The bounded sample was used; the 45-million-row dataset was not used for producer
debugging or benchmarking.

| Run | Sent | Console-consumed | Result |
| --- | ---: | ---: | --- |
| `NORMAL`, 5 events/s | 20 | 20 (offsets 0-19) | Exact schema; all BENIGN/Normal; runtime 3.802 s |
| `DDOS_BURST`, 20 events/s | 20 | 20 (offsets 20-39) | Exact schema; all `DDOS-*`/Attack; no BENIGN |
| `MIXED`, 20 events/s | 20 | 20 (offsets 40-59) | Exact schema; 10 Normal and 10 Attack in deterministic alternating order |
| Continuous `MIXED`, 10 events/s | 45 | Broker end offset advanced from 60 to 105 | Manual `Ctrl+C`; delivery failures 0; flush succeeded; exit 0 |

All consumed feature values were JSON numbers and finite. The three bounded runs
reported zero invalid rows because none appeared before their twentieth matching
record. A separate full-cycle NORMAL iterator check yielded 703 valid replay rows,
looped the source, and measured `skipped_invalid=1` for the sample's malformed
BENIGN row. Synthetic policy checks also rejected unknown, empty, NaN, `Inf`, and
`-Inf` values. A deliberately incomplete CSV header exited with status 1 before
delivery and listed its missing required columns.
