# DetectFlow Match Node
[![Version](https://img.shields.io/badge/version-1.1.0-blue.svg)](pyproject.toml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/release/python-3100/)
[![Apache Flink 2.2](https://img.shields.io/badge/Flink-2.2-green.svg)](https://flink.apache.org/)

Real-time threat detection using Apache Flink and Sigma rules. Consumes event streams from Kafka, applies Sigma rules, and tags matched events with MITRE ATT&CK techniques and rule metadata.
This project is a component of SOC Prime DetectFlow OSS. See its [README](https://github.com/socprime/detectflow-main) for more details and instructions.

## Features

- **Stream processing** — events are processed in real time via Apache Flink (Python DataStream API).
- **Sigma rules** — support Sigma rules.
- **Kafka** — input event topics, rules topic (broadcast), output detections topic; optional per-rule metrics topic; SASL/SSL/PLAINTEXT auth.
- **Checkpointing** — state persistence and recovery after failures.
- **Deployment variants** — Apache Flink on Kubernetes Operator (`[kubernetes]` extra) or Confluent Manager for Flink / CMF (`[confluent]` extra).

## System Architecture

- **JobManager** — manages the job, checkpoints, and REST API.
- **TaskManager** — runs events match job.

## Pipeline (Data Flow)

- **Events**: one or more topics are unioned, then keyed by (hash/computer/round_robin) for distribution across parallel instances.
- **Rules**: one topic is read from `earliest`, deserialized, and broadcast to all Sigma Matcher partitions.
- **Sigma Matcher**: buffers events in a window (per key), on timer loads rules, parses Sigma, applies field mapping and matching logic.

## Project Structure

```
├── app/
│   ├── main.py                 # Entry point, CLI, job invocation
│   ├── config/                 # Settings, logging, version
│   ├── connectors/             # Kafka source/sink
│   ├── domain/                 # Rules, filters, logsources, sigma_matcher, keying
│   ├── jobs/                   # sigma_detection — Flink pipeline build
│   └── operators/              # sigma_broadcast — KeyedBroadcastProcessFunction
├── lib/                        # Flink Kafka connector JAR
├── Dockerfile                  # Apache Flink image (Kubernetes Operator)
├── Dockerfile.confluent        # Confluent Flink (CMF) image
├── pyproject.toml
└── .env.example
```

## Requirements

- Python 3.10+
- Apache Flink 2.2 (Kubernetes variant) or Confluent Flink 2.0 / CMF (Confluent variant) — included in the respective container image
- Kafka (input topics, rules topic, output topic)
- Dependencies: see `pyproject.toml` (polars, PyYAML, orjson, pydantic, pydantic-settings, structlog, confluent-kafka, schema-parser, and one of apache-flink / cp-pyflink)

## Configuration

Copy `.env.example` to `.env` and fill in:

- **Kafka connection**: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_AUTH_METHOD` (`SASL` / `SSL` / `PLAINTEXT`), SASL (`KAFKA_API_KEY`, `KAFKA_API_SECRET`) or SSL (`KAFKA_SSL_CA_LOCATION`, `KAFKA_SSL_CERTIFICATE_LOCATION`, `KAFKA_SSL_KEY_LOCATION`, optional `KAFKA_SSL_KEY_PASSWORD`, `KAFKA_SSL_CHECK_HOSTNAME`).
- **Kafka topics**: `KAFKA_INPUT_TOPICS` (comma-separated), `KAFKA_OUTPUT_TOPIC`, `KAFKA_RULES_TOPIC`, optional `KAFKA_METRICS_TOPIC`; `KAFKA_CONSUMER_GROUP`, `KAFKA_STARTING_OFFSET`.
- **Job (multi-tenancy)**: `JOB_ID`, `OUTPUT_MODE` (`matched_only` / `all_events`), `KEEP_FILTERED_EVENTS`, `APPLY_PARSER_TO_OUTPUT_EVENTS`, optional `SIGMA_RULES_FILE`.
- **Flink**: `FLINK_PARALLELISM`, optional `KAFKA_SOURCE_PARALLELISM` (when Kafka partitions < parallelism), `FLINK_CHECKPOINT_INTERVAL_MS`, `FLINK_CHECKPOINT_TIMEOUT_MS`, `FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS`, `STATE_BACKEND` (`rocksdb` recommended), `CHECKPOINT_PATH`.
- **Keying & autoscaling**: `KEYING_STRATEGY` (`hash` / `computer` / `round_robin`), `KEY_GROUPS_PER_TASK`, `AUTOSCALER_ENABLED`, `AUTOSCALER_MAX_PARALLELISM`.
- **RocksDB** (when `STATE_BACKEND=rocksdb`): `ROCKSDB_WRITEBUFFER_SIZE_MB`, `ROCKSDB_WRITEBUFFER_COUNT`, `ROCKSDB_BLOCK_CACHE_SIZE_MB`, `ROCKSDB_BLOCK_SIZE_KB`, `ROCKSDB_COMPACTION_STYLE`, `ROCKSDB_THREAD_NUM`.
- **Windowing**: `WINDOW_TYPE` (`time` / `count`), `WINDOW_SIZE_SECONDS`, `WINDOW_COUNT_THRESHOLD`.
- **Watermarks**: `ENABLE_WATERMARKS`, `WATERMARK_OUT_OF_ORDERNESS_SECONDS`, `WATERMARK_IDLE_TIMEOUT_SECONDS`.
- **Logging**: `LOG_LEVEL`, `LOG_FORMAT` (`json` / `console`).

See comments in `.env.example` for details.

## Running

The repository ships two Docker images — pick the one that matches your target runtime.

### Apache Flink on Kubernetes Operator

```bash
docker build -t detectflow-matchnode:k8s .
```

Image is designed to be submitted by the Flink Kubernetes Operator, which supplies the entrypoint (PythonDriver with `--pyModule app.main`). Web UI: http://localhost:8081 (port 8081), RPC: 6123.

### Confluent Flink (CMF)

```bash
docker build -f Dockerfile.confluent -t detectflow-matchnode:cmf .
```

Image is built on `confluentinc/cp-flink` and uses `cp-pyflink` instead of `apache-flink`, installed via the `[confluent]` extra.

### Locally (single process, for development)

```bash
uv sync --extra kubernetes   # or: --extra confluent
cp .env.example .env         # then fill in Kafka and topics
python -m app.main --job-id local
# or via the installed console script:
flink-sigma-detector --job-id local
```

## Multi-tenancy

Each job is identified by `JOB_ID`. Rules from `KAFKA_RULES_TOPIC` are filtered by the tag/attribute matching `job_id`, so one cluster can serve multiple tenants or scenarios. Input/output topics can be set via CLI (`--input-topics`, `--output-topic`, `--rules-topic`, `--metrics-topic`) or environment variables.
