# DetectFlow Match Node
[![Version](https://img.shields.io/badge/version-0.9.2-blue.svg)](VERSION)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.121.3-green.svg)](https://fastapi.tiangolo.com/)

Real-time threat detection using Apache Flink and Sigma rules. Consumes event streams from Kafka, applies Sigma rules, and tags matched events with MITRE ATT&CK techniques and rule metadata.
This project is a component of SOC Prime DetectFlow OSS. See its [README](https://github.com/socprime/detectflow-main) for more details and instructions.

## Features

- **Stream processing** — events are processed in real time via Apache Flink (Python DataStream API).
- **Sigma rules** — support Sigma rules.
- **Kafka** — input event topics, rules topic (broadcast), output detections topic; optional per-rule metrics topic; SASL/SSL auth.
- **Checkpointing** — state persistence and recovery after failures.

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
│   ├── config/                 # Settings, logging
│   ├── connectors/             # Kafka source/sink
│   ├── domain/                 # Rules, filters, logsources, sigma_matcher
│   ├── jobs/                   # sigma_detection — Flink pipeline build
│   └── operators/              # sigma_broadcast — KeyedBroadcastProcessFunction
├── lib/                        # Flink Kafka connector JAR
├── docker-compose.yml          # JobManager + TaskManager(s)
├── Dockerfile
├── pyproject.toml
└── .env.example
```

## Requirements

- Python 3.10+
- Apache Flink 2.2 (included in the container)
- Kafka (input topics, rules topic, output topic)
- Dependencies: see `pyproject.toml` (apache-flink, confluent-kafka, polars, PyYAML, orjson, pydantic, structlog, schema-parser, etc.)

## Configuration

Copy `.env.example` to `.env` and fill in:

- **Kafka**: `KAFKA_BOOTSTRAP_SERVERS`, auth (SASL/SSL), `KAFKA_INPUT_TOPICS`, `KAFKA_OUTPUT_TOPIC`, `KAFKA_RULES_TOPIC`, optionally `KAFKA_METRICS_TOPIC`.
- **Job**: `JOB_ID` (multi-tenancy), `OUTPUT_MODE` (matched_only / all_events), `APPLY_PARSER_TO_OUTPUT_EVENTS`.
- **Flink**: `FLINK_PARALLELISM`, checkpoint (interval, timeout), `STATE_BACKEND` (rocksdb recommended), `CHECKPOINT_PATH`.
- **Keying**: `KEYING_STRATEGY` (hash/computer/round_robin), `KEY_GROUPS_PER_TASK`, optionally `MAX_PARALLELISM` for autoscaling.
- **RocksDB**: buffer sizes, block cache, compaction (when state backend is rocksdb).
- **Watermarks**: `ENABLE_WATERMARKS`, out-of-orderness and idle timeout (optional).
- **Logging**: `LOG_LEVEL`, `LOG_FORMAT` (json/console).

See comments in `.env.example` for details.

## Running

**Docker Compose** (JobManager + TaskManager):

```bash
cp .env.example .env
```

### Docker

```bash
docker build -t detectflow-matchnode .
docker run -p 8000:8000 --env-file .env detectflow-matchnode
```

The job is submitted via `standalone-job.sh` with `--pyModule app.main`. Web UI: http://localhost:8081.

**Locally** (single process, for development):

```bash
uv sync
# Set Kafka and topics in .env
python -m app.main --job-id local
```

## Multi-tenancy

Each job is identified by `JOB_ID`. Rules from `KAFKA_RULES_TOPIC` are filtered by the tag/attribute matching `job_id`, so one cluster can serve multiple tenants or scenarios. Input/output topics can be set via CLI (`--input-topics`, `--output-topic`) or environment variables.
