# Prime Detect Match Node
[![Version](https://img.shields.io/badge/version-2.3.3-blue.svg)](pyproject.toml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/release/python-3100/)
[![Apache Flink 2.2](https://img.shields.io/badge/Flink-2.2-green.svg)](https://flink.apache.org/)

Real-time threat detection using Apache Flink and Sigma rules. Consumes event streams from Kafka, applies Sigma rules, and tags matched events with MITRE ATT&CK techniques and rule metadata.
This project is a component of Prime Detect OSS by SOC Prime. See its [README](https://github.com/socprime/detectflow-main) for more details and instructions.

## Features

- **Stream processing** — events are processed in real time via Apache Flink (Python DataStream API).
- **Sigma rules** — rules, filters and parsers arrive on a Kafka topic and are hot-reloaded without restarting the job.
- **Multiple output destinations** — up to 10 Kafka topics, each with its own mode (`matched_only` / `all_events` / `unmatched_only`) and, for `matched_only`, an optional Sigma severity filter.
- **Deployment variants** — Apache Flink on Kubernetes Operator or Confluent Manager for Flink (CMF).

## System Architecture

- **JobManager** — manages the job, checkpoints, and REST API.
- **TaskManager** — runs the event matching job.

## Pipeline (Data Flow)

- **Events**: one or more input topics are unioned and distributed across parallel Sigma Matcher instances.
- **Rules**: the rules topic is filtered by `JOB_ID` and broadcast to all Sigma Matcher instances.
- **Sigma Matcher**: buffers events in a window (time or count), then applies filters, field mapping and Sigma rules, and tags matches.
- **Outputs**: each event is written to the destinations whose mode accepts it.

## Project Structure

```
├── app/
│   ├── main.py                 # Entry point, CLI, job invocation
│   ├── config/                 # Settings, logging, version
│   ├── connectors/             # Kafka source/sink
│   ├── domain/                 # Rules, filters, logsources, sigma_matcher, keying
│   ├── jobs/                   # Flink pipeline build
│   └── operators/              # sigma_broadcast — KeyedBroadcastProcessFunction
├── lib/                        # Flink Kafka connector JAR
├── tests/
├── Dockerfile                  # Apache Flink image (Kubernetes Operator)
├── Dockerfile.confluent        # Confluent Flink (CMF) image
├── pyproject.toml
└── .env.example
```

## Requirements

- Python 3.10+
- Apache Flink 2.2 (Kubernetes variant) or Confluent Flink 2.0 / CMF (Confluent variant) — included in the respective container image
- Kafka (input topics, rules topic, output topics)
- Dependencies: see `pyproject.toml` (Sigma parsing comes from [detectflow-parser](https://github.com/socprime/detectflow-parser))

## Configuration

Copy `.env.example` to `.env` and fill in:

- **Kafka connection**: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_AUTH_METHOD` (`SASL` / `SSL` / `PLAINTEXT`) and the matching credentials (`KAFKA_API_KEY` / `KAFKA_API_SECRET` for SASL, `KAFKA_SSL_*` certificate paths for SSL).
- **Input**: `KAFKA_INPUT_TOPICS` (comma-separated), `KAFKA_RULES_TOPIC`.
- **Outputs**: `KAFKA_OUTPUT_DESTINATIONS` — JSON array (max 10) of `{"topic", "output_mode", "apply_parser_to_output_events", "severities"}`, for example `[{"topic":"detectflow.detections","output_mode":"matched_only","severities":["high","critical"]}]`.
- **Job**: `JOB_ID` — selects which Sigma rules from the rules topic this job runs.
- **Flink**: `FLINK_PARALLELISM`.
- **Windowing**: `WINDOW_TYPE` (`time` / `count`), `WINDOW_SIZE_SECONDS`, `WINDOW_COUNT_THRESHOLD`.
- **Logging**: `LOG_LEVEL`.

All other settings have sensible defaults; see the comments in `.env.example`.

## Running

The repository ships two Docker images — pick the one that matches your target runtime.

### Apache Flink on Kubernetes Operator

```bash
docker build -t detectflow-matchnode:k8s .
```

The image is submitted by the Flink Kubernetes Operator. In Prime Detect the deployment is created per pipeline by [Prime Detect Backend](https://github.com/socprime/detectflow-backend).

### Confluent Flink (CMF)

```bash
docker build -f Dockerfile.confluent -t detectflow-matchnode:cmf .
```

### Locally (for development)

```bash
uv sync --extra kubernetes   # or: --extra confluent
cp .env.example .env         # then fill in Kafka and topics
uv run python -m app.main --job-id local
```

## Multi-tenancy

Each job is identified by `JOB_ID`. Rules from `KAFKA_RULES_TOPIC` are filtered by `job_id`, so one cluster can serve multiple tenants or scenarios.
