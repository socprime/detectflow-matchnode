"""
Unified Configuration using Pydantic Settings
All environment variables loaded from .env file
"""

from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Hard cap on Kafka detection sinks / OutputTags per pipeline.
MAX_OUTPUT_DESTINATIONS = 10

OutputMode = Literal["all_events", "matched_only", "unmatched_only"]
Severity = Literal["informational", "low", "medium", "high", "critical"]

# Which events each output mode accepts, as (matched, unmatched). Single source of truth
# for the emit filter — `unmatched_only` is exactly `all_events` minus the matches.
OUTPUT_MODE_ACCEPTS: dict[str, tuple[bool, bool]] = {
    "all_events": (True, True),
    "matched_only": (True, False),
    "unmatched_only": (False, True),
}


def destination_accepts(output_mode: str, matched: bool) -> bool:
    """True when a destination in ``output_mode`` should receive a (un)matched event."""
    accepts_matched, accepts_unmatched = OUTPUT_MODE_ACCEPTS[output_mode]
    return accepts_matched if matched else accepts_unmatched


class OutputDestination(BaseModel):
    """One Kafka destination for detection output (per-topic mode/parser)."""

    topic: str = Field(description="Kafka topic to write detection events to")
    output_mode: OutputMode = Field(
        default="matched_only",
        description=(
            "Output mode: 'all_events' (matched + unmatched), 'matched_only' (detections "
            "only), or 'unmatched_only' (everything that matched no rule — all_events "
            "minus the detections)"
        ),
    )
    apply_parser_to_output_events: bool = Field(
        default=False,
        description="Apply parser to events written to this destination",
    )
    severities: frozenset[Severity] = Field(
        default=frozenset(),
        description=(
            "matched_only only: Sigma levels a detection must have (any matching rule) to "
            "reach this destination; empty = all. Ignored (cleared) for all_events and "
            "unmatched_only, which always receive every event their mode accepts."
        ),
    )

    @field_validator("topic")
    @classmethod
    def topic_non_empty(cls, v: str) -> str:
        topic = v.strip()
        if not topic:
            raise ValueError("topic must be non-empty")
        return topic

    @model_validator(mode="after")
    def severities_only_for_matched_only(self) -> OutputDestination:
        if self.output_mode != "matched_only":
            self.severities = frozenset()
        return self


class Settings(BaseSettings):
    """
    Complete application configuration
    All settings loaded from environment variables or .env file
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ==========================================
    # KAFKA CONFIGURATION
    # ==========================================

    # Connection
    kafka_bootstrap_servers: str = Field(
        description="Kafka bootstrap servers (comma-separated)",
        examples=["pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092"],
    )
    kafka_cluster_id: str | None = Field(
        default=None,
        description="Confluent Cloud cluster ID (optional)",
    )

    # Authentication
    kafka_auth_method: Literal["SASL", "SSL", "PLAINTEXT"] = Field(
        default="SASL",
        description=(
            "Authentication method: SASL (API key), SSL (certificates), or PLAINTEXT (no auth)"
        ),
    )

    # SASL credentials (required if auth_method=SASL)
    kafka_api_key: str | None = Field(
        default=None,
        description="Kafka API key for SASL authentication",
    )
    kafka_api_secret: str | None = Field(
        default=None,
        description="Kafka API secret for SASL authentication",
    )

    # SSL certificates (required if auth_method=SSL)
    kafka_ssl_ca_location: str | None = Field(
        default=None,
        description="Path to CA certificate (PEM format)",
    )
    kafka_ssl_certificate_location: str | None = Field(
        default=None,
        description="Path to client certificate (PEM format)",
    )
    kafka_ssl_key_location: str | None = Field(
        default=None,
        description="Path to client private key (PEM format)",
    )
    kafka_ssl_key_password: str | None = Field(
        default=None,
        description="Password for encrypted private key",
    )
    kafka_ssl_truststore_location: str | None = Field(
        default=None,
        description="Path to truststore (optional)",
    )
    kafka_ssl_truststore_password: str | None = Field(
        default=None,
        description="Truststore password (optional)",
    )
    kafka_ssl_check_hostname: bool = Field(
        default=True,
        description=(
            "Verify broker hostname against SSL certificate (default: True). "
            "Set to False for development or when broker cert does not match hostname; "
            "sets ssl.endpoint.identification.algorithm=none."
        ),
    )

    # Topics
    # Note: stored as str internally, parsed to list via validator
    # This avoids Pydantic Settings trying to parse as JSON from env vars
    kafka_input_topics: str = Field(
        default="detectflow.events",
        description="Input Kafka topics for events (comma-separated for multiple topics)",
    )
    kafka_output_topic: str = Field(
        default="detectflow.detections",
        description=(
            "Legacy single output Kafka topic for detections. "
            "Used only when KAFKA_OUTPUT_DESTINATIONS is unset."
        ),
    )
    # Stored as str to avoid Pydantic Settings auto-parsing JSON from env.
    # Parse via get_output_destinations().
    kafka_output_destinations: str = Field(
        default="",
        description=(
            "JSON array of output destinations (max "
            f"{MAX_OUTPUT_DESTINATIONS}): "
            '[{"topic":"...","output_mode":"matched_only","apply_parser_to_output_events":false}]. '
            "When set, overrides KAFKA_OUTPUT_TOPIC / OUTPUT_MODE / APPLY_PARSER_TO_OUTPUT_EVENTS."
        ),
    )
    kafka_rules_topic: str = Field(
        default="detectflow.sigma-rules",
        description="Kafka topic for Sigma rules updates",
    )
    kafka_metrics_topic: str = Field(
        default="detectflow.rule-metrics",
        description="Kafka topic for per-rule metrics",
    )
    kafka_tagged_events_topic: str = Field(
        default="",
        description=(
            "Kafka topic for the tagged-events side output, assigned PER PIPELINE by the "
            "control plane (detectflow.tagged-events.<pipeline_id>). Empty disables the side "
            "output and its sink entirely: no correlation pipeline consumes this pipeline. "
            "Same convention as kafka_metrics_topic. The default is empty on purpose so a "
            "deployment that loses the variable never falls back to a shared topic."
        ),
    )

    # Consumer settings
    kafka_consumer_group: str = Field(
        default="flink-sigma",
        description="Kafka consumer group ID",
    )
    kafka_starting_offset: Literal["earliest", "latest", "committed"] = Field(
        default="latest",
        description="Kafka starting offset strategy",
    )
    kafka_producer_compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = Field(
        default="gzip",
        description=(
            "Compression codec for Flink Kafka sinks (detections output, tagged-events). "
            "Default gzip is the current production behavior. Cross-repo A/B with detectflow-agent "
            "showed gzip is Pareto-positive on Flink Java sinks (smaller payload → faster flush → "
            "less I/O wait outweighs compression CPU); lz4 is cheaper CPU when the detections "
            "producer writes several topics (compression still runs per topic-partition batch "
            "on the task thread). zstd requires zstd-jni on the Flink classpath."
        ),
    )
    kafka_producer_linger_ms: int = Field(
        default=50,
        ge=0,
        description=(
            "Kafka producer linger.ms for Flink sinks. Default 50ms batches records before send "
            "(Kafka client default is 0). Improves compression ratio and cuts produce-request "
            "rate — especially when the detections producer fans out to several topics. "
            "Windowed detection latency (seconds) makes this delay negligible."
        ),
    )
    kafka_producer_batch_size: int = Field(
        default=262144,
        ge=0,
        description=(
            "Kafka producer batch.size in bytes for Flink sinks (default 256 KiB; Kafka client "
            "default is 16 KiB). Larger batches amortize produce RPCs and improve compression "
            "when multi-destination fan-out multiplies output volume."
        ),
    )
    kafka_producer_buffer_memory_mb: int = Field(
        default=16,
        ge=1,
        le=512,
        description=(
            "Kafka producer buffer.memory per Flink sink writer, in MiB (default 16; Kafka "
            "client default is 32). Cap matters because detections + tagged-events + metrics "
            "each hold their own buffer on the TaskManager (detections are one sink for all topics)."
        ),
    )

    # ==========================================
    # MULTI-TENANCY CONFIGURATION
    # ==========================================

    job_id: str = Field(
        default="local",
        description="Unique job identifier for multi-tenancy (filters Sigma rules by job_id)",
    )
    output_mode: OutputMode = Field(
        default="matched_only",
        description=(
            "Legacy pipeline-wide output mode (shim for KAFKA_OUTPUT_DESTINATIONS). "
            "'all_events', 'matched_only', or 'unmatched_only'."
        ),
    )
    keep_filtered_events: bool = Field(
        default=False,
        description="Keep events excluded by prefilters in output as unmatched. If False, drop them.",
    )
    apply_parser_to_output_events: bool = Field(
        default=False,
        description=(
            "Legacy pipeline-wide parser-on-output flag (shim for KAFKA_OUTPUT_DESTINATIONS)."
        ),
    )
    matcher_engine: Literal["legacy", "fast"] = Field(
        default="fast",
        description=(
            "Sigma matching engine. 'fast' = compile-once ruleset with sparse null-aware "
            "DataFrame and per-column Aho-Corasick evaluation (~7-9x faster, identical "
            "matches). 'legacy' = per-window polars expression tree (fallback)."
        ),
    )

    # ==========================================
    # FLINK CONFIGURATION
    # ==========================================

    python_execution_mode: Literal["process", "thread"] = Field(
        default="process",
        description=(
            "PyFlink execution mode. 'process' (default, RECOMMENDED) runs Python UDFs in "
            "separate worker processes — better isolation from the TaskManager JVM (avoids "
            "Pemja/libpython native crashes under load). 'thread' embeds Python in the JVM "
            "via Pemja (lower per-record overhead) but requires the image Dockerfile's "
            "on_timer sed and is less stable under heavy matching. "
            "Ref: .claude/docs/pyflink/ — python.execution-mode"
        ),
    )

    # Global parallelism (controls matcher and sink)
    flink_parallelism: int = Field(
        default=2,
        ge=1,
        le=100,
        description="Parallelism for Sigma matcher and Kafka sink operators",
    )

    # Number of key groups (Flink maxParallelism). FALLBACK ONLY: used when the deployment does
    # not set pipeline.max-parallelism (e.g. local / docker standalone). In the Kubernetes
    # deployment the operator freezes pipeline.max-parallelism into the cluster config and every
    # checkpoint; the job reads that value and does NOT override it, because a mismatch breaks
    # last-state / savepoint restore. Must be >= flink_parallelism. Keep constant per pipeline.
    flink_max_parallelism: int = Field(
        default=256,
        ge=1,
        le=32768,
        description=(
            "Fallback Flink maxParallelism (number of key groups) used only when the deployment "
            "does not set pipeline.max-parallelism. Must be >= FLINK_PARALLELISM. "
            "Changing it invalidates existing savepoints."
        ),
    )

    # Optional: override source parallelism when Kafka partitions < flink_parallelism
    kafka_source_parallelism: int | None = Field(
        default=None,
        ge=1,
        le=100,
        description="Parallelism for Kafka event source. If None, uses flink_parallelism. Set this lower if your Kafka topic has fewer partitions than flink_parallelism",
    )

    flink_checkpoint_interval_ms: int = Field(
        default=60000,
        ge=1000,
        description="Checkpoint interval in milliseconds",
    )
    flink_checkpoint_timeout_ms: int = Field(
        default=300000,
        ge=10000,
        description="Checkpoint timeout in milliseconds",
    )
    flink_min_pause_between_checkpoints_ms: int = Field(
        default=30000,
        ge=0,
        description="Minimum pause between checkpoints in milliseconds",
    )

    # Event keying (see app/domain/keying/strategies.py).
    # - balanced (default): one evenly-filled bucket per subtask (LONG keys via Flink routing)
    # - hash: legacy oversharding — parallelism × KEY_GROUPS_PER_TASK string buckets (hash-N)
    # Switching strategy changes the keyed state space — do not restore a savepoint across modes.
    keying_strategy: Literal["balanced", "hash"] = Field(
        default="balanced",
        description=(
            "Event keying strategy:\n"
            "  - balanced (default): one evenly-filled bucket per subtask; even load with a "
            "single keyed window / on_timer() per subtask\n"
            "  - hash: legacy multi-bucket-per-slot (parallelism × KEY_GROUPS_PER_TASK "
            "hash-N string keys). More distribution headroom but smaller batches and more "
            "timer/matching work per window"
        ),
    )
    key_groups_per_task: int = Field(
        default=3,
        ge=1,
        le=100,
        description=(
            "Only used when KEYING_STRATEGY=hash.\n"
            "Number of key groups per parallel task (default: 3).\n"
            "Bucket heuristic:\n"
            "  - parallelism=1 -> 1 bucket\n"
            "  - parallelism>1 -> parallelism * key_groups_per_task buckets\n"
            "Example: parallelism=6, key_groups_per_task=3 -> 18 total key buckets"
        ),
    )

    # ==========================================
    # STATE BACKEND & CHECKPOINTS CONFIGURATION
    # ==========================================

    state_backend: Literal["memory", "rocksdb"] = Field(
        default="rocksdb",
        description=(
            "State backend for storing operator state:\n"
            "  - rocksdb: Disk-based embedded DB (default — incremental checkpoints, off-heap keyed state)\n"
            "  - memory: Flink hashmap (heap ListState; local/sandbox or bounded experiments)"
        ),
    )

    checkpoint_path: str = Field(
        default="file:///opt/flink/checkpoints",
        description=(
            "Checkpoint storage path:\n"
            "  - file:///opt/flink/checkpoints (default Docker volume)\n"
            "  - file:///mnt/nfs/flink-checkpoints (production NFS mount)\n"
            "For production: mount NFS and update this path"
        ),
    )

    # ==========================================
    # ROCKSDB PERFORMANCE TUNING
    # ==========================================

    rocksdb_writebuffer_size_mb: int = Field(
        default=64,
        ge=8,
        le=512,
        description=(
            "RocksDB write buffer size in MB (default: 64MB)\n"
            "Size before flushing to disk. Larger = fewer flushes, better write throughput, more memory.\n"
            "Recommendation: 64-128MB for production"
        ),
    )

    rocksdb_writebuffer_count: int = Field(
        default=3,
        ge=2,
        le=10,
        description=(
            "Number of RocksDB write buffers (default: 3)\n"
            "Total memory = writebuffer_size_mb * writebuffer_count\n"
            "Recommendation: 3-4 for production"
        ),
    )

    rocksdb_block_cache_size_mb: int = Field(
        default=256,
        ge=8,
        le=2048,
        description=(
            "RocksDB block cache size in MB (default: 256MB)\n"
            "Cache for frequently accessed data. Larger = better read performance, more memory.\n"
            "Recommendation: 256-512MB for production"
        ),
    )

    rocksdb_block_size_kb: int = Field(
        default=4,
        ge=1,
        le=64,
        description=(
            "RocksDB block size in KB (default: 4KB)\n"
            "Size of data blocks in cache. Smaller = better for point lookups, larger = better for scans.\n"
            "Recommendation: 4-16KB for streaming workloads"
        ),
    )

    rocksdb_compaction_style: Literal["LEVEL", "UNIVERSAL", "FIFO"] = Field(
        default="LEVEL",
        description=(
            "RocksDB compaction style (default: LEVEL)\n"
            "  - LEVEL: Better read performance, less space amplification (recommended for streaming)\n"
            "  - UNIVERSAL: Better write performance, more space amplification\n"
            "  - FIFO: Best write performance, no compaction (only for TTL use cases)"
        ),
    )

    rocksdb_thread_num: int = Field(
        default=4,
        ge=1,
        le=32,
        description=(
            "Number of RocksDB background threads (default: 4)\n"
            "Threads for compaction and flush operations.\n"
            "Recommendation: 4-8 threads for production"
        ),
    )

    # ==========================================
    # WINDOWING CONFIGURATION
    # ==========================================

    window_type: Literal["time", "count"] = Field(
        default="time",
        description="Window type: time-based or count-based",
    )
    window_size_seconds: int = Field(
        default=30,
        ge=1,
        le=3600,
        description="Time window size in seconds (for time-based windowing)",
    )
    window_count_threshold: int = Field(
        default=10000,
        ge=1,
        description=(
            "Maximum buffered events per key before scheduling an immediate flush.\n"
            "Acts as a guardrail to keep on_timer() state reads bounded and avoid oversized "
            "Python operator gRPC payloads."
        ),
    )

    # ==========================================
    # WATERMARK CONFIGURATION (Event-Time Processing)
    # ==========================================

    enable_watermarks: bool = Field(
        default=False,
        description=(
            "Enable watermarks for event-time processing (default: False)\n"
            "When False: uses processing-time (lower latency, simpler)\n"
            "When True: uses event-time (handles out-of-order events, late arrivals)"
        ),
    )

    watermark_out_of_orderness_seconds: int = Field(
        default=10,
        ge=0,
        le=300,
        description=(
            "Maximum out-of-orderness for watermarks in seconds (default: 10s)\n"
            "How late events can arrive before being considered too late.\n"
            "Larger values = more tolerance for late events, higher latency.\n"
            "Only used when enable_watermarks=True"
        ),
    )

    watermark_idle_timeout_seconds: int = Field(
        default=60,
        ge=0,
        le=600,
        description=(
            "Idle timeout for watermark sources in seconds (default: 60s)\n"
            "Prevents watermark stalling when some partitions have no data.\n"
            "Only used when enable_watermarks=True"
        ),
    )

    # ==========================================
    # SIGMA MATCHER CONFIGURATION
    # ==========================================

    sigma_rules_file: str | None = Field(
        default=None,
        description="Path to static Sigma rules JSON file (optional, for testing)",
    )

    # ==========================================
    # LOGGING CONFIGURATION
    # ==========================================

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level",
    )
    log_format: Literal["console", "json"] = Field(
        default="json",
        description="Log output format: 'console' for human-readable, 'json' for Fluent Bit/Loki/ELK",
    )

    # ==========================================
    # VALIDATORS
    # ==========================================

    @field_validator("kafka_api_key", "kafka_api_secret")
    @classmethod
    def validate_sasl_credentials(cls, v, info):
        """Ensure SASL credentials are provided when auth_method=SASL"""
        if info.data.get("kafka_auth_method") == "SASL" and not v:
            raise ValueError(f"{info.field_name} is required when KAFKA_AUTH_METHOD=SASL")
        return v

    @field_validator(
        "kafka_ssl_ca_location",
        "kafka_ssl_certificate_location",
        "kafka_ssl_key_location",
    )
    @classmethod
    def validate_ssl_certificates(cls, v, info):
        """Ensure SSL certificates are provided when auth_method=SSL"""
        if info.data.get("kafka_auth_method") == "SSL" and not v:
            raise ValueError(f"{info.field_name} is required when KAFKA_AUTH_METHOD=SSL")
        return v

    # ==========================================
    # HELPER METHODS
    # ==========================================

    def get_input_topics_list(self) -> list[str]:
        """Parse kafka_input_topics string into list of topics.

        Supports comma-separated format: 'topic1,topic2,topic3'
        """
        return [t.strip() for t in self.kafka_input_topics.split(",") if t.strip()]

    def tagged_events_enabled(self) -> bool:
        """True when the control plane assigned this pipeline a tagged-events topic.

        Empty means no correlation pipeline consumes this detection pipeline, so the
        side output is not built at all. Same convention as ``KAFKA_METRICS_TOPIC``.
        """
        return bool(self.kafka_tagged_events_topic and self.kafka_tagged_events_topic.strip())

    def get_output_destinations(self) -> list[OutputDestination]:
        """Resolve output destinations from JSON env or legacy single-topic shim.

        Prefer ``KAFKA_OUTPUT_DESTINATIONS`` when set. Otherwise synthesize one
        destination from ``KAFKA_OUTPUT_TOPIC`` / ``OUTPUT_MODE`` /
        ``APPLY_PARSER_TO_OUTPUT_EVENTS``.
        """
        raw = (self.kafka_output_destinations or "").strip()
        if raw:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                raise ValueError(f"KAFKA_OUTPUT_DESTINATIONS is not valid JSON: {e}") from e
            if not isinstance(data, list):
                raise ValueError("KAFKA_OUTPUT_DESTINATIONS must be a JSON array")
            destinations = [OutputDestination.model_validate(item) for item in data]
            if not destinations:
                raise ValueError("KAFKA_OUTPUT_DESTINATIONS must contain at least one destination")
            if len(destinations) > MAX_OUTPUT_DESTINATIONS:
                raise ValueError(
                    f"KAFKA_OUTPUT_DESTINATIONS supports at most {MAX_OUTPUT_DESTINATIONS} "
                    f"destinations (got {len(destinations)})"
                )
            topics = [d.topic for d in destinations]
            if len(topics) != len(set(topics)):
                raise ValueError("KAFKA_OUTPUT_DESTINATIONS contains duplicate topics")
            return destinations

        topic = (self.kafka_output_topic or "").strip()
        if not topic:
            raise ValueError("Either KAFKA_OUTPUT_DESTINATIONS or KAFKA_OUTPUT_TOPIC is required")
        return [
            OutputDestination(
                topic=topic,
                output_mode=self.output_mode,
                apply_parser_to_output_events=self.apply_parser_to_output_events,
            )
        ]

    def get_kafka_base_config(self) -> dict[str, str]:
        """Get base Kafka configuration"""
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
        }

    def get_kafka_auth_config(self) -> dict[str, str]:
        """Get Kafka authentication configuration (for Python clients)"""
        if self.kafka_auth_method == "SASL":
            return {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": self.kafka_api_key,
                "sasl.password": self.kafka_api_secret,
            }
        elif self.kafka_auth_method == "SSL":  # SSL
            config = {
                "security.protocol": "SSL",
                "ssl.ca.location": self.kafka_ssl_ca_location,
                "ssl.certificate.location": self.kafka_ssl_certificate_location,
                "ssl.key.location": self.kafka_ssl_key_location,
            }
            if not self.kafka_ssl_check_hostname:
                config["ssl.endpoint.identification.algorithm"] = "none"
            if self.kafka_ssl_key_password:
                config["ssl.key.password"] = self.kafka_ssl_key_password
            if self.kafka_ssl_truststore_location:
                config["ssl.truststore.location"] = self.kafka_ssl_truststore_location
            if self.kafka_ssl_truststore_password:
                config["ssl.truststore.password"] = self.kafka_ssl_truststore_password
            return config
        else:  # PLAINTEXT (no auth)
            return {
                "security.protocol": "PLAINTEXT",
            }

    def _read_pem_file(self, file_path: str | None) -> str | None:
        """Read PEM file content for Java Kafka client configuration."""
        if not file_path:
            return None
        try:
            from pathlib import Path

            return Path(file_path).read_text()
        except Exception:
            return None

    def get_kafka_flink_auth_config(self) -> dict[str, str]:
        """Get Kafka authentication config for PyFlink (Java Kafka client format).

        IMPORTANT: Java Kafka client uses different property names than librdkafka!
        - Truststore: ssl.truststore.type=PEM + ssl.truststore.location (file path works)
        - Keystore: ssl.keystore.type=PEM + ssl.keystore.certificate.chain (PEM content!)
                    + ssl.keystore.key (PEM content!)
        """
        if self.kafka_auth_method == "SASL":
            # PyFlink requires JAAS config format for SASL
            # IMPORTANT: Use shaded class name for Flink's shaded Kafka connector
            jaas_config = (
                f"org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{self.kafka_api_key}" '
                f'password="{self.kafka_api_secret}";'
            )
            return {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",  # Note: singular, not "mechanisms"
                "sasl.jaas.config": jaas_config,
                # Disable client telemetry to avoid zstd compression issues
                "enable.metrics.push": "false",
            }
        elif self.kafka_auth_method == "SSL":  # SSL
            # Java Kafka client SSL config (different from librdkafka!)
            # For PEM format: truststore.location accepts file path,
            # but keystore.certificate.chain and keystore.key need PEM CONTENT (not path!)
            cert_content = self._read_pem_file(self.kafka_ssl_certificate_location)
            key_content = self._read_pem_file(self.kafka_ssl_key_location)

            config: dict[str, str] = {
                "security.protocol": "SSL",
                # Truststore for CA certificate (file path works with PEM type)
                "ssl.truststore.type": "PEM",
                "ssl.truststore.location": self.kafka_ssl_ca_location,
                # Disable client telemetry to avoid zstd compression issues
                "enable.metrics.push": "false",
            }

            # Keystore for client certificate (mTLS) - needs PEM content, not file path!
            if cert_content and key_content:
                config["ssl.keystore.type"] = "PEM"
                config["ssl.keystore.certificate.chain"] = cert_content
                config["ssl.keystore.key"] = key_content

            if not self.kafka_ssl_check_hostname:
                # Empty string disables hostname verification in Java Kafka client
                # (not "none" which causes "Unknown identification algorithm" error)
                config["ssl.endpoint.identification.algorithm"] = ""
            if self.kafka_ssl_key_password:
                config["ssl.keystore.password"] = self.kafka_ssl_key_password
            # Override with JKS/PKCS12 truststore if explicitly provided
            if self.kafka_ssl_truststore_location:
                config["ssl.truststore.type"] = "JKS"
                config["ssl.truststore.location"] = self.kafka_ssl_truststore_location
            if self.kafka_ssl_truststore_password:
                config["ssl.truststore.password"] = self.kafka_ssl_truststore_password
            return config
        else:  # PLAINTEXT (no auth)
            return {
                "security.protocol": "PLAINTEXT",
                # Disable client telemetry to avoid zstd compression issues
                "enable.metrics.push": "false",
            }

    def get_kafka_consumer_config(self) -> dict[str, str]:
        """Get complete Kafka consumer configuration"""
        return {
            **self.get_kafka_base_config(),
            **self.get_kafka_auth_config(),
            "group.id": self.kafka_consumer_group,
            "auto.offset.reset": self.kafka_starting_offset,
            "enable.auto.commit": "false",
        }

    def get_kafka_flink_producer_properties(self) -> dict[str, str]:
        """Producer knobs applied to every Flink ``KafkaSink`` (acks, compression, batching)."""
        props: dict[str, str] = {
            "acks": "all",
            "linger.ms": str(self.kafka_producer_linger_ms),
            "batch.size": str(self.kafka_producer_batch_size),
            "buffer.memory": str(self.kafka_producer_buffer_memory_mb * 1024 * 1024),
        }
        if self.kafka_producer_compression_type != "none":
            props["compression.type"] = self.kafka_producer_compression_type
        return props

    def get_kafka_producer_config(self) -> dict[str, str]:
        """Get complete Kafka producer configuration (non-Flink clients / tooling)."""
        return {
            **self.get_kafka_base_config(),
            **self.get_kafka_auth_config(),
            **self.get_kafka_flink_producer_properties(),
        }


# ==========================================
# SINGLETON PATTERN
# ==========================================

_settings: Settings | None = None


def get_settings() -> Settings:
    """Get singleton settings instance"""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """Force reload settings (useful for testing)"""
    global _settings
    _settings = Settings()
    return _settings
