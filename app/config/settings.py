"""
Unified Configuration using Pydantic Settings
All environment variables loaded from .env file
"""

from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
        default="windows-events",
        description="Input Kafka topics for events (comma-separated for multiple topics)",
    )
    kafka_output_topic: str = Field(
        default="windows-detections",
        description="Output Kafka topic for detections",
    )
    kafka_rules_topic: str = Field(
        default="sigma-rules",
        description="Kafka topic for Sigma rules updates",
    )
    kafka_metrics_topic: str = Field(
        default="rule-statistics",
        description="Kafka topic for per-rule metrics",
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

    # ==========================================
    # MULTI-TENANCY CONFIGURATION
    # ==========================================

    job_id: str = Field(
        default="local",
        description="Unique job identifier for multi-tenancy (filters Sigma rules by job_id)",
    )
    output_mode: Literal["all_events", "matched_only"] = Field(
        default="matched_only",
        description="Output mode: 'all_events' (all events with match status) or 'matched_only' (only matched events)",
    )
    apply_parser_to_output_events: bool = Field(
        default=False,
        description="Apply parser to output events",
    )

    # ==========================================
    # FLINK CONFIGURATION
    # ==========================================

    # Global parallelism (controls matcher and sink)
    flink_parallelism: int = Field(
        default=2,
        ge=1,
        le=100,
        description="Parallelism for Sigma matcher and Kafka sink operators",
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

    # Keying strategy for horizontal scaling
    keying_strategy: Literal["hash", "computer", "round_robin"] = Field(
        default="hash",
        description=(
            "Event keying strategy for distributing across parallel tasks:\n"
            "  - hash: Hash-based distribution (recommended, fast, no JSON parsing)\n"
            "  - computer: Key by Computer/hostname (legacy, requires JSON parsing)\n"
            "  - round_robin: Simple round-robin (legacy, testing only)"
        ),
    )

    # Key groups per task (affects number of unique keys and on_timer() calls)
    # Higher values = better load balancing but more overhead from separate on_timer() calls
    # Lower values = less overhead but larger batches = better matching efficiency
    # Recommended: 1 for large rule sets (6400+ rules), 2-3 for smaller rule sets
    key_groups_per_task: int = Field(
        default=1,
        ge=1,
        le=100,
        description=(
            "Number of key groups per parallel task (default: 1)\n"
            "Total buckets = parallelism × key_groups_per_task\n"
            "Each key group triggers separate on_timer() with separate rule matching.\n"
            "Trade-off:\n"
            "  - Low (1): Larger batches, better matching efficiency for many rules\n"
            "  - Medium (2-3): Balance between batch size and load distribution\n"
            "  - High (10+): Better load distribution, but small batches = poor matching efficiency\n"
            "Example: parallelism=6, key_groups_per_task=1 → 6 total key buckets"
        ),
    )

    # Autoscaler settings (passed from admin-panel)
    autoscaler_enabled: bool = Field(
        default=False,
        description="Whether Flink autoscaler is enabled for this pipeline",
    )
    autoscaler_max_parallelism: int = Field(
        default=24,
        ge=1,
        description=(
            "Maximum parallelism for autoscaler (default: 24)\n"
            "Only used when autoscaler_enabled=True.\n"
            "Affects bucket calculation for hash keying."
        ),
    )

    # ==========================================
    # STATE BACKEND & CHECKPOINTS CONFIGURATION
    # ==========================================

    state_backend: Literal["memory", "rocksdb"] = Field(
        default="rocksdb",
        description=(
            "State backend for storing operator state:\n"
            "  - memory: Heap-based (fast, but limited by JVM heap)\n"
            "  - rocksdb: Disk-based embedded DB (supports large state, incremental checkpoints)"
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
        description="Event count threshold (for count-based windowing)",
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

    def get_kafka_flink_auth_config(self) -> dict[str, str]:
        """Get Kafka authentication config for PyFlink (Java Kafka client format)"""
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
            # SSL config is same for both Python and Java clients
            config = {
                "security.protocol": "SSL",
                "ssl.ca.location": self.kafka_ssl_ca_location,
                "ssl.certificate.location": self.kafka_ssl_certificate_location,
                "ssl.key.location": self.kafka_ssl_key_location,
                # Disable client telemetry to avoid zstd compression issues
                "enable.metrics.push": "false",
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

    def get_kafka_producer_config(self) -> dict[str, str]:
        """Get complete Kafka producer configuration"""
        return {
            **self.get_kafka_base_config(),
            **self.get_kafka_auth_config(),
            "acks": "all",
            "compression.type": "gzip",
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
