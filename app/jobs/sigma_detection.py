"""Sigma detection Flink job - Pipeline configuration and execution"""

from datetime import timedelta
from pathlib import Path

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.common.serialization import ByteArraySchema
from pyflink.datastream import StreamExecutionEnvironment

from app.config.logging import configure_logging, get_logger
from app.config.settings import Settings, get_settings
from app.connectors.kafka_sink import create_kafka_sink
from app.connectors.kafka_source import create_kafka_source
from app.domain.keying.strategies import create_key_extractor
from app.operators.sigma_broadcast import (
    METRICS_OUTPUT_TAG,
    RULES_STATE_DESCRIPTOR,
    SigmaMatcherBroadcastFunction,
)

logger = get_logger(__name__)


def add_kafka_connector_jar(env: StreamExecutionEnvironment) -> None:
    """
    Add Kafka connector JAR to Flink environment if not already in classpath.

    Checks if the JAR is already in Flink classpath (/opt/flink/lib/) when running
    in a Flink cluster (docker). If not found, adds it dynamically for local
    standalone execution.

    Args:
        env: StreamExecutionEnvironment instance
    """
    jar_name = "flink-sql-connector-kafka-4.0.1-2.0.jar"
    jar_in_classpath = Path("/opt/flink/lib") / jar_name

    if jar_in_classpath.exists():
        # JAR is already in Flink classpath (running in Flink cluster via docker)
        logger.info(
            "kafka connector loaded",
            jar=jar_name,
            path=str(jar_in_classpath),
            note="already in classpath",
        )
    else:
        # Dynamically resolve JAR path and add it (running locally as standalone script)
        jar_path = Path(__file__).parent.parent.parent / "lib" / jar_name
        if jar_path.exists():
            env.add_jars(f"file://{jar_path.absolute()}")
            logger.info("kafka connector loaded", jar=jar_name, path=str(jar_path))
        else:
            logger.warning(
                "kafka connector jar not found",
                jar=jar_name,
                checked_paths=[str(jar_in_classpath), str(jar_path)],
            )


def configure_rest_api(config) -> None:
    """
    Configure REST API (Web UI) for Flink environment.

    When running in Flink cluster (docker), REST API is already configured via FLINK_PROPERTIES.
    When running locally as standalone script, we need to configure it here.

    Args:
        config: Flink Configuration object (Java gateway)
    """
    rest_port = config.getString("rest.port", None)
    if rest_port is None:
        # Not configured yet - configure for local standalone execution
        config.setString("rest.port", "8081")
        config.setString("rest.bind-port", "8081")
        config.setString("rest.address", "0.0.0.0")  # Listen on all interfaces
        logger.info("rest api configured", port=8081, address="0.0.0.0")
    else:
        logger.info(
            "rest api configuration skipped",
            reason="already configured via FLINK_PROPERTIES",
            port=rest_port,
        )


def configure_flink_environment(env: StreamExecutionEnvironment, settings: Settings) -> None:
    """
    Configure Flink environment with state backend, checkpointing, and parallelism

    Args:
        env: StreamExecutionEnvironment instance
        settings: Application settings
    """
    add_kafka_connector_jar(env)

    # Get Flink Configuration object via Java gateway
    j_env = env._j_stream_execution_environment
    config = j_env.getConfiguration()

    configure_rest_api(config)

    # Configure State Backend
    if settings.state_backend == "rocksdb":
        # RocksDB state backend with incremental checkpoints
        config.setString("state.backend.type", "rocksdb")
        config.setString("state.backend.rocksdb.localdir", "/tmp/flink-rocksdb")
        config.setString("execution.checkpointing.incremental", "true")

        # RocksDB Performance Tuning
        # Write buffer: size before flushing to disk (default: 64MB)
        # Larger values = fewer flushes, better write throughput, more memory usage
        config.setString(
            "state.backend.rocksdb.writebuffer.size",
            str(settings.rocksdb_writebuffer_size_mb * 1024 * 1024),
        )
        config.setString(
            "state.backend.rocksdb.writebuffer.count", str(settings.rocksdb_writebuffer_count)
        )

        # Block cache: cache frequently accessed data (default: 8MB)
        # Larger values = better read performance, more memory usage
        config.setString(
            "state.backend.rocksdb.block.cache-size",
            str(settings.rocksdb_block_cache_size_mb * 1024 * 1024),
        )
        config.setString(
            "state.backend.rocksdb.block.blocksize", str(settings.rocksdb_block_size_kb * 1024)
        )

        # Compaction: strategy for organizing data on disk
        # LEVEL = better read performance, less space amplification (recommended for streaming)
        config.setString(
            "state.backend.rocksdb.compaction.style", settings.rocksdb_compaction_style
        )

        # Threads: number of background threads for RocksDB operations
        config.setString("state.backend.rocksdb.thread.num", str(settings.rocksdb_thread_num))

        logger.info(
            "state backend configured",
            backend="rocksdb",
            incremental=True,
            localdir="/tmp/flink-rocksdb",
            writebuffer_size_mb=settings.rocksdb_writebuffer_size_mb,
            writebuffer_count=settings.rocksdb_writebuffer_count,
            block_cache_size_mb=settings.rocksdb_block_cache_size_mb,
            block_size_kb=settings.rocksdb_block_size_kb,
            compaction_style=settings.rocksdb_compaction_style,
            thread_num=settings.rocksdb_thread_num,
        )
    else:
        # Use default hashmap state backend (in-memory)
        config.setString("state.backend.type", "hashmap")
        logger.info("state backend configured", backend="hashmap", incremental=False)

    # Configure Checkpoint Storage
    config.setString("state.checkpoints.dir", settings.checkpoint_path)
    logger.info("checkpoint storage configured", path=settings.checkpoint_path)

    # Configure Parallelism
    env.set_parallelism(settings.flink_parallelism)
    logger.info("parallelism configured", parallelism=settings.flink_parallelism)

    # Configure Checkpointing
    env.enable_checkpointing(settings.flink_checkpoint_interval_ms)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_timeout(settings.flink_checkpoint_timeout_ms)
    checkpoint_config.set_min_pause_between_checkpoints(
        settings.flink_min_pause_between_checkpoints_ms
    )
    logger.info(
        "checkpointing configured",
        interval_ms=settings.flink_checkpoint_interval_ms,
        timeout_ms=settings.flink_checkpoint_timeout_ms,
        min_pause_ms=settings.flink_min_pause_between_checkpoints_ms,
    )


def create_watermark_strategy(settings):
    """
    Create watermark strategy based on configuration

    Args:
        settings: Application settings

    Returns:
        WatermarkStrategy configured for event-time or processing-time
    """
    if settings.enable_watermarks:
        # Event-time processing with bounded out-of-orderness
        # Handles late events up to the specified delay
        strategy = WatermarkStrategy.for_bounded_out_of_orderness(
            timedelta(seconds=settings.watermark_out_of_orderness_seconds)
        )

        # Add idle timeout to prevent watermark stalling when partitions are idle
        if settings.watermark_idle_timeout_seconds > 0:
            strategy = strategy.with_idleness(
                timedelta(seconds=settings.watermark_idle_timeout_seconds)
            )

        logger.info(
            "watermark strategy configured",
            mode="event_time",
            out_of_orderness_seconds=settings.watermark_out_of_orderness_seconds,
            idle_timeout_seconds=settings.watermark_idle_timeout_seconds,
        )
        return strategy
    else:
        # Processing-time (no watermarks)
        # Lower latency, simpler, but doesn't handle out-of-order events
        logger.info("watermark strategy configured", mode="processing_time")
        return WatermarkStrategy.no_watermarks()


def log_kafka_configuration(settings: Settings) -> None:
    """Log Kafka configuration for visibility"""
    input_topics_list = settings.get_input_topics_list()
    logger.info(
        "kafka configuration",
        events_topics=input_topics_list,
        events_topics_count=len(input_topics_list),
        events_consumer_group=f"{settings.kafka_consumer_group}-events",
        events_starting_offset=settings.kafka_starting_offset,
        rules_topic=settings.kafka_rules_topic,
        rules_consumer_group=f"{settings.kafka_consumer_group}-rules",
        rules_starting_offset="earliest",  # Always earliest for compacted rules topic
        output_topic=settings.kafka_output_topic,
        window_size_seconds=settings.window_size_seconds,
        keying_strategy=settings.keying_strategy,
    )


def filter_and_decode_tombstones(value: bytes | None) -> list[str]:
    """Filter out tombstones and decode bytes to string.
    Returns empty list for tombstones, list with one string for valid messages.
    """
    if not value:  # Handles both None and empty bytes
        return []  # Tombstone - return empty list to filter out
    return [value.decode("utf-8")]  # Valid message - return as list for flat_map


def build_pipeline(env: StreamExecutionEnvironment, settings: Settings) -> None:
    """
    Build Flink pipeline: sources -> operators -> sinks

    Args:
        env: Configured StreamExecutionEnvironment
        settings: Application settings
    """
    # Create Kafka sources for each input topic
    # Multiple topics will be unioned into a single stream
    input_topics = settings.get_input_topics_list()
    events_sources = []
    for topic in input_topics:
        source = create_kafka_source(settings, topic, f"-events-{topic}")
        events_sources.append((topic, source))
        logger.info("kafka source created", source_type="events", topic=topic)

    # Rules source: use ByteArraySchema to handle tombstones gracefully
    # ByteArraySchema can handle null values (tombstones) without throwing NPE
    # We filter out tombstones in the flat_map operation below
    #
    # IMPORTANT: Always start from "earliest" for rules/prefilters topic!
    # This is a compacted topic - we need ALL existing rules on first startup.
    # On restart with checkpoint, Flink restores offsets from checkpoint state,
    # so this setting only affects first-time startup of new pipelines.
    rules_source = create_kafka_source(
        settings,
        settings.kafka_rules_topic,
        "-rules",
        deserializer=ByteArraySchema(),
        starting_offset_override="earliest",
    )
    logger.info("kafka source created", source_type="rules", topic=settings.kafka_rules_topic)

    # Create Kafka sinks
    kafka_sink = create_kafka_sink(settings, settings.kafka_output_topic)
    logger.info("kafka sink created", topic=settings.kafka_output_topic)

    # Create watermark strategy
    watermark_strategy = create_watermark_strategy(settings)

    # Build event stream with keying
    # IMPORTANT: Keying enables true horizontal scaling across parallel tasks
    # Each key will be processed by a separate task instance
    #
    # Calculate number of buckets for hash-based keying:
    # - Without autoscaler: parallelism × key_groups_per_task (minimal overhead)
    # - With autoscaler: autoscaler_max_parallelism × key_groups_per_task (headroom for scaling)
    #
    # Trade-off: More buckets = better distribution when scaling, but more on_timer() overhead

    if settings.autoscaler_enabled:
        # Autoscaler enabled: pre-allocate buckets for max expected parallelism
        num_buckets = settings.autoscaler_max_parallelism * settings.key_groups_per_task
        logger.info(
            "keying strategy configured (autoscaler mode)",
            strategy=settings.keying_strategy,
            initial_parallelism=settings.flink_parallelism,
            autoscaler_max_parallelism=settings.autoscaler_max_parallelism,
            key_groups_per_task=settings.key_groups_per_task,
            total_key_buckets=num_buckets,
            scaling_headroom=f"up to parallelism={settings.autoscaler_max_parallelism}",
        )
    else:
        # No autoscaler: use minimal buckets for current parallelism
        num_buckets = settings.flink_parallelism * settings.key_groups_per_task
        logger.info(
            "keying strategy configured (fixed parallelism)",
            strategy=settings.keying_strategy,
            parallelism=settings.flink_parallelism,
            key_groups_per_task=settings.key_groups_per_task,
            total_key_buckets=num_buckets,
        )

    key_extractor = create_key_extractor(settings.keying_strategy, num_buckets=num_buckets)

    # Build event streams from all sources
    # Ref: .claude/docs/pyflink/pyflink/datastream/README.md - Union operator
    event_streams = []
    for topic, source in events_sources:
        stream = env.from_source(
            source=source,
            watermark_strategy=watermark_strategy,
            source_name=f"Events Source ({topic})",
        ).set_parallelism(settings.kafka_source_parallelism or settings.flink_parallelism)
        event_streams.append(stream)

    # Union all event streams into single stream
    if len(event_streams) == 1:
        combined_events_stream = event_streams[0]
    else:
        combined_events_stream = event_streams[0].union(*event_streams[1:])
        logger.info(
            "event streams unioned",
            topics=input_topics,
            stream_count=len(event_streams),
        )

    # Key the combined stream for parallel processing
    events_stream = combined_events_stream.key_by(key_extractor)
    # Note: KeyedStream doesn't support .name() - name is set on source

    # Build rules broadcast stream
    # Rules don't need event-time semantics, use processing-time
    rules_stream = (
        env.from_source(
            source=rules_source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="Rules Source (Kafka)",
        )
        .set_parallelism(1)
        .flat_map(filter_and_decode_tombstones, output_type=Types.STRING())
        .set_parallelism(1)
        # Note: .name() must be before .broadcast() as BroadcastStream doesn't support it
        .broadcast(RULES_STATE_DESCRIPTOR)
    )

    # Connect streams with KeyedBroadcastProcessFunction
    # Keyed events stream + broadcast rules stream
    combined_stream = (
        events_stream.connect(rules_stream)
        .process(
            SigmaMatcherBroadcastFunction(
                window_size_seconds=settings.window_size_seconds,
                job_id=settings.job_id,
                output_mode=settings.output_mode,
                apply_parser_to_output_events=settings.apply_parser_to_output_events,
            ),
            output_type=Types.STRING(),
        )
        .set_parallelism(settings.flink_parallelism)
        .name("Sigma Detection Processor")
    )

    # Sink detections to Kafka
    (
        combined_stream.sink_to(kafka_sink)
        .set_parallelism(settings.flink_parallelism)
        .name("Detections Output (Kafka)")
    )

    # Sink per-rule metrics to Kafka (if metrics topic is configured)
    sinks = ["kafka_detections"]
    parallelism_config = {
        "kafka_source": settings.kafka_source_parallelism or settings.flink_parallelism,
        "sigma_matcher": settings.flink_parallelism,
        "kafka_sink": settings.flink_parallelism,
        "rules_source": 1,
    }

    if settings.kafka_metrics_topic:
        metrics_sink = create_kafka_sink(settings, settings.kafka_metrics_topic)
        logger.info("kafka metrics sink created", topic=settings.kafka_metrics_topic)
        (
            combined_stream.get_side_output(METRICS_OUTPUT_TAG)
            .sink_to(metrics_sink)
            .set_parallelism(settings.flink_parallelism)
            .name("Per-Rule Metrics Output (Kafka)")
        )
        sinks.append("kafka_metrics")
        parallelism_config["metrics_sink"] = settings.flink_parallelism

    logger.info(
        "pipeline built",
        input_topics=input_topics,
        input_topics_count=len(input_topics),
        sources=["events (unioned)" if len(input_topics) > 1 else "events", "rules"],
        operators=["union", "keyed_broadcast_function"]
        if len(input_topics) > 1
        else ["keyed_broadcast_function"],
        sinks=sinks,
        parallelism=parallelism_config,
    )


def run_sigma_detection_job(
    job_id: str,
    input_topics: list[str],
    output_topic: str,
    rules_topic: str | None = None,
    output_mode: str | None = None,
    metrics_topic: str | None = None,
    apply_parser_to_output_events: bool | None = None,
) -> None:
    """
    Main entry point for Sigma detection Flink job

    Args:
        job_id: Unique job identifier for rule filtering (multi-tenancy)
        input_topics: List of Kafka topics with input events (will be unioned)
        output_topic: Kafka topic for output (matched events or all)
        rules_topic: Kafka topic with Sigma rules (None = use settings default)
        output_mode: "all_events" or "matched_only" (None = use settings default)
        metrics_topic: Kafka topic for per-rule metrics (None = disabled)
        apply_parser_to_output_events: Apply parser to output events (None = use settings default)

    Settings defaults are used when CLI arguments are not provided.
    """
    # Load settings first (needed for log level)
    settings = get_settings()

    # Override settings from CLI arguments (only if explicitly provided)
    settings.job_id = job_id
    settings.kafka_input_topics = ",".join(input_topics)
    settings.kafka_output_topic = output_topic

    if rules_topic:
        settings.kafka_rules_topic = rules_topic
    if output_mode:
        settings.output_mode = output_mode
    if metrics_topic:
        settings.kafka_metrics_topic = metrics_topic
    if apply_parser_to_output_events is not None:
        settings.apply_parser_to_output_events = apply_parser_to_output_events

    # Setup structured logging
    configure_logging(
        log_level=settings.log_level,
        log_format=settings.log_format,
    )

    logger.info("startup", component="FLINK SIGMA DETECTOR")
    logger.info(
        "configuration loaded",
        log_level=settings.log_level,
        log_format=settings.log_format,
        parallelism=settings.flink_parallelism,
    )
    logger.info("rules source", source="kafka_broadcast_stream", topic=settings.kafka_rules_topic)

    # Setup Flink environment
    # Application always runs in APPLICATION mode:
    # - Locally: embedded mini-cluster (standalone script)
    # - In cluster: Flink cluster with JobManager + TaskManager (docker)
    conf = Configuration()

    # Python execution mode: use process mode for better parallelism
    # Process mode runs Python workers in separate processes on TaskManagers
    # This provides better isolation and true parallelism (avoids GIL limitations)
    conf.set_string("python.execution-mode", "process")
    logger.info(
        "flink environment created",
        mode="APPLICATION",
        python_mode="process",
        note="Python workers run in separate processes for better parallelism",
    )

    env = StreamExecutionEnvironment.get_execution_environment(conf)

    # Configure environment
    configure_flink_environment(env, settings)

    # Log Kafka configuration
    log_kafka_configuration(settings)

    # Build pipeline
    build_pipeline(env, settings)

    # Execute Flink job
    logger.info("job starting", job_name="Flink Sigma Detection")
    env.execute("Flink Sigma Detection")
