"""Sigma detection Flink job - Pipeline configuration and execution"""

import json
from datetime import timedelta
from pathlib import Path

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.common.serialization import ByteArraySchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway

from app.config.logging import configure_logging, get_logger
from app.config.settings import Settings, get_settings
from app.connectors.kafka_sink import create_kafka_sink, create_multi_topic_kafka_sink
from app.connectors.kafka_source import create_kafka_source
from app.domain.keying.strategies import (
    calculate_hash_bucket_count,
    compute_subtask_keys,
    create_balanced_key_extractor,
    create_hash_key_extractor,
)
from app.jobs.runtime_tuning import apply_companion_config
from app.operators.sigma_broadcast import (
    DETECTIONS_OUTPUT_TAG,
    METRICS_OUTPUT_TAG,
    RULES_STATE_DESCRIPTOR,
    TAGGED_EVENTS_OUTPUT_TAG,
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

    # Configure State Backend (+ companion memory / checkpoint knobs)
    if settings.state_backend == "rocksdb":
        # RocksDB state backend with incremental checkpoints
        config.setString("state.backend.type", "rocksdb")
        config.setString("state.backend.rocksdb.localdir", "/tmp/flink-rocksdb")

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
            localdir="/tmp/flink-rocksdb",
            writebuffer_size_mb=settings.rocksdb_writebuffer_size_mb,
            writebuffer_count=settings.rocksdb_writebuffer_count,
            block_cache_size_mb=settings.rocksdb_block_cache_size_mb,
            block_size_kb=settings.rocksdb_block_size_kb,
            compaction_style=settings.rocksdb_compaction_style,
            thread_num=settings.rocksdb_thread_num,
        )
    else:
        # Hashmap state backend (in-memory heap ListState; no incremental checkpoints)
        config.setString("state.backend.type", "hashmap")
        logger.info("state backend configured", backend="hashmap")

    # Managed fraction, incremental checkpoints, jvm-overhead, network buffers — follow
    # STATE_BACKEND + PYTHON_EXECUTION_MODE so flipping either switch keeps memory coherent.
    # Ref: app/jobs/runtime_tuning.py
    apply_companion_config(config, settings)

    # Configure Checkpoint Storage
    config.setString("state.checkpoints.dir", settings.checkpoint_path)
    logger.info("checkpoint storage configured", path=settings.checkpoint_path)

    # Configure Parallelism
    env.set_parallelism(settings.flink_parallelism)

    # Number of key groups (maxParallelism). The deterministic keying (see build_pipeline) routes
    # exactly one balanced bucket per subtask using Flink's KeyGroupRangeAssignment, which needs
    # the SAME maxParallelism the keyed operator actually runs with.
    #
    # In production the Kubernetes operator freezes pipeline.max-parallelism in the cluster config
    # (and into every checkpoint). We MUST NOT override it here: forcing a different value makes
    # last-state / savepoint restore fail with a "Max parallelism mismatch", and the operator then
    # tears down and redeploys the cluster in a loop. So we only set it ourselves when the
    # deployment did not (e.g. local / docker standalone), keeping keying deterministic there.
    configured_max_parallelism = config.getString("pipeline.max-parallelism", None)
    if configured_max_parallelism is not None:
        max_parallelism = int(configured_max_parallelism)
        logger.info(
            "max parallelism from cluster config",
            max_parallelism=max_parallelism,
            note="pipeline.max-parallelism set by deployment; not overriding",
        )
    else:
        max_parallelism = settings.flink_max_parallelism
        env.set_max_parallelism(max_parallelism)
        logger.info(
            "max parallelism set from settings",
            max_parallelism=max_parallelism,
            source="FLINK_MAX_PARALLELISM",
        )

    if max_parallelism < settings.flink_parallelism:
        raise ValueError(
            f"maxParallelism ({max_parallelism}) must be >= "
            f"flink_parallelism ({settings.flink_parallelism})"
        )
    logger.info(
        "parallelism configured",
        parallelism=settings.flink_parallelism,
        max_parallelism=max_parallelism,
    )

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
    destinations = settings.get_output_destinations()
    logger.info(
        "kafka configuration",
        events_topics=input_topics_list,
        events_topics_count=len(input_topics_list),
        events_consumer_group=f"{settings.kafka_consumer_group}-events",
        events_starting_offset=settings.kafka_starting_offset,
        rules_topic=settings.kafka_rules_topic,
        rules_consumer_group=f"{settings.kafka_consumer_group}-rules",
        rules_starting_offset="earliest",  # Always earliest for compacted rules topic
        output_destinations=[
            {
                "topic": d.topic,
                "output_mode": d.output_mode,
                "apply_parser_to_output_events": d.apply_parser_to_output_events,
                "severities": sorted(d.severities),
            }
            for d in destinations
        ],
        tagged_output_topic=settings.kafka_tagged_events_topic or None,
        tagged_events_enabled=settings.tagged_events_enabled(),
        window_size_seconds=settings.window_size_seconds,
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

    destinations = settings.get_output_destinations()
    if (settings.kafka_output_destinations or "").strip():
        logger.warning(
            "KAFKA_OUTPUT_DESTINATIONS is set; ignoring legacy "
            "KAFKA_OUTPUT_TOPIC / OUTPUT_MODE / APPLY_PARSER_TO_OUTPUT_EVENTS"
        )

    # One detections KafkaSink for every destination (topic selector on ROW<topic, json>).
    # Tagged-events / metrics stay fixed-topic — different schemas and volumes.
    detections_sink = create_multi_topic_kafka_sink(settings)
    for dest in destinations:
        logger.info(
            "kafka destination registered",
            topic=dest.topic,
            output_mode=dest.output_mode,
            apply_parser_to_output_events=dest.apply_parser_to_output_events,
        )
    logger.info(
        "kafka detections sink created",
        destination_count=len(destinations),
        topics=[d.topic for d in destinations],
    )
    emit_tagged_events = settings.tagged_events_enabled()

    # Create watermark strategy
    watermark_strategy = create_watermark_strategy(settings)

    # Build event stream with keying
    # IMPORTANT: Keying enables true horizontal scaling across parallel tasks
    # Each key will be processed by a separate task instance
    #
    # KEYING_STRATEGY selects the extractor (see app/domain/keying/strategies.py):
    # - balanced (default): one evenly-filled LONG bucket per subtask via Flink routing
    # - hash: legacy oversharding — parallelism × KEY_GROUPS_PER_TASK string keys (hash-N)
    # Do not restore a savepoint across strategies (keyed state space differs).
    parallelism = settings.flink_parallelism
    keying_strategy = settings.keying_strategy

    if keying_strategy == "hash":
        num_buckets = calculate_hash_bucket_count(parallelism, settings.key_groups_per_task)
        key_extractor = create_hash_key_extractor(num_buckets)
        logger.info(
            "hash keying configured",
            strategy="hash",
            parallelism=parallelism,
            key_groups_per_task=settings.key_groups_per_task,
            total_key_buckets=num_buckets,
        )
    else:
        # Use the SAME maxParallelism the keyed operator will actually run with.
        # configure_flink_environment resolves it from pipeline.max-parallelism (frozen by the
        # deployment) or FLINK_MAX_PARALLELISM (local).
        config = env._j_stream_execution_environment.getConfiguration()
        configured_max_parallelism = config.getString("pipeline.max-parallelism", None)
        max_parallelism = (
            int(configured_max_parallelism)
            if configured_max_parallelism is not None
            else settings.flink_max_parallelism
        )
        subtask_keys = compute_subtask_keys(get_gateway(), parallelism, max_parallelism)
        key_extractor = create_balanced_key_extractor(subtask_keys)
        logger.info(
            "balanced keying configured",
            strategy="balanced",
            parallelism=parallelism,
            max_parallelism=max_parallelism,
            buckets=len(subtask_keys),
            subtask_keys=subtask_keys,
        )

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

    # Key the combined stream for parallel processing.
    # balanced: key_type=LONG so routing keys match compute_subtask_keys (Row(Long)).
    # hash: default string keys (hash-N).
    if keying_strategy == "hash":
        events_stream = combined_events_stream.key_by(key_extractor)
    else:
        events_stream = combined_events_stream.key_by(key_extractor, key_type=Types.LONG())
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
    matcher_fn = SigmaMatcherBroadcastFunction(
        window_size_seconds=settings.window_size_seconds,
        window_count_threshold=settings.window_count_threshold,
        job_id=settings.job_id,
        destinations=destinations,
        keep_filtered_events=settings.keep_filtered_events,
        emit_tagged_events=emit_tagged_events,
    )
    combined_stream = (
        events_stream.connect(rules_stream)
        .process(
            matcher_fn,
            output_type=Types.STRING(),
        )
        .set_parallelism(settings.flink_parallelism)
        .name("Sigma Detection Processor")
    )

    # Sink detections to Kafka (one producer; topic comes from ROW field 0)
    (
        combined_stream.get_side_output(DETECTIONS_OUTPUT_TAG)
        .sink_to(detections_sink)
        .set_parallelism(settings.flink_parallelism)
        .name("Detections Output (Kafka)")
    )
    sinks = [f"kafka_detections:{d.topic}" for d in destinations]
    parallelism_config = {
        "kafka_source": settings.kafka_source_parallelism or settings.flink_parallelism,
        "sigma_matcher": settings.flink_parallelism,
        "kafka_sink": settings.flink_parallelism,
        "rules_source": 1,
        "detections_sinks": 1,
        "destination_topics": len(destinations),
    }

    # Tagged-events side output -> correlation. Skipped entirely (branch + sink)
    # when the control plane left KAFKA_TAGGED_EVENTS_TOPIC empty.
    if emit_tagged_events:
        tagged_events_sink = create_kafka_sink(settings, settings.kafka_tagged_events_topic)
        logger.info("kafka tagged events sink created", topic=settings.kafka_tagged_events_topic)
        (
            combined_stream.get_side_output(TAGGED_EVENTS_OUTPUT_TAG)
            .sink_to(tagged_events_sink)
            .set_parallelism(settings.flink_parallelism)
            .name("Tagged Events Output (Kafka)")
        )
        sinks.append("kafka_tagged_events")
        parallelism_config["tagged_events_sink"] = settings.flink_parallelism
    else:
        logger.info(
            "tagged events sink skipped",
            reason="KAFKA_TAGGED_EVENTS_TOPIC is empty",
            note="pipeline is not consumed by any correlation pipeline",
        )

    # Sink per-rule metrics to Kafka (if metrics topic is configured)
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
    output_topic: str | None = None,
    output_destinations: str | list | None = None,
    rules_topic: str | None = None,
    output_mode: str | None = None,
    keep_filtered_events: bool | None = None,
    metrics_topic: str | None = None,
    flink_parallelism: int | None = None,
    kafka_source_parallelism: int | None = None,
    window_size_seconds: int | None = None,
    apply_parser_to_output_events: bool | None = None,
) -> None:
    """
    Main entry point for Sigma detection Flink job

    Args:
        job_id: Unique job identifier for rule filtering (multi-tenancy)
        input_topics: List of Kafka topics with input events (will be unioned)
        output_topic: Legacy single Kafka output topic (shim when destinations unset)
        output_destinations: JSON string or list of destination dicts (preferred)
        rules_topic: Kafka topic with Sigma rules (None = use settings default)
        output_mode: Legacy "all_events" / "matched_only" / "unmatched_only" (shim when destinations unset)
        keep_filtered_events: Keep prefiltered events in output (None = use settings default)
        metrics_topic: Kafka topic for per-rule metrics (None = disabled)
        flink_parallelism: Parallelism for matcher and sink operators (None = use settings default)
        kafka_source_parallelism: Parallelism for Kafka sources (None = use settings default)
        window_size_seconds: Processing window size in seconds (None = use settings default)
        apply_parser_to_output_events: Legacy parser-on-output flag (shim when destinations unset)

    Settings defaults are used when CLI arguments are not provided.
    """
    # Load settings first (needed for log level)
    settings = get_settings()

    # Override settings from CLI arguments (only if explicitly provided)
    settings.job_id = job_id
    settings.kafka_input_topics = ",".join(input_topics)
    if output_destinations is not None:
        if isinstance(output_destinations, list):
            settings.kafka_output_destinations = json.dumps(output_destinations)
        else:
            settings.kafka_output_destinations = output_destinations
    if output_topic:
        settings.kafka_output_topic = output_topic

    if rules_topic:
        settings.kafka_rules_topic = rules_topic
    if output_mode:
        settings.output_mode = output_mode
    if keep_filtered_events is not None:
        settings.keep_filtered_events = keep_filtered_events
    if metrics_topic:
        settings.kafka_metrics_topic = metrics_topic
    if flink_parallelism is not None:
        settings.flink_parallelism = flink_parallelism
    if kafka_source_parallelism is not None:
        settings.kafka_source_parallelism = kafka_source_parallelism
    if window_size_seconds is not None:
        settings.window_size_seconds = window_size_seconds
    if apply_parser_to_output_events is not None:
        settings.apply_parser_to_output_events = apply_parser_to_output_events

    # Fail fast on invalid destination config before building the Flink graph
    settings.get_output_destinations()

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

    # Python execution mode (settings.python_execution_mode):
    # - "process" (default, RECOMMENDED): separate Python worker processes (isolation)
    # - "thread": Python embedded in TaskManager JVM via Pemja (lower overhead; less
    #   stable under load — see Dockerfile on_timer sed + native crash risk)
    conf.set_string("python.execution-mode", settings.python_execution_mode)
    logger.info(
        "flink environment created",
        mode="APPLICATION",
        python_mode=settings.python_execution_mode,
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
