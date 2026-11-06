"""Kafka sink connector for Apache Flink"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink

if TYPE_CHECKING:
    from app.config.settings import Settings


def _apply_sink_properties(
    set_property: Callable[[str, str], object], settings: "Settings"
) -> None:
    """Auth + producer knobs shared by fixed-topic and topic-selector sinks."""
    for key, value in settings.get_kafka_flink_auth_config().items():
        set_property(key, value)
    # linger/batch/buffer/compression — Settings.get_kafka_flink_producer_properties
    for key, value in settings.get_kafka_flink_producer_properties().items():
        set_property(key, value)


def create_kafka_sink(settings: "Settings", topic: str | None = None) -> KafkaSink:
    """
    Create a fixed-topic Kafka sink.

    Args:
        settings: Application settings with Kafka configuration
        topic: Destination topic. Prefer passing explicitly for each output
            destination / side output. Falls back to legacy
            ``settings.kafka_output_topic`` when omitted.

    Returns:
        Configured KafkaSink instance
    """
    target_topic = topic if topic is not None else settings.kafka_output_topic
    if not target_topic:
        raise ValueError("topic is required when creating a Kafka sink")

    serialization_schema = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(target_topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    builder = KafkaSink.builder()
    builder.set_bootstrap_servers(settings.kafka_bootstrap_servers)
    builder.set_record_serializer(serialization_schema)
    _apply_sink_properties(builder.set_property, settings)
    return builder.build()


def create_multi_topic_kafka_sink(settings: "Settings") -> KafkaSink:
    """Create a Kafka sink that routes ``ROW<topic, json>`` records to ``row[0]``.

    Pipeline destinations share **one** ``KafkaProducer`` per subtask (one gzip/lz4
    pipeline, one ``buffer.memory``). Tagged-events and metrics stay on
    :func:`create_kafka_sink` — different schemas and volumes.

    Why not ``set_topic_selector(callable)``: PyFlink wraps that path in a Python
    ``Map`` (``Row(selector(v), v)``) plus ``SecondColumnSerializationSchema``. If
    ``v`` is already ``ROW<topic, json>``, the Kafka value would be the inner Row
    rather than the JSON string. Building the Java first-column selector + second-
    column string serializer ourselves keeps the Kafka value as the event JSON.

    Stream type must be ``Types.ROW([Types.STRING(), Types.STRING()])``.
    Ref: Flink ``PythonConnectorUtils.createFirstColumnTopicSelector`` /
    ``SecondColumnSerializationSchema`` (flink-python).
    """
    # Py4J / Flink Python runtime — only available when the job runs under Flink.
    from py4j.java_gateway import get_java_class
    from pyflink.java_gateway import get_gateway

    jvm = get_gateway().jvm
    python_utils = jvm.org.apache.flink.python.util.PythonConnectorUtils
    topic_selector_iface = jvm.org.apache.flink.connector.kafka.sink.TopicSelector

    j_serializer_builder = (
        jvm.org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder()
    )
    j_serializer_builder.setTopicSelector(
        python_utils.createFirstColumnTopicSelector(get_java_class(topic_selector_iface))
    )
    j_serializer_builder.setValueSerializationSchema(
        python_utils.SecondColumnSerializationSchema(SimpleStringSchema()._j_serialization_schema)
    )

    j_sink_builder = jvm.org.apache.flink.connector.kafka.sink.KafkaSink.builder()
    j_sink_builder.setBootstrapServers(settings.kafka_bootstrap_servers)
    j_sink_builder.setRecordSerializer(j_serializer_builder.build())
    _apply_sink_properties(j_sink_builder.setProperty, settings)
    # transformer=None: skip PyFlink's topic-selector preprocessing Map.
    return KafkaSink(j_sink_builder.build(), None)
