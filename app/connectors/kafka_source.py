"""Kafka source connector for Apache Flink"""

from typing import TYPE_CHECKING, Any

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

if TYPE_CHECKING:
    from app.config.settings import Settings


def create_kafka_source(
    settings: "Settings",
    topic: str,
    consumer_group_suffix: str = "",
    deserializer: Any = None,
    starting_offset_override: str | None = None,
) -> KafkaSource:
    """
    Create Kafka source with optional custom deserializer and offset override

    Args:
        settings: Application settings with Kafka configuration
        topic: Kafka topic to consume from
        consumer_group_suffix: Suffix to append to consumer group (e.g., "-events", "-rules")
        deserializer: PyFlink DeserializationSchema wrapper (e.g., SimpleStringSchema, ByteArraySchema).
                     Must be a PyFlink object that wraps a Java deserialization schema
                     (must have _j_deserialization_schema attribute containing the Java object).
                     Default: SimpleStringSchema()
        starting_offset_override: Override for starting offset ("earliest", "latest", "committed")

    Returns:
        Configured KafkaSource instance
    """
    # Get auth config in Flink/Java format (JAAS for SASL)
    auth_props = settings.get_kafka_flink_auth_config()

    # Use override if provided, otherwise use settings
    offset_config = starting_offset_override or settings.kafka_starting_offset

    if offset_config == "earliest":
        starting_offset = KafkaOffsetsInitializer.earliest()
    elif offset_config == "latest":
        starting_offset = KafkaOffsetsInitializer.latest()
    else:
        starting_offset = KafkaOffsetsInitializer.committed_offsets()

    # Use separate consumer groups for different topics
    consumer_group = f"{settings.kafka_consumer_group}{consumer_group_suffix}"

    # Use custom deserializer if provided, otherwise default to SimpleStringSchema
    if deserializer is None:
        deserializer = SimpleStringSchema()

    builder = KafkaSource.builder()
    builder.set_bootstrap_servers(settings.kafka_bootstrap_servers)
    builder.set_topics(topic)
    builder.set_group_id(consumer_group)
    builder.set_starting_offsets(starting_offset)
    builder.set_value_only_deserializer(deserializer)

    # Add authentication properties
    for key, value in auth_props.items():
        builder.set_property(key, value)

    # Add consumer-specific properties
    builder.set_property("auto.offset.reset", offset_config)
    builder.set_property("enable.auto.commit", "false")

    return builder.build()
