"""Kafka sink connector for Apache Flink"""

from typing import TYPE_CHECKING

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink

if TYPE_CHECKING:
    from app.config.settings import Settings


def create_kafka_sink(settings: "Settings", topic: str | None = None) -> KafkaSink:
    """
    Create Kafka sink for producing detection results

    Args:
        settings: Application settings with Kafka configuration
        topic: Override topic (defaults to settings.kafka_output_topic)

    Returns:
        Configured KafkaSink instance
    """
    # Use provided topic or fall back to settings
    target_topic = topic if topic is not None else settings.kafka_output_topic

    # Get auth config in Flink/Java format (JAAS for SASL)
    auth_props = settings.get_kafka_flink_auth_config()

    serialization_schema = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(target_topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    builder = KafkaSink.builder()
    builder.set_bootstrap_servers(settings.kafka_bootstrap_servers)
    builder.set_record_serializer(serialization_schema)

    # Add authentication properties
    for key, value in auth_props.items():
        builder.set_property(key, value)

    # Add producer-specific properties
    builder.set_property("acks", "all")
    builder.set_property("compression.type", "gzip")

    return builder.build()
