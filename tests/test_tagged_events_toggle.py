"""The tagged-events side output is gated on a non-empty topic name.

Empty means the control plane assigned this detection pipeline to no correlation
pipeline, so neither the sink nor the per-event JSON document is built.
"""

from app.config.settings import Settings


def _settings(topic: str) -> Settings:
    return Settings(kafka_tagged_events_topic=topic)


def test_disabled_for_empty_topic():
    assert _settings("").tagged_events_enabled() is False


def test_disabled_for_whitespace_only_topic():
    assert _settings("   ").tagged_events_enabled() is False


def test_enabled_for_per_pipeline_topic():
    topic = "detectflow.tagged-events.11111111-1111-1111-1111-111111111111"
    assert _settings(topic).tagged_events_enabled() is True


def test_empty_settings_field_disables_the_side_output():
    """The wiring contract the control plane depends on: an empty injected
    KAFKA_TAGGED_EVENTS_TOPIC reaches the helper as False."""
    settings = Settings(kafka_tagged_events_topic="")
    assert settings.kafka_tagged_events_topic == ""
    assert settings.tagged_events_enabled() is False
