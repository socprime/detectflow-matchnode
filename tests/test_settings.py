"""Tests for application settings configuration."""


class TestSettings:
    """Test Settings configuration class."""

    def test_default_values(self):
        """Test that settings loads with default values from .env.example."""
        # Import inside test to ensure clean environment
        from app.config.settings import Settings

        # Create settings instance (uses .env file if present)
        settings = Settings()

        # Test Kafka defaults
        assert isinstance(settings.kafka_bootstrap_servers, str)
        assert settings.kafka_auth_method in ("SASL", "SSL", "PLAINTEXT")
        assert isinstance(settings.kafka_consumer_group, str)
        assert settings.kafka_starting_offset in ("earliest", "latest", "committed")

        # Test Flink defaults
        assert isinstance(settings.flink_parallelism, int)
        assert settings.flink_parallelism >= 1
        assert isinstance(settings.flink_checkpoint_interval_ms, int)
        assert settings.flink_checkpoint_interval_ms >= 1000

        # Test windowing defaults
        assert isinstance(settings.window_size_seconds, int)
        assert settings.window_size_seconds >= 1
        assert settings.window_type in ("time", "count")

        # Test state backend defaults (Field default is rocksdb; .env may override)
        assert settings.state_backend in ("memory", "rocksdb")
        assert Settings.model_fields["state_backend"].default == "rocksdb"
        assert isinstance(settings.checkpoint_path, str)

        # Test keying strategy defaults
        assert Settings.model_fields["keying_strategy"].default == "balanced"
        assert settings.keying_strategy in ("balanced", "hash")
        assert settings.key_groups_per_task >= 1

        # Test logging defaults
        assert settings.log_level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        assert settings.log_format in ("console", "json")

    def test_kafka_input_topics_list(self):
        """Test that kafka_input_topics can be parsed to a list."""
        from app.config.settings import Settings

        settings = Settings()

        # Should be a string
        assert isinstance(settings.kafka_input_topics, str)

        # Can be split into topics
        topics = settings.kafka_input_topics.split(",")
        assert len(topics) >= 1
        assert all(isinstance(t, str) for t in topics)

    def test_output_mode_values(self):
        """Test output_mode validation."""
        from app.config.settings import Settings

        settings = Settings()
        assert settings.output_mode in ("all_events", "matched_only", "unmatched_only")

    def test_destination_accepts_matrix(self):
        """`unmatched_only` is `all_events` minus the matches — pin the whole table."""
        from app.config.settings import destination_accepts

        assert (
            destination_accepts("all_events", True),
            destination_accepts("all_events", False),
        ) == (
            True,
            True,
        )
        assert (
            destination_accepts("matched_only", True),
            destination_accepts("matched_only", False),
        ) == (
            True,
            False,
        )
        assert (
            destination_accepts("unmatched_only", True),
            destination_accepts("unmatched_only", False),
        ) == (False, True)

    def test_get_output_destinations_accepts_unmatched_only(self, monkeypatch):
        import json

        from app.config.settings import Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv(
            "KAFKA_OUTPUT_DESTINATIONS",
            json.dumps([{"topic": "noise", "output_mode": "unmatched_only"}]),
        )

        destinations = Settings().get_output_destinations()

        assert destinations[0].output_mode == "unmatched_only"

    def test_get_output_destinations_severities(self, monkeypatch):
        """``severities`` parses from JSON; absent means no filter; unknown level rejected."""
        import json

        import pytest

        from app.config.settings import Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv(
            "KAFKA_OUTPUT_DESTINATIONS",
            json.dumps(
                [
                    {"topic": "hot", "severities": ["high", "critical", "high"]},
                    {"topic": "all"},
                ]
            ),
        )
        destinations = Settings().get_output_destinations()
        assert destinations[0].severities == frozenset({"high", "critical"})
        assert destinations[1].severities == frozenset()

        monkeypatch.setenv(
            "KAFKA_OUTPUT_DESTINATIONS",
            json.dumps(
                [
                    {"topic": "all", "output_mode": "all_events", "severities": ["high"]},
                    {"topic": "noise", "output_mode": "unmatched_only", "severities": ["high"]},
                ]
            ),
        )
        # Only matched_only filters by severity; other modes drop the setting.
        assert [d.severities for d in Settings().get_output_destinations()] == [frozenset()] * 2

        monkeypatch.setenv(
            "KAFKA_OUTPUT_DESTINATIONS", json.dumps([{"topic": "x", "severities": ["urgent"]}])
        )
        with pytest.raises(ValueError):
            Settings().get_output_destinations()

    def test_get_output_destinations_legacy_shim(self, monkeypatch):
        """Legacy single-topic env synthesizes one destination."""
        from app.config.settings import Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_OUTPUT_DESTINATIONS", "")
        monkeypatch.setenv("KAFKA_OUTPUT_TOPIC", "legacy-out")
        monkeypatch.setenv("OUTPUT_MODE", "all_events")
        monkeypatch.setenv("APPLY_PARSER_TO_OUTPUT_EVENTS", "true")

        settings = Settings()
        destinations = settings.get_output_destinations()
        assert len(destinations) == 1
        assert destinations[0].topic == "legacy-out"
        assert destinations[0].output_mode == "all_events"
        assert destinations[0].apply_parser_to_output_events is True

    def test_get_output_destinations_json_preferred(self, monkeypatch):
        """KAFKA_OUTPUT_DESTINATIONS wins over legacy singular vars."""
        import json

        from app.config.settings import Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_OUTPUT_TOPIC", "legacy-out")
        monkeypatch.setenv("OUTPUT_MODE", "all_events")
        monkeypatch.setenv(
            "KAFKA_OUTPUT_DESTINATIONS",
            json.dumps(
                [
                    {
                        "topic": "dest-a",
                        "output_mode": "matched_only",
                        "apply_parser_to_output_events": False,
                    },
                    {
                        "topic": "dest-b",
                        "output_mode": "all_events",
                        "apply_parser_to_output_events": True,
                    },
                ]
            ),
        )

        settings = Settings()
        destinations = settings.get_output_destinations()
        assert [d.topic for d in destinations] == ["dest-a", "dest-b"]
        assert destinations[0].output_mode == "matched_only"
        assert destinations[1].apply_parser_to_output_events is True

    def test_get_output_destinations_rejects_duplicates(self, monkeypatch):
        import json

        import pytest

        from app.config.settings import Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv(
            "KAFKA_OUTPUT_DESTINATIONS",
            json.dumps(
                [
                    {"topic": "same", "output_mode": "matched_only"},
                    {"topic": "same", "output_mode": "all_events"},
                ]
            ),
        )

        settings = Settings()
        with pytest.raises(ValueError, match="duplicate topics"):
            settings.get_output_destinations()

    def test_get_output_destinations_rejects_empty_array(self, monkeypatch):
        import pytest

        from app.config.settings import Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_OUTPUT_DESTINATIONS", "[]")

        settings = Settings()
        with pytest.raises(ValueError, match="at least one destination"):
            settings.get_output_destinations()

    def test_get_output_destinations_rejects_empty_topic(self, monkeypatch):
        import json

        import pytest

        from app.config.settings import Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv(
            "KAFKA_OUTPUT_DESTINATIONS",
            json.dumps([{"topic": "  ", "output_mode": "matched_only"}]),
        )

        settings = Settings()
        with pytest.raises(ValueError, match="topic must be non-empty"):
            settings.get_output_destinations()

    def test_get_output_destinations_rejects_more_than_max(self, monkeypatch):
        import json

        import pytest

        from app.config.settings import MAX_OUTPUT_DESTINATIONS, Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv(
            "KAFKA_OUTPUT_DESTINATIONS",
            json.dumps(
                [
                    {"topic": f"dest-{i}", "output_mode": "matched_only"}
                    for i in range(MAX_OUTPUT_DESTINATIONS + 1)
                ]
            ),
        )

        settings = Settings()
        with pytest.raises(ValueError, match="at most 10 destinations"):
            settings.get_output_destinations()

    def test_keep_filtered_events_default(self):
        """Test keep_filtered_events default value."""
        from app.config.settings import Settings

        settings = Settings()
        assert settings.keep_filtered_events is False

    def test_kafka_tagged_events_topic_default_is_empty(self):
        """Fail closed: a deployment that loses KAFKA_TAGGED_EVENTS_TOPIC must not
        fall back to a shared topic.

        Asserted on the field default rather than on an instance, because a local
        .env would otherwise decide the outcome of this test.
        """
        from app.config.settings import Settings

        default = Settings.model_fields["kafka_tagged_events_topic"].default
        assert isinstance(default, str)
        assert default == ""

    def test_rocksdb_settings(self):
        """Test RocksDB configuration settings."""
        from app.config.settings import Settings

        settings = Settings()

        assert isinstance(settings.rocksdb_writebuffer_size_mb, int)
        assert settings.rocksdb_writebuffer_size_mb >= 8

        assert isinstance(settings.rocksdb_writebuffer_count, int)
        assert settings.rocksdb_writebuffer_count >= 2

        assert isinstance(settings.rocksdb_block_cache_size_mb, int)
        assert settings.rocksdb_block_cache_size_mb >= 8

        assert settings.rocksdb_compaction_style in ("LEVEL", "UNIVERSAL", "FIFO")

    def test_watermark_settings(self):
        """Test watermark configuration settings."""
        from app.config.settings import Settings

        settings = Settings()

        assert isinstance(settings.enable_watermarks, bool)
        assert isinstance(settings.watermark_out_of_orderness_seconds, int)
        assert isinstance(settings.watermark_idle_timeout_seconds, int)

    def test_kafka_flink_producer_properties_defaults(self):
        """Flink sink producer knobs: batching defaults + compression from setting."""
        from app.config.settings import Settings

        settings = Settings()
        props = settings.get_kafka_flink_producer_properties()

        assert props["acks"] == "all"
        assert props["linger.ms"] == "50"
        assert props["batch.size"] == "262144"
        assert props["buffer.memory"] == str(16 * 1024 * 1024)
        assert props["compression.type"] == settings.kafka_producer_compression_type

        producer = settings.get_kafka_producer_config()
        assert producer["compression.type"] == settings.kafka_producer_compression_type
        assert producer["linger.ms"] == props["linger.ms"]

    def test_kafka_flink_producer_properties_compression_none(self, monkeypatch):
        from app.config.settings import Settings

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_PRODUCER_COMPRESSION_TYPE", "none")

        props = Settings().get_kafka_flink_producer_properties()
        assert "compression.type" not in props


class TestSettingsValidation:
    """Test settings validation."""

    def test_parallelism_bounds(self):
        """Test that parallelism has valid bounds."""
        from app.config.settings import Settings

        settings = Settings()

        # Default parallelism should be within bounds
        assert 1 <= settings.flink_parallelism <= 100

    def test_checkpoint_intervals(self):
        """Test checkpoint interval configurations."""
        from app.config.settings import Settings

        settings = Settings()

        # Checkpoint interval should be at least 1 second
        assert settings.flink_checkpoint_interval_ms >= 1000

        # Checkpoint timeout should be at least 10 seconds
        assert settings.flink_checkpoint_timeout_ms >= 10000

        # Min pause should be non-negative
        assert settings.flink_min_pause_between_checkpoints_ms >= 0
