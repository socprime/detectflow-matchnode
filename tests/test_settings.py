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

        # Test state backend defaults
        assert settings.state_backend in ("memory", "rocksdb")
        assert isinstance(settings.checkpoint_path, str)

        # Test logging defaults
        assert settings.log_level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        assert settings.log_format in ("console", "json")

        # Test keying strategy defaults
        assert settings.keying_strategy in ("hash", "computer", "round_robin")
        assert isinstance(settings.key_groups_per_task, int)
        assert settings.key_groups_per_task >= 1

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
        assert settings.output_mode in ("all_events", "matched_only")

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


class TestSettingsValidation:
    """Test settings validation."""

    def test_parallelism_bounds(self):
        """Test that parallelism has valid bounds."""
        from app.config.settings import Settings

        settings = Settings()

        # Default parallelism should be within bounds
        assert 1 <= settings.flink_parallelism <= 100

        # Key groups per task should be within bounds
        assert 1 <= settings.key_groups_per_task <= 100

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
