"""Unit tests for LogsourceConfig and logsources loader."""

import pytest

from app.domain.logsources.loader import LogsourceConfig, convert_kafka_event_to_logsource_config


class TestGetFieldMappingFromStr:
    """Tests for LogsourceConfig._get_field_mapping_from_str (via LogsourceConfig)."""

    def test_empty_or_none_field_mapping_str_returns_empty_mapping(self):
        """None or empty string yields empty FieldMapping."""
        config = LogsourceConfig(field_mapping_str=None)
        assert config.field_mapping.field_mapping == {}

        config = LogsourceConfig(field_mapping_str="")
        assert config.field_mapping.field_mapping == {}

    def test_valid_string_value(self):
        """Single string value becomes single-element list."""
        config = LogsourceConfig(field_mapping_str="sigma_field: event_field")
        assert config.field_mapping.field_mapping == {"sigma_field": ["event_field"]}

    def test_valid_list_value(self):
        """List value is stored as list."""
        config = LogsourceConfig(field_mapping_str="sigma_field:\n  - a\n  - b")
        assert config.field_mapping.field_mapping == {"sigma_field": ["a", "b"]}

    def test_empty_string_value_skipped(self):
        """Empty string value for a key is skipped (key not added)."""
        config = LogsourceConfig(field_mapping_str='valid_key: x\nempty_key: ""\nanother: y')
        assert config.field_mapping.field_mapping == {
            "valid_key": ["x"],
            "another": ["y"],
        }
        assert "empty_key" not in config.field_mapping.field_mapping

    def test_empty_list_value_skipped(self):
        """Empty list value for a key is skipped (key not added)."""
        config = LogsourceConfig(field_mapping_str="valid_key: x\nempty_list: []\nanother: y")
        assert config.field_mapping.field_mapping == {
            "valid_key": ["x"],
            "another": ["y"],
        }
        assert "empty_list" not in config.field_mapping.field_mapping

    def test_none_value_skipped(self):
        """None value for a key is skipped (key not added)."""
        config = LogsourceConfig(field_mapping_str="valid_key: x\nnull_key: null\nanother: y")
        assert config.field_mapping.field_mapping == {
            "valid_key": ["x"],
            "another": ["y"],
        }
        assert "null_key" not in config.field_mapping.field_mapping

    def test_list_with_empty_string_items_skips_empty(self):
        """List items that are empty strings are skipped."""
        config = LogsourceConfig(field_mapping_str='sigma_field:\n  - a\n  - ""\n  - b\n  - ""')
        assert config.field_mapping.field_mapping == {"sigma_field": ["a", "b"]}

    def test_list_with_mixed_empty_and_non_empty(self):
        """Only non-empty string items are kept in list values."""
        config = LogsourceConfig(field_mapping_str='key:\n  - first\n  - ""\n  - second')
        assert config.field_mapping.field_mapping == {"key": ["first", "second"]}

    def test_non_dict_raises(self):
        """Non-dict YAML root raises ValueError."""
        with pytest.raises(ValueError, match="field mapping must be a dictionary"):
            LogsourceConfig(field_mapping_str="not a dict")

    def test_invalid_value_type_raises(self):
        """Invalid value type (e.g. int) for a key raises ValueError."""
        with pytest.raises(ValueError, match="invalid field mapping for key bad_key"):
            LogsourceConfig(field_mapping_str="bad_key: 123")

    def test_invalid_list_item_type_raises(self):
        """Non-string item in list raises ValueError."""
        with pytest.raises(ValueError, match="invalid field mapping for key k"):
            LogsourceConfig(field_mapping_str="k:\n  - a\n  - 42")

    def test_key_with_no_value_trailing_colon_skipped(self):
        """Key with colon but no value (parses as null) is skipped."""
        yaml_str = "field1: value1\nfield2:\n"
        config = LogsourceConfig(field_mapping_str=yaml_str)
        assert config.field_mapping.field_mapping == {"field1": ["value1"]}
        assert "field2" not in config.field_mapping.field_mapping

    def test_commented_line_not_in_mapping(self):
        """Commented-out key: value line is not in the mapping."""
        yaml_str = "field1: value1\n# field2: ignored\nfield3: value3"
        config = LogsourceConfig(field_mapping_str=yaml_str)
        assert config.field_mapping.field_mapping == {
            "field1": ["value1"],
            "field3": ["value3"],
        }
        assert "field2" not in config.field_mapping.field_mapping

    def test_inline_comment_stripped_from_value(self):
        """Inline comment after value does not become part of the value."""
        yaml_str = "field1: value1  # inline comment"
        config = LogsourceConfig(field_mapping_str=yaml_str)
        assert config.field_mapping.field_mapping == {"field1": ["value1"]}

    def test_commented_item_in_list_skipped(self):
        """Commented list item is not included in the mapping."""
        yaml_str = "field:\n  - a\n  # - b (commented out)\n  - c"
        config = LogsourceConfig(field_mapping_str=yaml_str)
        assert config.field_mapping.field_mapping == {"field": ["a", "c"]}

    def test_yaml_only_comments_raises(self):
        """YAML with only comment lines parses to None and raises (not a dict)."""
        yaml_str = "# field1: value1\n# field2: value2"
        with pytest.raises(ValueError, match="field mapping must be a dictionary"):
            LogsourceConfig(field_mapping_str=yaml_str)


class TestConvertKafkaEventToLogsourceConfig:
    """Tests for convert_kafka_event_to_logsource_config."""

    def test_none_event_returns_default_config(self):
        """None event yields LogsourceConfig with defaults."""
        config = convert_kafka_event_to_logsource_config(None)
        assert config.parser_config is None
        assert config.field_mapping.field_mapping == {}

    def test_empty_event_returns_default_config(self):
        """Empty dict event yields LogsourceConfig with defaults."""
        config = convert_kafka_event_to_logsource_config({})
        assert config.parser_config is None
        assert config.field_mapping.field_mapping == {}

    def test_event_with_mapping(self):
        """Event with mapping string is parsed into field_mapping."""
        event = {"mapping": "EventID: winlog.event_id"}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.field_mapping.field_mapping == {"EventID": ["winlog.event_id"]}
