"""Unit tests for LogsourceConfig and logsources loader."""

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

    def test_non_dict_falls_back_to_empty_mapping(self):
        """Non-dict YAML root falls back to empty FieldMapping."""
        config = LogsourceConfig(field_mapping_str="not a dict")
        assert config.field_mapping.field_mapping == {}

    def test_invalid_value_type_falls_back_to_empty_mapping(self):
        """Invalid value type (e.g. int) falls back to empty FieldMapping."""
        config = LogsourceConfig(field_mapping_str="bad_key: 123")
        assert config.field_mapping.field_mapping == {}

    def test_invalid_list_item_type_falls_back_to_empty_mapping(self):
        """Non-string item in list falls back to empty FieldMapping."""
        config = LogsourceConfig(field_mapping_str="k:\n  - a\n  - 42")
        assert config.field_mapping.field_mapping == {}

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

    def test_yaml_only_comments_falls_back_to_empty_mapping(self):
        """YAML with only comment lines parses to None, falls back to empty FieldMapping."""
        yaml_str = "# field1: value1\n# field2: value2"
        config = LogsourceConfig(field_mapping_str=yaml_str)
        assert config.field_mapping.field_mapping == {}

    def test_invalid_mapping_preserves_parser_config(self):
        """Invalid mapping should not discard the parser_config."""
        parser = {"type": "syslog", "pattern": ".*"}
        config = LogsourceConfig(parser_config=parser, field_mapping_str="not a dict")
        assert config.parser_config == parser
        assert config.field_mapping.field_mapping == {}


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

    def test_event_with_parser_config(self):
        """Event with parser.config is extracted correctly."""
        parser_cfg = {"type": "syslog", "pattern": ".*"}
        event = {"parser": {"config": parser_cfg}}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == parser_cfg
        assert config.field_mapping.field_mapping == {}

    def test_event_with_parser_config_and_valid_mapping(self):
        """Both parser config and valid mapping are preserved."""
        parser_cfg = {"type": "syslog", "pattern": ".*"}
        mapping = "EventID: winlog.event_id\nSourceName:\n  - source\n  - provider"
        event = {"parser": {"config": parser_cfg}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == parser_cfg
        assert config.field_mapping.field_mapping == {
            "EventID": ["winlog.event_id"],
            "SourceName": ["source", "provider"],
        }

    def test_event_with_parser_config_and_bad_mapping_preserves_parser(self):
        """Invalid mapping must not discard the valid parser_config."""
        parser_cfg = {"type": "syslog", "pattern": ".*"}
        event = {"parser": {"config": parser_cfg}, "mapping": "# some bad staff"}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == parser_cfg
        assert config.field_mapping.field_mapping == {}

    def test_event_with_parser_config_and_non_dict_mapping_preserves_parser(self):
        """Non-dict mapping string falls back to empty mapping, parser_config kept."""
        parser_cfg = {"type": "json"}
        event = {"parser": {"config": parser_cfg}, "mapping": "just a string"}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == parser_cfg
        assert config.field_mapping.field_mapping == {}

    def test_event_with_parser_config_and_invalid_yaml_mapping_preserves_parser(self):
        """Malformed YAML mapping falls back to empty mapping, parser_config kept."""
        parser_cfg = {"type": "json"}
        event = {"parser": {"config": parser_cfg}, "mapping": ":::bad yaml{["}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == parser_cfg
        assert config.field_mapping.field_mapping == {}

    def test_event_with_no_parser_key(self):
        """Event without 'parser' key results in parser_config=None."""
        event = {"mapping": "field: value"}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config is None
        assert config.field_mapping.field_mapping == {"field": ["value"]}

    def test_event_with_empty_parser_config(self):
        """Event with parser key but no config yields parser_config=None."""
        event = {"parser": {}, "mapping": "field: value"}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config is None
        assert config.field_mapping.field_mapping == {"field": ["value"]}

    def test_event_with_none_mapping(self):
        """Explicit None mapping yields empty field_mapping."""
        parser_cfg = {"type": "json"}
        event = {"parser": {"config": parser_cfg}, "mapping": None}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == parser_cfg
        assert config.field_mapping.field_mapping == {}


class TestConvertKafkaEventMappingWithComments:
    """Tests for convert_kafka_event_to_logsource_config with YAML comments in mapping."""

    PARSER_CFG = {"some": "parser_config"}

    def test_mapping_with_leading_comment(self):
        """Leading comment before valid entries is ignored."""
        mapping = "# Windows field mapping\nEventID: winlog.event_id"
        event = {"parser": {"config": self.PARSER_CFG}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == self.PARSER_CFG
        assert config.field_mapping.field_mapping == {"EventID": ["winlog.event_id"]}

    def test_mapping_with_trailing_comment(self):
        """Trailing comment after valid entries is ignored."""
        mapping = "EventID: winlog.event_id\n# end of mapping"
        event = {"parser": {"config": self.PARSER_CFG}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == self.PARSER_CFG
        assert config.field_mapping.field_mapping == {"EventID": ["winlog.event_id"]}

    def test_mapping_with_comments_between_entries(self):
        """Comments between valid entries are ignored, all entries parsed."""
        mapping = (
            "EventID: winlog.event_id\n"
            "# SourceName is mapped to two fields\n"
            "SourceName:\n"
            "  - source\n"
            "  - provider"
        )
        event = {"parser": {"config": self.PARSER_CFG}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == self.PARSER_CFG
        assert config.field_mapping.field_mapping == {
            "EventID": ["winlog.event_id"],
            "SourceName": ["source", "provider"],
        }

    def test_mapping_with_inline_comments(self):
        """Inline comments after values are stripped by YAML parser."""
        mapping = "EventID: winlog.event_id  # primary ID\nChannel: winlog.channel  # log channel"
        event = {"parser": {"config": self.PARSER_CFG}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == self.PARSER_CFG
        assert config.field_mapping.field_mapping == {
            "EventID": ["winlog.event_id"],
            "Channel": ["winlog.channel"],
        }

    def test_mapping_with_commented_out_entries(self):
        """Commented-out entries are excluded, only active entries are kept."""
        mapping = (
            "EventID: winlog.event_id\n"
            "# SourceName: winlog.provider  # disabled\n"
            "Channel: winlog.channel"
        )
        event = {"parser": {"config": self.PARSER_CFG}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == self.PARSER_CFG
        assert config.field_mapping.field_mapping == {
            "EventID": ["winlog.event_id"],
            "Channel": ["winlog.channel"],
        }
        assert "SourceName" not in config.field_mapping.field_mapping

    def test_mapping_with_commented_list_items(self):
        """Commented items in a list are excluded."""
        mapping = "SourceName:\n  - source\n  # - old_provider\n  - provider"
        event = {"parser": {"config": self.PARSER_CFG}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == self.PARSER_CFG
        assert config.field_mapping.field_mapping == {
            "SourceName": ["source", "provider"],
        }

    def test_mapping_only_comments_preserves_parser(self):
        """Mapping that is entirely comments falls back to empty mapping, parser kept."""
        mapping = "# all entries commented out\n# EventID: winlog.event_id"
        event = {"parser": {"config": self.PARSER_CFG}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == self.PARSER_CFG
        assert config.field_mapping.field_mapping == {}

    def test_mapping_multiline_comment_block(self):
        """Multi-line comment block followed by valid entries."""
        mapping = (
            "# === Field Mapping Configuration ===\n"
            "# Version: 1.0\n"
            "# Author: admin\n"
            "#\n"
            "EventID: winlog.event_id\n"
            "Channel: winlog.channel"
        )
        event = {"parser": {"config": self.PARSER_CFG}, "mapping": mapping}
        config = convert_kafka_event_to_logsource_config(event)
        assert config.parser_config == self.PARSER_CFG
        assert config.field_mapping.field_mapping == {
            "EventID": ["winlog.event_id"],
            "Channel": ["winlog.channel"],
        }
