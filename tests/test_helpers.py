from unittest.mock import MagicMock, patch

import orjson
import pytest

from app.domain.filters.loader import Filter
from app.domain.sigma_matcher.constants import FULL_EVENT_COLUMN_NAME
from app.domain.sigma_matcher.field_mapping import FieldMapping
from app.domain.sigma_matcher.helpers import (
    SCHEMA_PARSER_ERROR_FIELD,
    convert_events_to_df,
    parse_events_with_schema_parser,
    prepare_df,
    process_events,
)
from app.domain.sigma_matcher.sigma_parser import Sigma, SigmaNotSupported


def test_empty_events():
    # Test with empty list
    events = []
    df = convert_events_to_df(events, used_fields=[])
    assert hasattr(df, "columns")
    assert len(df) == 0


def test_simple_events():
    # Test with simple list of dictionaries
    events = [{"id": 1, "name": "event1"}, {"id": 2, "name": "event2"}]
    df = convert_events_to_df(events, used_fields=["id", "name"])

    assert hasattr(df, "columns")
    assert len(df) == 2
    assert set(df.columns) == {"id", "name"}
    assert df["id"].to_list() == ["1", "2"]
    assert df["name"].to_list() == ["event1", "event2"]


def test_full_event_column_passthrough():
    # New behavior: helper doesn't auto-add _raw; it passes through if provided in events
    event1 = {"id": 1, "name": "event1", FULL_EVENT_COLUMN_NAME: orjson.dumps({"a": 1}).decode()}
    event2 = {"id": 2, "name": "event2", FULL_EVENT_COLUMN_NAME: orjson.dumps({"b": 2}).decode()}
    events = [event1, event2]
    df = convert_events_to_df(events, used_fields=["id", "name", FULL_EVENT_COLUMN_NAME])

    assert FULL_EVENT_COLUMN_NAME in df.columns
    assert df[FULL_EVENT_COLUMN_NAME].to_list() == [
        event1[FULL_EVENT_COLUMN_NAME],
        event2[FULL_EVENT_COLUMN_NAME],
    ]


def test_events_with_none_values():
    # Test with None values to verify fillna behavior
    events = [{"id": 1, "name": None}, {"id": 2, "name": "event2"}, {"id": 3, "name": ""}]
    df = convert_events_to_df(events, used_fields=["id", "name"])

    assert df["name"].to_list() == ["", "event2", ""]


def test_events_with_mixed_types():
    # Test with different data types to verify astype(str) behavior
    events = [
        {"id": 1, "value": 100, "flag": True, "rate": 3.14},
        {"id": 2, "value": 200, "flag": False, "rate": 2.71},
    ]
    df = convert_events_to_df(events, used_fields=["id", "value", "flag", "rate"])

    # Check if all values are strings
    for col in df.columns:
        assert "object" in str(df[col].dtype) or "str" in str(df[col].dtype).lower()

    # Note: values are now lowercased during conversion for performance
    assert df["value"].to_list() == ["100", "200"]
    assert df["flag"].to_list() == ["true", "false"]
    assert df["rate"].to_list() == ["3.14", "2.71"]


# ============================================================================
# Tests for prefiltering logic in process_events
# ============================================================================


@pytest.fixture
def field_mapping():
    """Field mapping for tests that use EventID in Sigma rules."""
    return FieldMapping({"EventID": ["winlog.event_id"]})


@pytest.fixture
def simple_sigma_rule():
    """Create a simple Sigma rule that matches EventID 4624."""
    yaml_text = """
title: Test Rule
id: TEST001
status: test
detection:
    selection:
        EventID: 4624
    condition: selection
"""
    return Sigma(text=yaml_text, case_id="rule_001")


@pytest.fixture
def filter_sigma_rule():
    """Create a filter rule that matches EventID 4625."""
    yaml_text = """
title: Filter Rule
id: FILTER001
status: test
detection:
    selection:
        EventID: 4625
    condition: selection
"""
    return Sigma(text=yaml_text, case_id="filter_001")


def test_process_events_no_filters(simple_sigma_rule, field_mapping):
    """Test process_events without filters - should work normally."""
    events = [
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user1"},
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user2"},
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user3"},
    ]
    sigmas = [simple_sigma_rule]

    result = process_events(
        events=events, sigmas=sigmas, filters=None, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    # Should have 3 results (one per event)
    assert len(matches) == 3
    # Events 0 and 2 should match (EventID 4624)
    assert matches[0] == ["rule_001"]
    assert matches[1] == []  # EventID 4625 doesn't match
    assert matches[2] == ["rule_001"]
    assert result.prefiltered_mask is None


def test_process_events_with_filters_no_matches(
    simple_sigma_rule, filter_sigma_rule, field_mapping
):
    """Test process_events with filters that don't match any events."""
    events = [
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user1"},
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user2"},
    ]
    sigmas = [simple_sigma_rule]
    # Filter matches EventID 4625, but events have 4624
    filters = [Filter(id="filter_001", body=filter_sigma_rule._text)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    # All events should be evaluated since no filters matched
    assert len(matches) == 2
    assert matches[0] == ["rule_001"]
    assert matches[1] == ["rule_001"]


def test_process_events_with_filters_some_matches(
    simple_sigma_rule, filter_sigma_rule, field_mapping
):
    """Test process_events with filters that match some events - those should be excluded."""
    events = [
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user1",
        },  # Matches sigma, not filter
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user2",
        },  # Matches filter, should be excluded
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user3",
        },  # Matches sigma, not filter
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user4",
        },  # Matches filter, should be excluded
    ]
    sigmas = [simple_sigma_rule]
    filters = [Filter(id="filter_001", body=filter_sigma_rule._text)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    # Should have 4 results (one per original event)
    assert len(matches) == 4
    # Events 0 and 2 should match sigma (EventID 4624, not filtered)
    assert matches[0] == ["rule_001"]
    # Events 1 and 3 should be empty (filtered out, so not evaluated)
    assert matches[1] == []
    assert matches[2] == ["rule_001"]
    assert matches[3] == []


def test_process_events_with_filters_all_matches(filter_sigma_rule, field_mapping):
    """Test process_events when all events match filters - should return empty results."""
    events = [
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user1"},
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user2"},
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user3"},
    ]
    # Create a sigma that would match if not filtered
    sigma_yaml = """
title: Test Rule
id: TEST001
status: test
detection:
    selection:
        EventID: 4625
    condition: selection
"""
    sigmas = [Sigma(text=sigma_yaml, case_id="rule_001")]
    filters = [Filter(id="filter_001", body=filter_sigma_rule._text)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )

    matches = result.case_ids_per_event

    # All events were filtered out, so all results should be empty
    assert len(matches) == 3
    assert matches[0] == []
    assert matches[1] == []
    assert matches[2] == []


def test_process_events_with_filters_result_mapping(
    simple_sigma_rule, filter_sigma_rule, field_mapping
):
    """Test that results are correctly mapped back to original event positions."""
    events = [
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user1",
        },  # Position 0: matches sigma
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user2",
        },  # Position 1: filtered out
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user3",
        },  # Position 2: matches sigma
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user4",
        },  # Position 3: filtered out
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user5",
        },  # Position 4: matches sigma
    ]
    sigmas = [simple_sigma_rule]
    filters = [Filter(id="filter_001", body=filter_sigma_rule._text)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )

    matches = result.case_ids_per_event

    # Results should be in original order
    assert len(matches) == 5
    assert matches[0] == ["rule_001"]  # Original position 0
    assert matches[1] == []  # Original position 1 (filtered)
    assert matches[2] == ["rule_001"]  # Original position 2
    assert matches[3] == []  # Original position 3 (filtered)
    assert matches[4] == ["rule_001"]  # Original position 4


def test_process_events_prefiltered_mask(simple_sigma_rule, filter_sigma_rule, field_mapping):
    """Test that prefiltered_mask correctly marks which events were excluded by prefilters."""
    events = [
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user1"},  # Not filtered
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user2"},  # Filtered
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user3"},  # Not filtered
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user4"},  # Filtered
    ]
    sigmas = [simple_sigma_rule]
    filters = [Filter(id="filter_001", body=filter_sigma_rule._text)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )

    assert result.prefiltered_mask is not None
    assert result.prefiltered_mask == [False, True, False, True]


def test_process_events_prefiltered_mask_none_without_filters(simple_sigma_rule, field_mapping):
    """Test that prefiltered_mask is None when no filters are applied."""
    events = [
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user1"},
    ]
    result = process_events(
        events=events, sigmas=[simple_sigma_rule], filters=None, field_mapping=field_mapping
    )
    assert result.prefiltered_mask is None


def test_process_events_prefiltered_mask_no_matches(simple_sigma_rule, filter_sigma_rule, field_mapping):
    """Test prefiltered_mask when filters don't match any events."""
    events = [
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user1"},
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user2"},
    ]
    filters = [Filter(id="filter_001", body=filter_sigma_rule._text)]

    result = process_events(
        events=events, sigmas=[simple_sigma_rule], filters=filters, field_mapping=field_mapping
    )

    assert result.prefiltered_mask == [False, False]


def test_process_events_multiple_filters(simple_sigma_rule, field_mapping):
    """Test process_events with multiple filters."""
    # Create two different filter rules
    filter1_yaml = """
title: Filter 1
id: FILTER001
status: test
detection:
    selection:
        EventID: 4625
    condition: selection
"""
    filter2_yaml = """
title: Filter 2
id: FILTER002
status: test
detection:
    selection:
        EventID: 4626
    condition: selection
"""
    events = [
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user1",
        },  # No filter match, matches sigma
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user2",
        },  # Matches filter1, filtered out
        {
            "winlog.event_id": 4626,
            "winlog.event_data.TargetUserName": "user3",
        },  # Matches filter2, filtered out
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user4",
        },  # No filter match, matches sigma
    ]
    sigmas = [simple_sigma_rule]
    filters = [
        Filter(id="filter_001", body=filter1_yaml),
        Filter(id="filter_002", body=filter2_yaml),
    ]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    assert len(matches) == 4
    assert matches[0] == ["rule_001"]  # Not filtered, matches sigma
    assert matches[1] == []  # Filtered by filter1
    assert matches[2] == []  # Filtered by filter2
    assert matches[3] == ["rule_001"]  # Not filtered, matches sigma


def test_process_events_filters_and_sigmas_both_match(field_mapping):
    """Test when both filters and sigmas match different events."""
    sigma_yaml = """
title: Sigma Rule
id: SIGMA001
status: test
detection:
    selection:
        EventID: 4624
    condition: selection
"""
    filter_yaml = """
title: Filter Rule
id: FILTER001
status: test
detection:
    selection:
        EventID: 4625
    condition: selection
"""
    events = [
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user1",
        },  # Matches sigma only
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user2",
        },  # Matches filter only (excluded)
        {"winlog.event_id": 4626, "winlog.event_data.TargetUserName": "user3"},  # Matches neither
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="sigma_001")]
    filters = [Filter(id="filter_001", body=filter_yaml)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    assert len(matches) == 3
    assert matches[0] == ["sigma_001"]  # Matches sigma, not filtered
    assert matches[1] == []  # Matches filter, excluded from evaluation
    assert matches[2] == []  # Matches neither


def test_process_events_empty_events_with_filters(filter_sigma_rule, field_mapping):
    """Test process_events with empty events list and filters."""
    events = []
    sigmas = []
    filters = [Filter(id="filter_001", body=filter_sigma_rule._text)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    assert len(matches) == 0


def test_process_events_empty_filters_list(simple_sigma_rule, field_mapping):
    """Test process_events with empty filters list - should work like no filters."""
    events = [
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "user1"},
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user2"},
    ]
    sigmas = [simple_sigma_rule]
    filters = []

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    assert len(matches) == 2
    assert matches[0] == ["rule_001"]
    assert matches[1] == []


def test_process_events_sigma_matches_filter_excludes(field_mapping):
    """Test where sigma matches events 1 and 2, but filter excludes event 2.

    Scenario:
    - 4 events total (indices 0, 1, 2, 3)
    - Sigma matches events at indices 0 and 1 (EventID 4624 or 4625)
    - Filter matches event at index 1 (EventID 4625) and excludes it
    - Final result: only event at index 0 should be matched
    """
    # Create a sigma that matches EventID 4624 OR 4625
    sigma_yaml = """
title: Test Rule
id: TEST001
status: test
detection:
    selection1:
        EventID: 4624
    selection2:
        EventID: 4625
    condition: 1 of selection*
"""

    # Create a filter that matches EventID 4625
    filter_yaml = """
title: Filter Rule
id: FILTER001
status: test
detection:
    selection:
        EventID: 4625
    condition: selection
"""

    events = [
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user1",
        },  # Index 0: matches sigma (4624), not filter
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user2",
        },  # Index 1: matches sigma (4625) AND filter -> filtered out
        {
            "winlog.event_id": 4626,
            "winlog.event_data.TargetUserName": "user3",
        },  # Index 2: matches neither
        {
            "winlog.event_id": 4627,
            "winlog.event_data.TargetUserName": "user4",
        },  # Index 3: matches neither
    ]

    sigmas = [Sigma(text=sigma_yaml, case_id="rule_001")]
    filters = [Filter(id="filter_001", body=filter_yaml)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    # Verify results
    assert len(matches) == 4
    assert matches[0] == ["rule_001"]  # Event 0: matches sigma (4624), not filtered
    assert matches[1] == []  # Event 1: matches sigma (4625) but filtered out
    assert matches[2] == []  # Event 2: matches neither
    assert matches[3] == []  # Event 3: matches neither


# ============================================================================
# Tests for full event matching using keywords condition
# ============================================================================


def test_keywords_single_keyword_match():
    """Test keywords condition with a single keyword that matches."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS001
status: test
detection:
    keywords:
        - "suspicious_process"
    condition: keywords
"""
    events = [
        {"process.name": "cmd.exe", "user.name": "admin", "message": "suspicious_process detected"},
        {"process.name": "notepad.exe", "user.name": "user1"},
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_001")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 2
    assert matches[0] == ["keywords_001"]  # Contains "suspicious_process"
    assert matches[1] == []  # Doesn't contain keyword


def test_keywords_multiple_keywords_or():
    """Test keywords condition with multiple keywords (OR condition)."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS002
status: test
detection:
    keywords:
        - "malware"
        - "trojan"
        - "virus"
    condition: keywords
"""
    events = [
        {"process.name": "cmd.exe", "message": "malware detected"},  # Matches first keyword
        {"process.name": "notepad.exe", "message": "trojan found"},  # Matches second keyword
        {"process.name": "calc.exe", "message": "virus alert"},  # Matches third keyword
        {"process.name": "mspaint.exe", "message": "normal operation"},  # No match
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_002")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 4
    assert matches[0] == ["keywords_002"]  # Contains "malware"
    assert matches[1] == ["keywords_002"]  # Contains "trojan"
    assert matches[2] == ["keywords_002"]  # Contains "virus"
    assert matches[3] == []  # No keyword match


def test_keywords_case_insensitive():
    """Test that keywords matching is case-insensitive (values are lowercased)."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS003
status: test
detection:
    keywords:
        - "suspicious"
    condition: keywords
"""
    events = [
        {"process.name": "cmd.exe", "message": "SUSPICIOUS activity"},  # Uppercase
        {"process.name": "notepad.exe", "message": "Suspicious behavior"},  # Mixed case
        {"process.name": "calc.exe", "message": "suspicious event"},  # Lowercase
        {"process.name": "mspaint.exe", "message": "normal operation"},  # No match
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_003")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 4
    assert matches[0] == ["keywords_003"]  # Uppercase matches (lowercased)
    assert matches[1] == ["keywords_003"]  # Mixed case matches (lowercased)
    assert matches[2] == ["keywords_003"]  # Lowercase matches
    assert matches[3] == []  # No keyword match


def test_keywords_nested_fields():
    """Test that keywords can match in nested fields within the full event JSON."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS004
status: test
detection:
    keywords:
        - "admin"
    condition: keywords
"""
    events = [
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "admin",
            "winlog.event_data.LogonType": 2,
        },  # Keyword in nested field
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user1",
            "winlog.event_data.LogonType": 3,
        },  # No keyword
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_004")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 2
    assert matches[0] == ["keywords_004"]  # Contains "admin" in nested field
    assert matches[1] == []  # Doesn't contain keyword


def test_keywords_special_characters():
    """Test keywords with special characters and numbers."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS005
status: test
detection:
    keywords:
        - "system32"
        - "192.168.1.1"
        - "user@domain.com"
    condition: keywords
"""
    events = [
        {
            "process.path": "C:\\Windows\\System32\\cmd.exe"
        },  # Contains "system32" (lowercased in JSON)
        {"source.ip": "192.168.1.1", "destination.ip": "10.0.0.1"},  # Matches second keyword
        {"user.email": "user@domain.com"},  # Matches third keyword
        {"process.path": "C:\\Program Files\\app.exe"},  # No match
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_005")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 4
    assert matches[0] == ["keywords_005"]  # Contains "system32" (lowercased in JSON)
    assert matches[1] == ["keywords_005"]  # Contains IP
    assert matches[2] == ["keywords_005"]  # Contains email
    assert matches[3] == []  # No keyword match


def test_keywords_partial_match():
    """Test that keywords use contains matching (partial matches)."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS006
status: test
detection:
    keywords:
        - "admin"
    condition: keywords
"""
    events = [
        {"user.name": "administrator"},  # Contains "admin"
        {"user.name": "admin_user"},  # Contains "admin"
        {"user.name": "admin"},  # Exact match
        {"user.name": "admind"},  # Contains "admin"
        {"user.name": "user1"},  # No match
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_006")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 5
    assert matches[0] == ["keywords_006"]  # "administrator" contains "admin"
    assert matches[1] == ["keywords_006"]  # "admin_user" contains "admin"
    assert matches[2] == ["keywords_006"]  # Exact match
    assert matches[3] == ["keywords_006"]  # "admind" contains "admin"
    assert matches[4] == []  # No match


def test_keywords_no_match():
    """Test keywords condition when no keywords match."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS007
status: test
detection:
    keywords:
        - "malware"
        - "trojan"
    condition: keywords
"""
    events = [
        {"process.name": "notepad.exe", "message": "normal operation"},
        {"process.name": "calc.exe", "message": "user opened calculator"},
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_007")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 2
    assert matches[0] == []  # No keyword match
    assert matches[1] == []  # No keyword match


def test_keywords_with_filters(field_mapping):
    """Test keywords condition with filters applied."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS008
status: test
detection:
    keywords:
        - "suspicious"
    condition: keywords
"""
    filter_yaml = """
title: Filter Rule
id: FILTER001
status: test
detection:
    selection:
        EventID: 4625
    condition: selection
"""
    events = [
        {
            "winlog.event_id": 4624,
            "message": "suspicious activity detected",
        },  # Matches sigma, not filter
        {
            "winlog.event_id": 4625,
            "message": "suspicious activity detected",
        },  # Matches sigma and filter -> filtered out
        {
            "winlog.event_id": 4624,
            "message": "normal operation",
        },  # No match
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_008")]
    filters = [Filter(id="filter_001", body=filter_yaml)]

    result = process_events(
        events=events, sigmas=sigmas, filters=filters, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    assert len(matches) == 3
    assert matches[0] == ["keywords_008"]  # Matches sigma, not filtered
    assert matches[1] == []  # Matches filter, excluded from evaluation
    assert matches[2] == []  # No match


def test_keywords_with_field_selection(field_mapping):
    """Test keywords condition combined with field-based selection."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS009
status: test
detection:
    selection:
        EventID: 4624
    keywords:
        - "admin"
    condition: selection and keywords
"""
    events = [
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "admin",
        },  # Matches both selection and keywords
        {
            "winlog.event_id": 4624,
            "winlog.event_data.TargetUserName": "user1",
        },  # Matches selection but not keywords
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "admin",
        },  # Matches keywords but not selection
        {
            "winlog.event_id": 4625,
            "winlog.event_data.TargetUserName": "user1",
        },  # Matches neither
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_009")]

    result = process_events(
        events=events, sigmas=sigmas, filters=None, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event

    assert len(matches) == 4
    assert matches[0] == ["keywords_009"]  # Matches both
    assert matches[1] == []  # Matches selection only
    assert matches[2] == []  # Matches keywords only
    assert matches[3] == []  # Matches neither


def test_keywords_empty_keywords_list():
    """Test keywords condition with empty keywords list - should raise error."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS010
status: test
detection:
    keywords: []
    condition: keywords
"""
    # Empty keywords list is not supported and should raise an error
    with pytest.raises(SigmaNotSupported):
        _ = [Sigma(text=sigma_yaml, case_id="keywords_010")]


def test_keywords_json_structure():
    """Test that keywords can match values in the JSON structure of the full event."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS011
status: test
detection:
    keywords:
        - "4624"
        - "logon"
    condition: keywords
"""
    events = [
        {
            "winlog.event_id": 4624,
            "winlog.event_data.LogonType": 2,
            "message": "successful logon",
        },  # Contains both "4624" and "logon"
        {
            "winlog.event_id": 4625,
            "winlog.event_data.LogonType": 3,
            "message": "failed authentication",
        },  # Contains "4624" (as substring of "4625") but not "logon" - should match due to OR logic
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_011")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 2
    assert matches[0] == ["keywords_011"]  # Contains both keywords
    assert matches[1] == ["keywords_011"]  # Contains "4624" as substring of "4625" (OR condition)


def test_keywords_match_across_nested_structures():
    """Test that keywords can match across deeply nested structures in the full event JSON."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS016
status: test
detection:
    keywords:
        - "powershell"
        - "bypass"
    condition: keywords
"""
    events = [
        {
            "winlog.event_id": 4688,
            "process.name": "powershell.exe",
            "process.parent.name": "cmd.exe",
            "process.command_line": "powershell -ExecutionPolicy Bypass -Command ...",
            "user.name": "admin",
        },
        {
            "winlog.event_id": 4688,
            "process.name": "notepad.exe",
            "process.command_line": "normal operation",
        },
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_016")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 2
    assert matches[0] == ["keywords_016"]  # Contains both keywords in full event
    assert matches[1] == []  # No keyword match


def test_keywords_case_insensitive_in_full_event():
    """Test that keywords matching in full event is case-insensitive."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS017
status: test
detection:
    keywords:
        - "suspicious"
    condition: keywords
"""
    events = [
        {
            "winlog.event_id": 4624,
            "message": "SUSPICIOUS activity detected",  # Uppercase
            "process.name": "cmd.exe",
        },
        {
            "winlog.event_id": 4625,
            "message": "Suspicious behavior",  # Mixed case
            "process.name": "notepad.exe",
        },
        {
            "winlog.event_id": 4626,
            "message": "suspicious event",  # Lowercase
            "process.name": "calc.exe",
        },
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_017")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 3
    # All should match because the full event JSON is lowercased
    assert matches[0] == ["keywords_017"]  # Uppercase in original, but lowercased in JSON
    assert matches[1] == ["keywords_017"]  # Mixed case, but lowercased in JSON
    assert matches[2] == ["keywords_017"]  # Already lowercase


def test_keywords_literal_strings_in_full_event():
    """Test that keywords match as literal strings in the full event JSON."""
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS018
status: test
detection:
    keywords:
        - "administrator"
        - "api_token"
    condition: keywords
"""
    events = [
        {
            "user.name": "administrator",
            "process.name": "cmd.exe",
        },
        {
            "config.api_token": "abc123",
            "process.name": "notepad.exe",
        },
        {
            "user.name": "user1",
            "process.name": "calc.exe",
        },
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_018")]

    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event

    assert len(matches) == 3
    assert matches[0] == ["keywords_018"]  # Contains "administrator"
    assert matches[1] == ["keywords_018"]  # Contains "api_token"
    assert matches[2] == []  # No keyword match


def test_full_event_column_not_added_when_not_needed(field_mapping):
    """Test that __raw_event column is NOT added to DataFrame when Sigma rule doesn't require it."""
    # Create a Sigma rule that uses field-based selection (not keywords)
    sigma_yaml = """
title: Field-based Test Rule
id: FIELD001
status: test
detection:
    selection:
        EventID: 4624
    condition: selection
"""
    events = [
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "admin"},
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user1"},
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="field_001")]

    # Get used fields from the sigma rule
    used_fields = list(sigmas[0].get_event_fields(field_mapping))

    # Verify that __raw_event is NOT in used_fields
    assert FULL_EVENT_COLUMN_NAME not in used_fields

    # Prepare DataFrame
    df = prepare_df(events=events, used_fields=used_fields)

    # Verify that __raw_event is NOT in the DataFrame columns
    assert FULL_EVENT_COLUMN_NAME not in df.columns

    # Verify that the rule still works correctly
    result = process_events(
        events=events, sigmas=sigmas, filters=None, field_mapping=field_mapping
    )
    matches = result.case_ids_per_event
    assert len(matches) == 2
    assert matches[0] == ["field_001"]  # Matches EventID 4624
    assert matches[1] == []  # Doesn't match


def test_full_event_column_added_when_keywords_used():
    """Test that __raw_event column IS added to DataFrame when Sigma rule uses keywords."""
    # Create a Sigma rule that uses keywords (requires __raw_event)
    sigma_yaml = """
title: Keywords Test Rule
id: KEYWORDS019
status: test
detection:
    keywords:
        - "admin"
    condition: keywords
"""
    events = [
        {"winlog.event_id": 4624, "winlog.event_data.TargetUserName": "admin"},
        {"winlog.event_id": 4625, "winlog.event_data.TargetUserName": "user1"},
    ]
    sigmas = [Sigma(text=sigma_yaml, case_id="keywords_019")]

    # Get used fields from the sigma rule
    fm = FieldMapping({})
    used_fields = list(sigmas[0].get_event_fields(fm))

    # Verify that __raw_event IS in used_fields
    assert FULL_EVENT_COLUMN_NAME in used_fields

    # Prepare DataFrame
    df = prepare_df(events=events, used_fields=used_fields)

    # Verify that __raw_event IS in the DataFrame columns
    assert FULL_EVENT_COLUMN_NAME in df.columns

    # Verify that the rule works correctly
    result = process_events(events=events, sigmas=sigmas, filters=None)
    matches = result.case_ids_per_event
    assert len(matches) == 2
    assert matches[0] == ["keywords_019"]  # Contains "admin"
    assert matches[1] == []  # Doesn't contain keyword


# ============================================================================
# Tests for parse_events_with_schema_parser error handling
# ============================================================================


class TestParseEventsWithSchemaParser:
    """Tests for parse_events_with_schema_parser function."""

    def test_no_parser_dict_returns_events_unchanged(self):
        """When parser_dict is None, events should be returned unchanged."""
        events = [{"id": 1, "data": "test"}, {"id": 2, "data": "test2"}]

        result, error_count = parse_events_with_schema_parser(events, parser_dict=None)

        assert result is events  # Same object reference
        assert error_count == 0

    @patch("app.domain.sigma_matcher.helpers.ParserManager")
    def test_successful_parsing(self, mock_parser_manager_class):
        """When parsing succeeds, parsed events are returned."""
        events = [{"raw": "data1"}, {"raw": "data2"}]
        parser_dict = {"steps": ["parse_json"], "args": {}}

        # Mock successful parsing
        mock_manager = MagicMock()
        mock_manager.configured_parser.side_effect = [
            {"parsed": "result1"},
            {"parsed": "result2"},
        ]
        mock_parser_manager_class.return_value = mock_manager

        result, error_count = parse_events_with_schema_parser(events, parser_dict)

        assert len(result) == 2
        assert result[0] == {"parsed": "result1"}
        assert result[1] == {"parsed": "result2"}
        assert error_count == 0
        assert mock_manager.configured_parser.call_count == 2

    @patch("app.domain.sigma_matcher.helpers.ParserManager")
    def test_failed_parsing_adds_error_field(self, mock_parser_manager_class):
        """When parsing fails, original event is returned with error flag."""
        events = [{"raw": "bad_data"}]
        parser_dict = {"steps": ["parse_json"], "args": {}}

        # Mock failed parsing
        mock_manager = MagicMock()
        mock_manager.configured_parser.side_effect = ValueError("Parse error")
        mock_parser_manager_class.return_value = mock_manager

        result, error_count = parse_events_with_schema_parser(events, parser_dict)

        assert len(result) == 1
        assert result[0]["raw"] == "bad_data"  # Original data preserved
        assert result[0][SCHEMA_PARSER_ERROR_FIELD] is True
        assert error_count == 1

    @patch("app.domain.sigma_matcher.helpers.ParserManager")
    def test_mixed_success_and_failure(self, mock_parser_manager_class):
        """Mix of successful and failed parsing."""
        events = [
            {"raw": "good1"},
            {"raw": "bad"},
            {"raw": "good2"},
            {"raw": "also_bad"},
        ]
        parser_dict = {"steps": ["parse_json"], "args": {}}

        # Mock mixed results
        mock_manager = MagicMock()
        mock_manager.configured_parser.side_effect = [
            {"parsed": "result1"},  # Success
            ValueError("Parse error 1"),  # Fail
            {"parsed": "result2"},  # Success
            KeyError("missing key"),  # Fail
        ]
        mock_parser_manager_class.return_value = mock_manager

        result, error_count = parse_events_with_schema_parser(events, parser_dict)

        assert len(result) == 4
        # First event: success
        assert result[0] == {"parsed": "result1"}
        assert SCHEMA_PARSER_ERROR_FIELD not in result[0]
        # Second event: failure
        assert result[1]["raw"] == "bad"
        assert result[1][SCHEMA_PARSER_ERROR_FIELD] is True
        # Third event: success
        assert result[2] == {"parsed": "result2"}
        assert SCHEMA_PARSER_ERROR_FIELD not in result[2]
        # Fourth event: failure
        assert result[3]["raw"] == "also_bad"
        assert result[3][SCHEMA_PARSER_ERROR_FIELD] is True

        assert error_count == 2

    @patch("app.domain.sigma_matcher.helpers.ParserManager")
    def test_all_events_fail(self, mock_parser_manager_class):
        """All events fail parsing."""
        events = [{"raw": "bad1"}, {"raw": "bad2"}, {"raw": "bad3"}]
        parser_dict = {"steps": ["parse_json"], "args": {}}

        mock_manager = MagicMock()
        mock_manager.configured_parser.side_effect = Exception("All fail")
        mock_parser_manager_class.return_value = mock_manager

        result, error_count = parse_events_with_schema_parser(events, parser_dict)

        assert len(result) == 3
        assert all(e[SCHEMA_PARSER_ERROR_FIELD] is True for e in result)
        assert error_count == 3

    @patch("app.domain.sigma_matcher.helpers.ParserManager")
    def test_original_event_not_mutated(self, mock_parser_manager_class):
        """Original event should not be mutated when parsing fails."""
        original_event = {"raw": "data", "other": "field"}
        events = [original_event]
        parser_dict = {"steps": ["parse_json"], "args": {}}

        mock_manager = MagicMock()
        mock_manager.configured_parser.side_effect = ValueError("error")
        mock_parser_manager_class.return_value = mock_manager

        result, _ = parse_events_with_schema_parser(events, parser_dict)

        # Original event should not have the error field
        assert SCHEMA_PARSER_ERROR_FIELD not in original_event
        # Result should have the error field
        assert SCHEMA_PARSER_ERROR_FIELD in result[0]

    def test_empty_events_list(self):
        """Empty events list should return empty list."""
        events = []
        parser_dict = {"steps": ["parse_json"], "args": {}}

        result, error_count = parse_events_with_schema_parser(events, parser_dict)

        assert result == []
        assert error_count == 0
