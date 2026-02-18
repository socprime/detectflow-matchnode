"""Unit tests for FieldMapping class."""

from app.domain.sigma_matcher.field_mapping import FieldMapping


def test_get_field_name_in_event_no_mapping():
    """Test get_field_name_in_event when no mapping exists."""
    fm = FieldMapping({})
    assert fm.get_field_name_in_event("EventID") == ["EventID"]
    assert fm.get_field_name_in_event("CommandLine") == ["CommandLine"]


def test_get_field_name_in_event_with_mapping():
    """Test get_field_name_in_event when mapping exists."""
    fm = FieldMapping({"EventID": ["winlog.event_id"]})
    # Should return mapped field + original field (fallback)
    fields = fm.get_field_name_in_event("EventID")
    assert "winlog.event_id" in fields
    assert "EventID" in fields
    assert len(fields) == 2


def test_get_field_name_in_event_original_in_mapping():
    """Test that original field is not duplicated when already in mapping."""
    fm = FieldMapping({"field1": ["field1", "field1_additional"]})
    fields = fm.get_field_name_in_event("field1")
    # Should return the mapping as-is (no duplication)
    assert fields == ["field1", "field1_additional"]
    assert fields.count("field1") == 1


def test_get_field_name_in_event_multiple_mapped_fields():
    """Test get_field_name_in_event with multiple mapped fields."""
    fm = FieldMapping({"CommandLine": ["process.command_line", "process.args"]})
    fields = fm.get_field_name_in_event("CommandLine")
    # Should return all mapped fields + original
    assert "process.command_line" in fields
    assert "process.args" in fields
    assert "CommandLine" in fields
    assert len(fields) == 3


def test_get_field_name_in_event_fallback_behavior():
    """Test that original field is always included for fallback."""
    # Mapping to a field that might not exist
    fm = FieldMapping({"EventID": ["winlog.event_id"]})
    fields = fm.get_field_name_in_event("EventID")

    # Original field should always be present for fallback
    assert "EventID" in fields
    assert "winlog.event_id" in fields

    # Order: mapped fields first, then original
    assert fields[0] == "winlog.event_id"
    assert fields[1] == "EventID"


def test_get_field_name_in_event_empty_mapping():
    """Test get_field_name_in_event with empty mapping dict."""
    fm = FieldMapping({})
    assert fm.get_field_name_in_event("AnyField") == ["AnyField"]


def test_get_field_name_in_event_multiple_sigma_fields():
    """Test get_field_name_in_event for different sigma fields."""
    fm = FieldMapping(
        {
            "EventID": ["winlog.event_id"],
            "CommandLine": ["process.command_line"],
        }
    )

    event_fields = fm.get_field_name_in_event("EventID")
    assert set(event_fields) == {"winlog.event_id", "EventID"}

    command_fields = fm.get_field_name_in_event("CommandLine")
    assert set(command_fields) == {"process.command_line", "CommandLine"}

    # Unmapped field should return just itself
    unmapped = fm.get_field_name_in_event("UnmappedField")
    assert unmapped == ["UnmappedField"]
