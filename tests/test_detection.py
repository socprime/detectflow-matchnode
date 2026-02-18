from app.domain.sigma_matcher.evaluator import Evaluator
from app.domain.sigma_matcher.field_mapping import FieldMapping
from app.domain.sigma_matcher.helpers import convert_events_to_df, prepare_df
from app.domain.sigma_matcher.sigma_parser import Sigma


def _matches(sigma: Sigma, logs: list[dict], field_mapping: FieldMapping) -> list[bool]:
    """Evaluate a single sigma rule against logs and return per-row match booleans."""
    uf = list(sigma.get_event_fields(field_mapping))
    df = convert_events_to_df(logs, used_fields=uf)
    evaluator = Evaluator(sigmas=[sigma], field_mapping=field_mapping)
    results = evaluator.evaluate(df)
    return [bool(r) for r in results]


def _matches_prepared(sigma: Sigma, logs: list[dict], field_mapping: FieldMapping) -> list[bool]:
    """Same as _matches but uses prepare_df (applies lowercasing via full pipeline)."""
    uf = list(sigma.get_event_fields(field_mapping))
    df = prepare_df(logs, used_fields=uf)
    evaluator = Evaluator(sigmas=[sigma], field_mapping=field_mapping)
    results = evaluator.evaluate(df)
    return [bool(r) for r in results]


def test_detection1():
    sigma_text = """
detection:
    condition: selection1 or (selection2 and not filter)
    selection1:
        field1: value1
    selection2:
        field2: value2
    filter:
        field3: filtered_value
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "value1", "field2": "test", "field3": "test"},  # match by selection1
        {"field1": "test", "field2": "value2", "field3": "test"},  # match by selection2
        {"field1": "test", "field2": "value2", "field3": "filtered_value"},  # filtered
        {"field1": "value1", "field2": "value2", "field3": "test"},  # match both
        {"field1": "value1", "field2": "value2", "field3": "filtered_value"},  # match sel1
        {"field1": "test", "field2": "test", "field3": "test"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches(sigma, logs, fm)
    assert result == [True, True, False, True, True, False]


def test_detection_with_null():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: null
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": None},  # match
        {"field1": "test"},  # no match
        {"field1": ""},  # match (null == empty)
        {"field1": "null"},  # no match
        {"field1": "NULL"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches(sigma, logs, fm)
    assert result == [True, False, True, False, False]


def test_detection_with_empty_string():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: ""
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": ""},  # match
        {"field1": "test"},  # no match
        {"field1": None},  # match (null == empty)
        {"field1": "null"},  # no match
        {"field1": "NULL"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches(sigma, logs, fm)
    assert result == [True, False, True, False, False]


def test_detection_with_non_existing_field_and_null():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: null
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field2": "test"},  # match (field1 missing = null)
        {"field1": "test"},  # no match
        {"field1": ""},  # match
        {"field1": None},  # match
    ]
    fm = FieldMapping({})
    result = _matches(sigma, logs, fm)
    assert result == [True, False, True, True]


def test_detection_with_non_existing_field_and_null_and_negation():
    sigma_text = """
detection:
    condition: not selection
    selection:
        field1: null
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field2": "test"},  # no match (field1 missing = null, negated)
        {"field1": "test"},  # match
        {"field1": ""},  # no match
        {"field1": None},  # no match
    ]
    fm = FieldMapping({})
    result = _matches(sigma, logs, fm)
    assert result == [False, True, False, False]


def test_detection_with_non_existing_field():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: value1
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field2": "test"},  # no match
        {"field1": "value1"},  # match
        {"field1": ""},  # no match
        {"field1": None},  # no match
        {"field1": "test"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches(sigma, logs, fm)
    assert result == [False, True, False, False, False]


def test_detection_with_non_existing_field_and_negation():
    sigma_text = """
detection:
    condition: not selection
    selection:
        field1: value1
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field2": "test"},  # match
        {"field1": "value1"},  # no match
        {"field1": ""},  # match
        {"field1": None},  # match
        {"field1": "test"},  # match
    ]
    fm = FieldMapping({})
    result = _matches(sigma, logs, fm)
    assert result == [True, False, True, True, True]


def test_case_sensitivity_for_equals_comparison():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: value
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "value"},  # match
        {"field1": "VALUE"},  # match
        {"field1": "Value"},  # match
        {"field1": "notvalue"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, True, True, False]


def test_case_sensitivity_for_contains_comparison():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1|contains: value
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "test value test"},  # match
        {"field1": "test VALUE test"},  # match
        {"field1": "test Value test"},  # match
        {"field1": "not valid"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, True, True, False]


def test_case_sensitivity_for_startswith_comparison():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1|startswith: value
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "value test"},  # match
        {"field1": "VALUE test"},  # match
        {"field1": "Value test"},  # match
        {"field1": "test value"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, True, True, False]


def test_case_sensitivity_for_endswith_comparison():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1|endswith: value
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "test value"},  # match
        {"field1": "test VALUE"},  # match
        {"field1": "test Value"},  # match
        {"field1": "value test"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, True, True, False]


def test_asterisk():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: a*c
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "abc"},  # match
        {"field1": "abdc"},  # match
        {"field1": "ac"},  # match
        {"field1": "abcd"},  # no match
        {"field1": "dabc"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, True, True, False, False]


def test_escaped_asterisk():
    sigma_text = r"""
detection:
    condition: selection
    selection:
        field1: a\*c
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "abc"},  # no match
        {"field1": r"a\*c"},  # match
        {"field1": r"a\\*c"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [False, True, False]


def test_question_mark():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: a?c
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "abc"},  # match
        {"field1": "abdc"},  # no match
        {"field1": "ac"},  # no match
        {"field1": "abcd"},  # no match
        {"field1": "dabc"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, False, False, False, False]


def test_multiple_wildcard_chars_in_value():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: a*c?e
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "abcde"},  # match
        {"field1": "abbbbbbcde"},  # match
        {"field1": "acde"},  # match
        {"field1": "abcd"},  # no match
        {"field1": "abce"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, True, True, False, False]


def test_multiple_values_with_widcards():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: a*c
        field2: 1*3
        field3: test
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "abc", "field2": 123, "field3": "test"},  # match
        {"field1": "abcd", "field2": 123, "field3": "test"},  # no match
        {"field1": "abc", "field2": 1234, "field3": "test"},  # no match
        {"field1": "abc", "field2": 123, "field3": "not valid"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, False, False, False]


def test_multiple_values_with_widcards_v2():
    sigma_text = """
detection:
    condition: selection
    selection:
        - field1: a*c
        - field2: 1*3
        - field3: test
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "abc", "field2": 123, "field3": "test"},  # match
        {"field1": "abcd", "field2": 123, "field3": "test"},  # match
        {"field1": "abc", "field2": 1234, "field3": "test"},  # match
        {"field1": "abc", "field2": 123, "field3": "not valid"},  # match
        {"field1": "abcd", "field2": 1234, "field3": "not valid"},  # no match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [True, True, True, True, False]


def test_special_chars_are_just_literals():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: a*c[de]
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": "abcd"},  # no match
        {"field1": "abc[de]"},  # match
    ]
    fm = FieldMapping({})
    result = _matches_prepared(sigma, logs, fm)
    assert result == [False, True]


def test_detection_with_null_with_additional_field():
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: null
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    logs = [
        {"field1": None},  # match
        {"field1_additional": None},  # match
        {"field1": None, "field1_additional": None},  # match
        {"field2": "test"},  # match
        {"field1": None, "field1_additional": "test"},  # no match
        {"field1": "test", "field1_additional": None},  # no match
        {"field1": "test", "field1_additional": "test"},  # no match
    ]
    # Field mapping that maps field1 to both field1 and field1_additional
    fm = FieldMapping({"field1": ["field1", "field1_additional"]})
    result = _matches(sigma, logs, fm)
    assert result == [True, True, True, True, False, False, False]


def test_field_mapping_fallback_to_original_field():
    """Test that field mapping falls back to original field when mapped field doesn't exist."""
    sigma_text = """
detection:
    condition: selection
    selection:
        EventID: 1234
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    fm = FieldMapping({"EventID": ["winlog.event_id"]})
    logs = [
        {"EventID": "1234"},  # match
        {"winlog.event_id": "1234"},  # match
        {"EventID": "1234", "winlog.event_id": "5678"},  # match (EventID matches)
        {"OtherField": "1234"},  # no match
        {"EventID": "9999"},  # no match
    ]
    result = _matches(sigma, logs, fm)
    assert result == [True, True, True, False, False]


def test_field_mapping_no_duplicate_when_original_in_mapping():
    """Test that original field name is not duplicated when it's already in the mapping."""
    sigma_text = """
detection:
    condition: selection
    selection:
        field1: value1
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    fm = FieldMapping({"field1": ["field1", "field1_additional"]})

    # Verify get_field_name_in_event doesn't duplicate field1
    fields = fm.get_field_name_in_event("field1")
    assert fields == ["field1", "field1_additional"]
    assert fields.count("field1") == 1

    logs = [
        {"field1": "value1"},  # match
        {"field1_additional": "value1"},  # match
        {"field1": "value1", "field1_additional": "value1"},  # match
    ]
    result = _matches(sigma, logs, fm)
    assert result == [True, True, True]


def test_field_mapping_multiple_mapped_fields():
    """Test field mapping with multiple mapped fields plus original fallback."""
    sigma_text = """
detection:
    condition: selection
    selection:
        CommandLine|contains: powershell
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    fm = FieldMapping({"CommandLine": ["process.command_line", "process.args"]})
    logs = [
        {"CommandLine": "powershell.exe -c test"},  # match
        {"process.command_line": "powershell.exe -c test"},  # match
        {"process.args": "powershell.exe -c test"},  # match
        {"CommandLine": "cmd.exe"},  # no match
    ]
    result = _matches(sigma, logs, fm)
    assert result == [True, True, True, False]


def test_field_mapping_no_mapping_uses_original():
    """Test that when no mapping exists, original field name is used."""
    sigma_text = """
detection:
    condition: selection
    selection:
        EventID: 1234
"""
    sigma = Sigma(text=sigma_text, case_id="test_case_id", technique_ids=[])
    fm = FieldMapping({})

    fields = fm.get_field_name_in_event("EventID")
    assert fields == ["EventID"]

    logs = [
        {"EventID": "1234"},  # match
        {"EventID": "9999"},  # no match
        {"winlog.event_id": "1234"},  # no match (no mapping)
    ]
    result = _matches(sigma, logs, fm)
    assert result == [True, False, False]


# TODO: Add testing detection with bool values
