import pytest

from app.domain.sigma_matcher.utils import (
    create_wildcard_regex,
    extract_hostname_from_sigma_event,
    extract_timestamp_from_sigma_event,
    parse_parentheses,
    sigma_detection_value_has_unescaped_wildcard,
)


def test_simple_expression():
    assert parse_parentheses("aaa and not (bbb or ccc)") == [
        "aaa",
        "and",
        "not",
        ["bbb", "or", "ccc"],
    ]


def test_nested_parentheses():
    assert parse_parentheses("aaa and (bbb or (ccc and ddd))") == [
        "aaa",
        "and",
        ["bbb", "or", ["ccc", "and", "ddd"]],
    ]


def test_no_parentheses():
    assert parse_parentheses("aaa and bbb or ccc") == ["aaa", "and", "bbb", "or", "ccc"]


def test_empty_string():
    assert parse_parentheses("") == []


def test_invalid_expression():
    with pytest.raises(ValueError):
        parse_parentheses("aaa and (bbb or ccc")


def test_multiple_parentheses():
    assert parse_parentheses("(aaa and bbb) or (ccc and ddd)") == [
        ["aaa", "and", "bbb"],
        "or",
        ["ccc", "and", "ddd"],
    ]


def test_sigma_detection_value_has_unescaped_wildcard():
    test_cases = [
        ("abc", False),
        ("a*c", True),
        ("a?c", True),
        (r"a\*c", False),  # Input: a\*c -> Escaped * -> False
        (r"a\?c", False),  # Input: a\?c -> Escaped ? -> False
        (r"a\\*c", True),  # Input: a\\*c -> Literal \, Unescaped * -> True
        (r"a\\\*c", False),  # Input: a\\\*c -> Literal \, Escaped * -> False
        (r"a\\\\*c", True),  # Input: a\\\\*c -> Literal \\, Unescaped * -> True
        ("a*", True),
        ("a?", True),
        (r"a\*", False),  # Input: a\* -> Escaped * -> False
        (r"a\?", False),  # Input: a\? -> Escaped ? -> False
        (r"a\\", False),  # Input: a\\ -> Literal \ -> False (no wildcard)
        (r"a\\*", True),  # Input: a\\* -> Literal \, Unescaped * -> True
        (r"a\\?", True),  # Input: a\\? -> Literal \, Unescaped ? -> True
        ("", False),
        ("*", True),
        ("?", True),
        (r"\*", False),  # Input: \* -> Escaped * -> False
        (r"\?", False),  # Input: \? -> Escaped ? -> False
        (r"\\*", True),  # Input: \\* -> Literal \, Unescaped * -> True
        (r"\\?", True),  # Input: \\? -> Literal \, Unescaped ? -> True
    ]
    for val, res in test_cases:
        assert sigma_detection_value_has_unescaped_wildcard(val) is res


def test_create_wildcard_regex_handles_tabs():
    # tabs should be treated as literal characters
    pattern = "abc\tdef"
    regex = create_wildcard_regex(pattern, strict_start=True, strict_end=True)
    assert regex == r"^abc\\tdef$"


def test_create_wildcard_regex_handles_handles_spaces():
    # spaces should be treated as literal characters
    pattern = "abc def"
    regex = create_wildcard_regex(pattern, strict_start=True, strict_end=True)
    assert regex == r"^abc def$"


def test_create_wildcard_regex_handles_handles_n():
    # \n should be treated as literal characters
    pattern = "abc\ndef"
    regex = create_wildcard_regex(pattern, strict_start=True, strict_end=True)
    assert regex == r"^abc\\ndef$"


def test_create_wildcard_regex_handles_handles_underscores():
    # underscores should be treated as literal characters
    pattern = "abc_def"
    regex = create_wildcard_regex(pattern, strict_start=True, strict_end=True)
    assert regex == r"^abc_def$"


def test_extract_hostname_from_sigma_event_uses_configured_fields():
    event = {
        "host": {"name": "host-01"},
        "winlog": {"computer_name": "fallback-host"},
    }

    assert extract_hostname_from_sigma_event(event, ["winlog.computer_name"]) == "fallback-host"


def test_extract_hostname_from_sigma_event_returns_none_without_configured_fields():
    event = {"host": {"name": "host-01"}}

    assert extract_hostname_from_sigma_event(event, None) is None


def test_extract_timestamp_from_sigma_event_uses_configured_fields():
    event = {
        "@timestamp": "2026-05-19T10:00:00Z",
        "winlog": {"time_created": "2026-05-19T11:22:33.456Z"},
    }

    assert (
        extract_timestamp_from_sigma_event(event, ["winlog.time_created"])
        == "2026-05-19T11:22:33.456Z"
    )


def test_extract_timestamp_from_sigma_event_returns_none_without_configured_fields():
    event = {"@timestamp": "2026-05-19T10:00:00Z"}

    assert extract_timestamp_from_sigma_event(event, None) is None
