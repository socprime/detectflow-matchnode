import pytest

from app.domain.sigma_matcher.core import Signature, SignatureNotValidException


def test_valid_string_values():
    # Valid string values should pass validation
    signature = Signature(
        field="test", values=["value1", "value2"], operator="and", command="contains"
    )
    assert signature.values == ["value1", "value2"]


def test_none_conversion():
    # None values should be converted to empty strings
    signature = Signature(field="test", values=["value1", None], operator="and", command="equals")
    assert signature.values == ["value1", ""]


def test_non_string_values():
    # Non-string, non-None values should raise SignatureNotValidException
    with pytest.raises(SignatureNotValidException):
        Signature(field="test", values=["value1", 123], operator="and", command="contains")


def test_empty_string_with_equals():
    # Empty strings should be allowed with "equals" command
    signature = Signature(field="test", values=["value1", ""], operator="and", command="equals")
    assert signature.values == ["value1", ""]


def test_none_with_equals():
    # None values should be converted to empty strings with "equals" command
    signature = Signature(field="test", values=[None], operator="and", command="equals")
    assert signature.values == [""]


def test_empty_string_with_other_commands():
    # Empty strings should raise SignatureNotValidException with commands other than "equals"
    commands = ["contains", "startswith", "endswith"]
    for command in commands:
        with pytest.raises(SignatureNotValidException):
            Signature(field="test", values=["value1", ""], operator="and", command=command)


def test_none_with_other_commands():
    # None values should raise SignatureNotValidException with commands other than "equals"
    commands = ["contains", "startswith", "endswith"]
    for command in commands:
        with pytest.raises(SignatureNotValidException):
            Signature(field="test", values=["value1", None], operator="and", command=command)
