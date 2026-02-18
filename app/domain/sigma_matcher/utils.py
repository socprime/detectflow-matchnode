import re
import time
from collections.abc import Callable
from typing import Literal

from pydantic import BaseModel


class ConditionIsNotValid(Exception):  # noqa: N818
    pass


def timer_wrapper(func: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"Execution time: {round(end - start, 5)}s - {func.__name__}")
        return result

    return wrapper


def parse_parentheses(s: str) -> list[str, list]:
    """example input: aaa and not (bbb or ccc)
    example output: ['aaa', 'and', 'not', ['bbb', 'or', 'ccc']]"""
    stack = []
    s = s.replace("(", " ( ").replace(")", " ) ")
    s = s.split()
    for i in s:
        if i == ")":
            tmp = []
            while stack[-1] != "(":
                tmp.append(stack.pop())
            stack.pop()
            tmp = tmp[::-1]
            stack.append(tmp)
        else:
            stack.append(i)

    def _validate_stack(stack):
        for i in stack:
            if isinstance(i, list):
                _validate_stack(i)
            elif i in ["(", ")"]:
                raise ValueError("Invalid expression")

    _validate_stack(stack)

    return stack


class Token(BaseModel, extra="forbid"):
    value: str | list
    prefix_operator: Literal["or", "and"] | None
    has_not_operator: bool


def _convert_group_to_parsed_token(tokens: list[str | list]) -> Token:
    if len(tokens) == 1:
        assert tokens[0] not in ["and", "or", "not"]
        value = tokens[0]
        operator = None
        has_not_operator = False
    elif len(tokens) == 2:
        assert tokens[0] in ["and", "or", "not"]
        assert tokens[1] not in ["and", "or", "not"]
        value = tokens[1]
        operator = tokens[0] if tokens[0] in ["and", "or"] else None
        has_not_operator = True if tokens[0] == "not" else False
    elif len(tokens) == 3:
        assert tokens[0] in ["and", "or"]
        assert tokens[1] == "not"
        assert tokens[2] not in ["and", "or", "not"]
        value = tokens[2]
        operator = tokens[0]
        has_not_operator = True
    else:
        raise ValueError("Invalid group of tokens")

    return Token(value=value, prefix_operator=operator, has_not_operator=has_not_operator)


def _convert_list_of_tokens_to_parsed_tokens(tokens: list[str | list]) -> list[Token]:
    if not tokens:
        return []
    parsed_tokens: list[Token] = []
    current_group: list[str | list] = []
    for token in tokens:
        if token not in ["and", "or", "not"]:
            current_group.append(token)
            parsed_tokens.append(_convert_group_to_parsed_token(current_group))
            current_group = []
        else:
            current_group.append(token)

    if current_group:
        raise ValueError("Invalid expression")

    for n in range(len(parsed_tokens)):
        if n == 0:
            assert parsed_tokens[n].prefix_operator is None
        else:
            assert parsed_tokens[n].prefix_operator is not None

    return parsed_tokens


def _make_operators_lowercased(tokens: list[str | list]) -> None:
    for n in range(len(tokens)):
        if isinstance(tokens[n], str) and tokens[n].lower() in ["and", "or", "not"]:
            tokens[n] = tokens[n].lower()


# TODO: add tests
def group_tokens_by_precedence(tokens: list[str | list]) -> list[str | list]:
    """Example input ['aaa', 'and', 'bbb', 'and', 'ccc', 'or', 'ddd', 'and', 'eee', 'or', 'fff']
    Example output [['aaa', 'and', 'bbb', 'and', 'ccc'], 'or' ['ddd', 'and', 'eee'] 'or', 'fff']
    """
    _make_operators_lowercased(tokens)
    try:
        parsed_tokens = _convert_list_of_tokens_to_parsed_tokens(tokens)
    except AssertionError as e:
        raise ConditionIsNotValid(str(e)) from e
    res = []
    current_group = []

    def _add_token_to_current_group(current_token: Token, current_group: list) -> None:
        if current_group:
            current_group.append("and")
        if current_token.has_not_operator:
            current_group.append("not")
        if isinstance(current_token.value, str):
            current_group.append(current_token.value)
        else:
            current_group.append(group_tokens_by_precedence(current_token.value))

    def _add_current_group_to_result(current_group: list, res: list) -> None:
        if len(current_group) > 1:
            res.append(current_group)
        else:
            res.append(current_group[0])

    for n in range(len(parsed_tokens)):
        current_token = parsed_tokens[n]
        if n + 1 >= len(parsed_tokens):
            # closing current group because it's the last token
            _add_token_to_current_group(current_token, current_group)
            _add_current_group_to_result(current_group, res)
            break
        if parsed_tokens[n + 1].prefix_operator == "and":  # if after current token is "and"
            # adding current token to current group
            _add_token_to_current_group(current_token, current_group)
        else:
            # closing current group because next token is "or"
            _add_token_to_current_group(current_token, current_group)
            _add_current_group_to_result(current_group, res)
            res.append("or")
            current_group = []

    return res


def sigma_detection_value_has_unescaped_wildcard(s: str) -> bool:
    """
    Checks if a string contains an unescaped wildcard character ('*' or '?').

    An unescaped wildcard is a '*' or '?' character that is not immediately
    preceded by an odd number of backslash ('\') escape characters.
    """
    escaped = False
    for char in s:
        if char == "\\":
            # Toggle the escaped state. If already escaped (due to previous '\'),
            # this '\' is now considered escaped, so the *next* char is not.
            # If not escaped, this '\' might escape the *next* char.
            escaped = not escaped
        elif char in ("*", "?"):
            # Found a wildcard character
            if not escaped:
                # If the wildcard was not preceded by an effective escape character
                return True
            # If it was escaped, reset escaped status and continue checking
            escaped = False
        else:
            # Non-wildcard, non-escape character. Reset escaped status.
            escaped = False
    # Reached end of string without finding an unescaped wildcard
    return False


def create_wildcard_regex(pattern_string: str, strict_start: bool, strict_end: bool) -> str:
    """
    Converts a string with filesystem-style wildcards (*, ?) into a regex pattern.

    - Unescaped '*' matches zero or more characters ('.*').
    - Unescaped '?' matches exactly one character ('.').
    - Escaped wildcards match the literal character ('*', '?').
    - Escaped backslash matches a literal backslash.
    - Other characters, including regex metacharacters ('.', '+', '[]', etc.),
      are escaped to match literally using re.escape().
    - The pattern is anchored to match the entire string ('^...$').

    Args:
      pattern_string: The input string with wildcards.
      strict_start: Whether to anchor the pattern at the start of string.
      strict_end: Whether to anchor the pattern at the end of string.

    Returns:
      A string representing the regex pattern.
    """
    regex = ""
    i = 0
    n = len(pattern_string)
    while i < n:
        char = pattern_string[i]

        if char == "\\":
            if i + 1 < n:
                next_char = pattern_string[i + 1]
                # For any character following a backslash, treat it as a literal.
                # This includes *, ?, and \ itself.
                regex += re.escape(next_char)
                i += 2
            else:
                # Trailing backslash, treat as a literal backslash.
                regex += re.escape(char)
                i += 1
        elif char == "*":
            regex += ".*"
            i += 1
        elif char == "?":
            regex += "."
            i += 1
        else:
            regex += re.escape(char)
            i += 1

    regex = regex.replace("\\ ", " ")  # Replace escaped space with a literal space
    regex = regex.replace("\t", r"\t")  # Replace tab with a literal tab
    regex = regex.replace("\n", r"\n")  # Replace newline with a literal newline
    regex = regex.replace("\\_", r"\_")  # Replace escaped underscore with a literal underscore
    start_char = "^" if strict_start else ""
    end_char = "$" if strict_end else ""
    return f"{start_char}{regex}{end_char}"


if __name__ == "__main__":
    # tokens = [
    #     "aaa",
    #     "and",
    #     "bbb",
    #     "and",
    #     "ccc",
    #     "or",
    #     "ddd",
    #     "and",
    #     "not",
    #     "eee",
    #     "or",
    #     "not",
    #     ["fff", "and", "ggg", "or", "hhh"],
    # ]
    # tokens = ["a", "or", "b"]
    expr = "selection_baseline and (selection_enable OR selection_add_server)"
    tokens = parse_parentheses(expr)
    print(tokens)
    res = group_tokens_by_precedence(tokens)
    for x in res:
        print(x)
