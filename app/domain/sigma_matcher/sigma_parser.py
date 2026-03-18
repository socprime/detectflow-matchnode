import re
from typing import Literal, Union

import yaml
from pydantic import BaseModel
from schema_parser.sigma_validation import SigmaNotSupported

from .constants import FULL_EVENT_COLUMN_NAME
from .core import InvertedSignature, Signature, SignatureGroup, SignatureNotValidException
from .field_mapping import FieldMapping
from .utils import (
    ConditionIsNotValid,
    create_wildcard_regex,
    group_tokens_by_precedence,
    parse_parentheses,
    sigma_detection_value_has_unescaped_wildcard,
)


class Condition(BaseModel, extra="forbid"):
    name: str
    definition: dict | list
    is_inverted: bool


class ConditionsGroup(BaseModel, extra="forbid"):
    conditions: list[Union[Condition, "ConditionsGroup"]]
    operator: Literal["and", "or"]
    is_inverted: bool = False


class Sigma:
    def __init__(
        self,
        text: str,
        case_id: str,
        title: str | None = None,
        level: str | None = None,
        techniques: list[dict] | None = None,
    ):
        self._text = text
        self.case_id = case_id
        self._dict = self._get_sigma_dict_from_text(text)
        self.query = self._parse_query()

        self.title: str | None = title or self._dict.get("title")
        self.level: str | None = level or self._dict.get("level")
        self.techniques: list[dict] = techniques or []
        self.technique_ids: list[str] = [t["id"] for t in self.techniques if t.get("id")]

    def __repr__(self):
        return f"Sigma(case_id={self.case_id}, level={self.level}, query={self.query})"

    def _get_sigma_dict_from_text(self, text: str) -> dict:
        try:
            return yaml.safe_load(text)
        except yaml.YAMLError as e:
            raise SigmaNotSupported(
                f"Error parsing sigma yaml (case_id: {self.case_id}) - {e}"
            ) from e

    def _parse_query(self) -> SignatureGroup:
        detection = self._get_detection_from_sigma()
        if not detection:
            raise SigmaNotSupported("Detection is not found in sigma")

        conditions_group = self._get_conditions_from_detection(detection)
        sig = self._get_signatures_from_condition_group(conditions_group)
        return sig

    def _get_conditions_from_detection(self, detection: dict) -> ConditionsGroup:
        expression: str = detection["condition"]

        expression = re.sub(r"all of ", "all_of_", expression, flags=re.IGNORECASE)
        expression = re.sub(r"1 of ", "1_of_", expression, flags=re.IGNORECASE)

        if not re.match(r"^[a-zA-Z0-9_ \(\)\?\*]+$", expression):
            raise SigmaNotSupported(
                f"Found not valid characters in condition (case_id: {self.case_id}) - {expression}"
            )

        if (
            ("all_of_" in expression or "1_of_" in expression)
            and "*" not in expression
            and "?" not in expression
            and not expression.endswith("of_them")
        ):
            raise SigmaNotSupported("all of or 1 of condition shoud be pattern")

        condition_tokens = self._get_condition_tokens_from_expression(expression)
        return self._build_conditions_from_expression_tokens(condition_tokens, detection)

    def _get_condition_tokens_from_expression(self, expression: str) -> list[str | list]:
        try:
            condition_tokens = parse_parentheses(expression)
            condition_tokens = group_tokens_by_precedence(condition_tokens)
        except ConditionIsNotValid as e:
            raise SigmaNotSupported(
                f"Condition is not valid (case_id: {self.case_id}) - {e}"
            ) from e
        return condition_tokens

    def _build_conditions_from_expression_tokens(
        self, condition_tokens: list[str | list], detection: dict
    ) -> ConditionsGroup:
        conditions: list[Condition | ConditionsGroup] = []
        is_inverted = False
        operator = None
        for token in condition_tokens:
            if isinstance(token, list):
                cg = self._build_conditions_from_expression_tokens(token, detection)
                if is_inverted:
                    cg.is_inverted = True
                conditions.append(cg)
            else:
                if token.lower() == "not":
                    is_inverted = True
                    continue
                if token.lower() in ["and", "or"]:
                    if operator and operator != token.lower():
                        raise SigmaNotSupported(
                            f"CRITICAL! Only one operator is allowed in the condition group (case_id: {self.case_id}). Tokens: {condition_tokens}"
                        )
                    operator = token.lower()
                    continue

                conditions.append(self._get_element_from_detection(token, detection, is_inverted))

            is_inverted = False

        return ConditionsGroup(conditions=conditions, operator=operator or "and")

    @staticmethod
    def _get_element_from_detection(
        condition_name: str, detection: dict, is_inverted: bool
    ) -> Condition | ConditionsGroup:
        if condition_name.lower().startswith("1_of_") or condition_name.lower().startswith(
            "all_of_"
        ):
            operator = "and" if condition_name.lower().startswith("all_of_") else "or"
            expression = re.sub(r"all_of_|1_of_", "", condition_name, flags=re.IGNORECASE)
            names = []
            if expression.lower() == "them":
                for k in detection.keys():
                    if k != "condition" and not k.startswith("_"):
                        names.append(k)
            else:
                if not re.match(r"^[a-zA-Z0-9_*?]+$", expression):
                    raise SigmaNotSupported(
                        "Found not valid characters in [1 of/all of] condition."
                    )
                pattern = re.compile(expression)
                for k in detection.keys():
                    if k != "condition" and pattern.match(k):
                        names.append(k)

            if not names:
                raise SigmaNotSupported(f"Condition {expression} not found in detection")

            return ConditionsGroup(
                conditions=[
                    Condition(name=n, definition=detection[n], is_inverted=False) for n in names
                ],
                operator=operator,
                is_inverted=is_inverted,
            )

        else:
            if condition_name not in detection:
                raise SigmaNotSupported(f"Condition {condition_name} not found in detection")
            return Condition(
                name=condition_name,
                definition=detection[condition_name],
                is_inverted=is_inverted,
            )

    def _get_signatures_from_condition_group(
        self, conditions_group: ConditionsGroup
    ) -> SignatureGroup:
        groups = []
        for condition in conditions_group.conditions:
            if isinstance(condition, ConditionsGroup):
                gr = self._get_signatures_from_condition_group(condition)

            elif isinstance(condition.definition, dict):
                sigs = [self._parse_signature(k, v) for k, v in condition.definition.items()]
                gr = SignatureGroup(signatures=sigs, operator="and")

            elif isinstance(condition.definition, list):
                # TODO: refactor and write tests
                list_type = self._determine_type_of_list_signatures(values=condition.definition)

                if list_type == "regular":
                    groups_list = []
                    for d in condition.definition:
                        if not isinstance(d, dict):
                            raise SigmaNotSupported(
                                f"Sigma parsing internal logic error. Dict expected, got {type(d)}"
                            )
                        sigs = [self._parse_signature(k, v) for k, v in d.items()]
                        groups_list.append(SignatureGroup(signatures=sigs, operator="and"))
                    gr = SignatureGroup(signatures=groups_list, operator="or")
                elif list_type == "keywords":
                    gr = self._parse_signature(
                        key=f"{FULL_EVENT_COLUMN_NAME}|contains",
                        values=condition.definition,
                    )
                else:
                    raise SigmaNotSupported(
                        "Condition contains multiple data types. Should be list of strings or list of dicts."
                    )

            else:
                raise SigmaNotSupported(
                    f"Error parsing condition - it should be a dict or a list (case_id: {self.case_id})"
                )
            if condition.is_inverted:
                gr = InvertedSignature(gr)
            groups.append(gr)

        return SignatureGroup(signatures=groups, operator=conditions_group.operator)

    @staticmethod
    def _determine_type_of_list_signatures(
        values: list,
    ) -> Literal["regular", "keywords"] | None:
        """
        list of values returns "keywords":
            - value1
            - value2

        list of dicts returns "regular":
            - key1: value1
              key2: value2
            - key3: value3
            - key4: value4

        in other cases list is considered as incorrect and function returns None
        """
        if not isinstance(values, list):
            return None

        keyword_count = 0
        regular_count = 0

        for v in values:
            if isinstance(v, dict):
                regular_count += 1
            elif isinstance(v, str):
                keyword_count += 1
            else:
                return None

            if regular_count and keyword_count:
                return None

        if regular_count and not keyword_count:
            return "regular"
        elif keyword_count and not regular_count:
            return "keywords"
        else:
            return None

    def _parse_signature(self, key: str, values: list[str]) -> Signature | SignatureGroup:
        field, *modifiers = key.split("|")
        modifiers = set(modifiers)
        operator = self._get_operator(modifiers)
        command = self._get_command(modifiers)
        if not isinstance(values, list):
            values = [values]

        normal_values = []
        regex_values = []

        for v in values:
            if sigma_detection_value_has_unescaped_wildcard(str(v)):
                escaped_regex = create_wildcard_regex(v, strict_start=True, strict_end=True)
                regex_values.append(escaped_regex)
            else:
                normal_values.append(v)

        try:
            if regex_values:
                if command != "equals":
                    raise SigmaNotSupported(f"Wildcard is not supported with {command} modifier.")
                sigs = [
                    Signature(field=field, values=[v], operator=operator, command="regex")
                    for v in regex_values
                ]
                if normal_values:
                    sigs.append(
                        Signature(
                            field=field,
                            values=normal_values,
                            operator=operator,
                            command=command,
                        )
                    )
                return SignatureGroup(operator=operator, signatures=sigs)
            else:
                return Signature(
                    field=field,
                    values=normal_values,
                    operator=operator,
                    command=command,
                )
        except SignatureNotValidException as e:
            raise SigmaNotSupported(f"Error parsing detection - {e}") from e

    def _get_detection_from_sigma(self) -> dict:
        detection = self._dict.get("detection")
        if not detection:
            raise SigmaNotSupported(f"Detection not found in sigma (case_id: {self.case_id})")
        if not isinstance(detection, dict):
            raise SigmaNotSupported(f"Detection should be a dictionary (case_id: {self.case_id})")
        detection = self._process_detection_values(detection)
        return detection

    @classmethod
    def _process_detection_values(cls, data):
        """
        Recursively processes detection data after initial loading.
        Converts bool, int and float values to their string representations.
        Leaves None (from YAML null) values as they are.
        """
        if isinstance(data, dict):
            new_dict = {}
            for k, v in data.items():
                if isinstance(v, bool | float | int):
                    new_dict[k] = str(v).lower()
                else:
                    new_dict[k] = cls._process_detection_values(v)
            return new_dict
        elif isinstance(data, list):
            new_list = []
            for item in data:
                if isinstance(item, bool | float | int):
                    new_list.append(str(item).lower())
                else:
                    new_list.append(cls._process_detection_values(item))
            return new_list
        else:
            if isinstance(data, bool | float | int):
                return str(data).lower()
            else:
                return data

    @staticmethod
    def _get_operator(modifiers: set[str]) -> str:
        if "all" in modifiers:
            modifiers.remove("all")
            return "and"
        return "or"

    @staticmethod
    def _get_command(modifiers: set[str]) -> str:
        command = None
        for m in modifiers:
            if m in ["contains", "endswith", "startswith"]:
                command = m
            else:
                raise SigmaNotSupported(f"Command {m} is not supported for now")
        if not command:
            command = "equals"
        return command

    def get_fields(self) -> set[str]:
        """
        Returns a set of fields used in the Sigma signature.
        """
        fields = set()
        for sig in self.query.signatures:
            fields.update(sig.get_fields())
        return fields

    def get_event_fields(self, field_mapping: FieldMapping) -> set[str]:
        """
        Returns a set of fields used in the Sigma signature as they are named in the event.
        """
        fields = set()
        for sig in self.query.signatures:
            fields.update(sig.get_event_fields(field_mapping))
        return fields
