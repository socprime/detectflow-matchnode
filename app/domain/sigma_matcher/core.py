from typing import Literal

from .field_mapping import FieldMapping


class SignatureNotValidException(Exception):  # noqa: N818
    pass


class SignatureGroup:
    def __init__(
        self,
        signatures: list["Signature | SignatureGroup"],
        operator: Literal["and", "or"],
    ):
        assert operator in ["and", "or"], f"Operator {operator} not supported"
        self.signatures = signatures
        self.operator = operator

    def __repr__(self):
        return f"SignatureGroup(operator={self.operator}, signatures={self.signatures})"

    def get_fields(self) -> set[str]:
        fields = set()
        for sig in self.signatures:
            fields.update(sig.get_fields())
        return fields

    def get_event_fields(self, field_mapping: FieldMapping) -> set[str]:
        fields = set()
        for sig in self.signatures:
            fields.update(sig.get_event_fields(field_mapping))
        return fields


class InvertedSignature:
    def __init__(self, sig: "Signature | SignatureGroup"):
        self.sig = sig

    def __repr__(self):
        return f"InvertedSignature(sig={self.sig})"

    def get_fields(self) -> set[str]:
        return self.sig.get_fields()

    def get_event_fields(self, field_mapping: FieldMapping) -> set[str]:
        return self.sig.get_event_fields(field_mapping)


class Signature:
    def __init__(
        self,
        field: str,
        values: list[str | None],
        operator: Literal["and", "or"],
        command: Literal["contains", "endswith", "startswith", "equals", "regex"],
    ):
        assert operator in ["and", "or"], f"Operator {operator} not supported"
        assert command in [
            "contains",
            "endswith",
            "startswith",
            "equals",
            "regex",
        ], f"Command {command} not supported"
        self.field = field
        self.values = values
        self.operator = operator
        self.command = command
        self._validate_and_convert_values()

    def __repr__(self):
        return (
            f"Signature(field={self.field}, operator={self.operator}, "
            f"command={self.command}, values={self.values})"
        )

    def _validate_and_convert_values(self) -> None:
        for n, v in enumerate(self.values):
            if not isinstance(v, str) and v is not None:
                raise SignatureNotValidException(f"Value {v} is not a string or None")
            if v is None:
                self.values[n] = ""
            if (v is None or v == "") and self.command != "equals":
                raise SignatureNotValidException(
                    f"Null value not supported for command {self.command}"
                )
        if self.command == "regex" and len(self.values) != 1:
            raise SignatureNotValidException(
                "Multiple values passed to regex function, only one value is supported."
            )

    def get_fields(self) -> set[str]:
        return {self.field}

    def get_event_fields(self, field_mapping: FieldMapping) -> set[str]:
        return set(field_mapping.get_field_name_in_event(self.field))
