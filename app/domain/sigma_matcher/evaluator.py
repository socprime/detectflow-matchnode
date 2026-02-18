"""Polars-native sigma evaluator.

Each sigma rule is compiled into a self-contained Polars expression.
All rules are evaluated in a single ``df.lazy().select().collect()`` call.

Polars' lazy engine automatically:
  - parallelises across CPU cores
  - applies Common Sub-Expression Elimination (comm_subexpr_elim)
    so shared atomic conditions are evaluated only once
  - uses SIMD-vectorised all_horizontal / any_horizontal for AND/OR
"""

import re
import time

import numpy as np
import polars as pl

from app.config.logging import get_logger

from .core import InvertedSignature, Signature, SignatureGroup
from .field_mapping import FieldMapping
from .sigma_parser import Sigma

logger = get_logger(__name__)


_REGEX_VALUE_THRESHOLD = 500


class Evaluator:
    """Single-pass evaluator that compiles every sigma rule into a ``pl.Expr``.

    All rules are evaluated in one ``df.lazy().select().collect()`` call.
    Polars handles parallelism, SIMD, and common sub-expression elimination.
    """

    def __init__(
        self,
        sigmas: list[Sigma],
        field_mapping: FieldMapping | None = None,
    ) -> None:
        self.sigmas = sigmas
        self.field_mapping = field_mapping or FieldMapping({})

    # ------------------------------------------------------------------
    # evaluate
    # ------------------------------------------------------------------

    def evaluate(self, df: pl.DataFrame) -> list[list[str]]:
        """Evaluate all sigma rules against *df* in one lazy pass.

        Returns ``case_ids[i]`` -- list of matched sigma case_id values
        for row *i*.
        """
        if df.height == 0:
            return []

        n_rows = df.height
        case_ids: list[list[str]] = [[] for _ in range(n_rows)]

        if not self.sigmas:
            return case_ids

        df_columns = set(df.columns)

        # Build one self-contained expression per rule
        t0_build = time.time()
        rule_exprs = [
            self._build_query_expr(s.query, df_columns).alias(f"__r{i}")
            for i, s in enumerate(self.sigmas)
        ]
        logger.debug(
            "expressions_built",
            duration_seconds=round(time.time() - t0_build, 4),
            num_rules=len(self.sigmas),
        )

        # Single lazy pass -- Polars optimises, deduplicates, parallelises
        t0_eval = time.time()
        rules_df = df.lazy().select(rule_exprs).collect()
        logger.debug(
            "polars_evaluated",
            duration_seconds=round(time.time() - t0_eval, 4),
            num_rules=len(self.sigmas),
            rows=n_rows,
        )

        # Extract matches via numpy matrix
        t0_extract = time.time()
        rules_matrix = rules_df.to_numpy()  # (n_rows, n_rules) bool
        row_hits, rule_hits = np.nonzero(rules_matrix)

        sigma_ids = [s.case_id for s in self.sigmas]
        for row_idx, rule_idx in zip(row_hits, rule_hits, strict=False):
            case_ids[row_idx].append(sigma_ids[rule_idx])
        logger.debug(
            "matches_extracted",
            duration_seconds=round(time.time() - t0_extract, 4),
            hits=len(row_hits),
        )

        return case_ids

    # ------------------------------------------------------------------
    # expression builders – query tree
    # ------------------------------------------------------------------

    def _build_query_expr(
        self,
        node: Signature | InvertedSignature | SignatureGroup,
        df_columns: set[str],
    ) -> pl.Expr:
        """Recursively compile a query-tree node into a ``pl.Expr``."""
        if isinstance(node, Signature):
            return self._build_atom_expr(node, df_columns)

        if isinstance(node, InvertedSignature):
            return ~self._build_query_expr(node.sig, df_columns)

        if isinstance(node, SignatureGroup):
            children = node.signatures
            if not children:
                return pl.lit(False)
            child_exprs = [self._build_query_expr(c, df_columns) for c in children]
            if len(child_exprs) == 1:
                return child_exprs[0]
            if node.operator == "and":
                return pl.all_horizontal(child_exprs)
            return pl.any_horizontal(child_exprs)

        return pl.lit(False)

    # ------------------------------------------------------------------
    # expression builders – atomic signature
    # ------------------------------------------------------------------

    def _build_atom_expr(self, sig: Signature, df_columns: set[str]) -> pl.Expr:
        """Build a Polars expression for an atomic Signature."""
        fields_in_event = sig.get_event_fields(self.field_mapping)

        # values == [""] means "field should be null/empty"
        combine_op = "and" if sig.values == [""] else "or"

        field_exprs = [self._build_field_expr(sig, field, df_columns) for field in fields_in_event]

        if not field_exprs:
            return pl.lit(False)
        if len(field_exprs) == 1:
            return field_exprs[0]
        if combine_op == "and":
            return pl.all_horizontal(field_exprs)
        return pl.any_horizontal(field_exprs)

    def _build_field_expr(self, sig: Signature, field: str, df_columns: set[str]) -> pl.Expr:
        """Build expression for one event field within a Signature."""
        if field not in df_columns:
            return pl.lit(sig.command == "equals" and sig.values == [""])

        if sig.operator == "and":
            value_exprs = [self._build_command_expr(sig.command, field, [v]) for v in sig.values]
            if len(value_exprs) == 1:
                return value_exprs[0]
            return pl.all_horizontal(value_exprs)
        # operator == "or"
        return self._build_command_expr(sig.command, field, sig.values)

    # ------------------------------------------------------------------
    # expression builders – per-command
    # ------------------------------------------------------------------

    def _build_command_expr(self, command: str, field: str, values: list[str]) -> pl.Expr:
        """Dispatch to the appropriate command-specific expression builder."""
        if command == "contains":
            return self._expr_contains(field, values)
        if command == "endswith":
            return self._expr_endswith(field, values)
        if command == "startswith":
            return self._expr_startswith(field, values)
        if command == "equals":
            return self._expr_equals(field, values)
        if command == "regex":
            return self._expr_regex(field, values)
        return pl.lit(False)

    @staticmethod
    def _expr_contains(field: str, values: list[str]) -> pl.Expr:
        col = pl.col(field)
        if len(values) == 1:
            return col.str.contains(values[0].lower(), literal=True).fill_null(False)
        if len(values) <= _REGEX_VALUE_THRESHOLD:
            pattern = "|".join(re.escape(v.lower()) for v in values)
            return col.str.contains(pattern, literal=False).fill_null(False)
        return pl.any_horizontal(
            [col.str.contains(v.lower(), literal=True).fill_null(False) for v in values]
        )

    @staticmethod
    def _expr_endswith(field: str, values: list[str]) -> pl.Expr:
        col = pl.col(field)
        vals = [v.lower() for v in values]
        if len(vals) == 1:
            return col.str.ends_with(vals[0]).fill_null(False)
        return pl.any_horizontal([col.str.ends_with(v).fill_null(False) for v in vals])

    @staticmethod
    def _expr_startswith(field: str, values: list[str]) -> pl.Expr:
        col = pl.col(field)
        vals = [v.lower() for v in values]
        if len(vals) == 1:
            return col.str.starts_with(vals[0]).fill_null(False)
        return pl.any_horizontal([col.str.starts_with(v).fill_null(False) for v in vals])

    @staticmethod
    def _expr_equals(field: str, values: list[str]) -> pl.Expr:
        return pl.col(field).is_in([v.lower() for v in values]).fill_null(False)

    @staticmethod
    def _expr_regex(field: str, values: list[str]) -> pl.Expr:
        pattern = values[0].lower()
        if not pattern.startswith("^"):
            pattern = f"^{pattern}"
        return pl.col(field).str.contains(pattern, literal=False).fill_null(False)
