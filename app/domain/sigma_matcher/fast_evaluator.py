"""Fast Sigma evaluator — compile-once ruleset with per-column evaluation.

Architecture (vs the legacy ``Evaluator``):

1. **Atom dedup + compile-once.**  Rule condition trees are compiled ONCE per
   rule-set into postfix programs over a deduplicated list of atomic
   signatures.  The legacy engine rebuilds ~3k polars expressions on every
   window; here the compiled ruleset is cached by the operator across windows.

2. **Sparse, null-aware DataFrame.**  Absent fields stay ``null`` (legacy
   filled ``""``), and only columns that actually have data in the batch are
   materialized.  Polars string kernels skip nulls, so the per-window cost is
   proportional to populated data, not ``atoms x rows``.

3. **Unified Aho-Corasick per-column pass.**  Columns carrying many literal
   atoms are wrapped in sentinels (``"\\x01" + s + "\\x02"``) so every literal
   command becomes a plain substring pattern::

       contains   lit   ->  lit
       startswith lit   ->  "\\x01" + lit
       endswith   lit   ->  lit + "\\x02"
       equals     lit   ->  "\\x01" + lit + "\\x02"

   One ``str.extract_many`` (Aho-Corasick) scan then resolves ALL of the
   column's literal atoms at once, eliminating per-atom kernel dispatch
   (measured: 1885 CommandLine atoms on a 5k-event window drop from ~113ms to
   ~2ms).  Only regex atoms stay on the per-atom kernel path.  A guard falls
   back to per-atom evaluation if column data contains a sentinel char.

4. **numpy rule combine.**  Boolean AND/OR/NOT over atom result rows is done
   with numpy vector ops — microseconds per rule on 5k-row windows.

5. **Multi-field atom decomposition.**  An atom whose Sigma field maps to
   several candidate event fields (e.g. ``CommandLine`` -> ``process.command_line``
   + original) is split into one per-field sub-atom, each registered on its
   column so it rides the same per-column Aho-Corasick + value-dedup pass; the
   sub-atom rows are then OR/AND-combined back into the atom.  This keeps the
   hot mapped columns (``CommandLine``, ``Image``, ...) on the fast path
   instead of a per-atom, per-field fallback.

Match semantics are identical to the legacy ``Evaluator`` (parity-tested on
the sandbox corpus, sparse and schema-parsed variants):

- ``equals [""]`` matches an absent field / ``None`` value
- other commands are ``False`` on absent fields
- regex patterns that can match ``""`` are ``True`` on absent fields
  (legacy evaluated them against the ``""`` fill value)
"""

from __future__ import annotations

import re

import numpy as np
import polars as pl

from .core import InvertedSignature, Signature, SignatureGroup
from .field_mapping import FieldMapping
from .sigma_parser import Sigma

# A column uses the Aho-Corasick path when it carries at least this many
# literal atoms; below that, per-atom kernels are cheap enough.
_AC_MIN_ATOMS = 8

# Per-column value dedup: evaluate atoms on unique values and expand results
# back via categorical codes. Real event streams are highly repetitive once
# projected onto rule fields (measured 4.4x on a schema-parsed corpus, 625x
# on raw winlogbeat), so this trims atom-kernel and AC-scan work. Applied only
# when the column has enough atoms to amortize the cast and at least half of
# the values are duplicates.
_DEDUP_MIN_ATOMS = 8

# Skip dedup for long-string columns (serialized full events etc.): hashing
# them for the categorical cast costs more than the duplicate ratio returns —
# such values are almost always unique anyway.
_DEDUP_MAX_AVG_LEN = 256

_SENT_START = "\x01"
_SENT_END = "\x02"

_AC_COMMANDS = ("contains", "startswith", "endswith", "equals")

# program ops: ("atom", idx) | ("and", n) | ("or", n) | ("not",) | ("false",)
Program = list[tuple]


def _regex_matches_empty(pattern: str) -> bool:
    try:
        return re.compile(pattern).search("") is not None
    except re.error:
        return False


def _ac_patterns_for(sig: Signature) -> list[str] | None:
    """Sentinel-wrapped AC patterns for a literal atom (None if not eligible)."""
    if sig.command not in _AC_COMMANDS:
        return None
    vals = sorted({v.lower() for v in sig.values})
    if sig.command == "contains":
        return vals
    if sig.command == "startswith":
        return [_SENT_START + v for v in vals]
    if sig.command == "endswith":
        return [v + _SENT_END for v in vals]
    return [_SENT_START + v + _SENT_END for v in vals]  # equals


def _series_command(s: pl.Series, command: str, values: list[str]) -> pl.Series:
    """Evaluate one command against a non-null sub-series."""
    vals = [v.lower() for v in values]
    if command == "contains":
        if len(vals) == 1:
            return s.str.contains(vals[0], literal=True)
        # '|'-joined literal alternation compiles to a multi-literal SIMD
        # searcher inside the rust regex crate (faster than str.contains_any)
        return s.str.contains("|".join(re.escape(v) for v in vals), literal=False)
    if command == "endswith":
        if len(vals) == 1:
            return s.str.ends_with(vals[0])
        return s.str.contains("(?:" + "|".join(re.escape(v) for v in vals) + ")$", literal=False)
    if command == "startswith":
        if len(vals) == 1:
            return s.str.starts_with(vals[0])
        return s.str.contains("^(?:" + "|".join(re.escape(v) for v in vals) + ")", literal=False)
    if command == "equals":
        return s.is_in(vals)
    if command == "regex":
        pattern = vals[0]
        if not pattern.startswith("^"):
            pattern = f"^{pattern}"
        return s.str.contains(pattern, literal=False)
    raise ValueError(f"Command {command} not supported")


def _atom_on_series(sig: Signature, s: pl.Series) -> pl.Series:
    """Evaluate one atom against one column's non-null sub-series."""
    if sig.operator == "and" and len(sig.values) > 1:
        out = _series_command(s, sig.command, [sig.values[0]])
        for v in sig.values[1:]:
            out = out & _series_command(s, sig.command, [v])
        return out
    return _series_command(s, sig.command, sig.values)


def _null_row_value(sig: Signature) -> bool:
    """Atom result on a null (absent) field — legacy ``""`` semantics."""
    if sig.command == "equals":
        return "" in sig.values
    if sig.command == "regex":
        pattern = sig.values[0].lower()
        if not pattern.startswith("^"):
            pattern = f"^{pattern}"
        return _regex_matches_empty(pattern)
    # contains/startswith/endswith have non-empty literals, never match ""
    return False


def _compile_node(
    node: Signature | InvertedSignature | SignatureGroup,
    atom_ids: dict,
    atom_list: list[Signature],
    program: Program,
) -> None:
    """Flatten a condition tree into a postfix program over deduped atoms."""
    if isinstance(node, Signature):
        key = (node.field, node.command, node.operator, tuple(node.values))
        idx = atom_ids.get(key)
        if idx is None:
            idx = len(atom_list)
            atom_ids[key] = idx
            atom_list.append(node)
        program.append(("atom", idx))
        return
    if isinstance(node, InvertedSignature):
        _compile_node(node.sig, atom_ids, atom_list, program)
        program.append(("not",))
        return
    if isinstance(node, SignatureGroup):
        children = node.signatures
        if not children:
            program.append(("false",))
            return
        for c in children:
            _compile_node(c, atom_ids, atom_list, program)
        if len(children) > 1:
            program.append((node.operator, len(children)))
        return
    program.append(("false",))


class FieldExtractor:
    """Prefix-pruned, sparse field extraction (replaces full-event flatten).

    Walks event keys exactly like the legacy ``_flatten_dot`` (nesting
    boundaries == dot boundaries of the flattened key), but descends into a
    subtree only when its dotted prefix can still lead to a used field, and
    materializes columns lazily — fields with no data produce no column.
    """

    def __init__(self, used_fields: list[str]):
        self.used_fields = list(used_fields)
        self.leaves = set(used_fields)
        self.prefixes: set[str] = set()
        for f in used_fields:
            parts = f.split(".")
            for i in range(1, len(parts)):
                self.prefixes.add(".".join(parts[:i]))

    def build_df(self, events: list[dict]) -> pl.DataFrame:
        n = len(events)
        leaves = self.leaves
        prefixes = self.prefixes
        columns: dict[str, list] = {}

        for row_idx, event in enumerate(events):
            stack = [(event, "")]
            while stack:
                obj, pfx = stack.pop()
                for k, v in obj.items():
                    key = f"{pfx}{k}" if pfx else k
                    if isinstance(v, dict):
                        if key in prefixes:
                            stack.append((v, f"{key}."))
                    elif key in leaves:
                        col = columns.get(key)
                        if col is None:
                            col = columns[key] = [None] * n
                        col[row_idx] = "" if v is None else str(v).lower()

        if not columns:
            # evaluator needs row count; one all-null column carries it
            return pl.DataFrame(
                {"__no_used_fields": pl.Series("__no_used_fields", [None] * n, dtype=pl.Utf8)}
            )
        return pl.DataFrame({f: pl.Series(f, col, dtype=pl.Utf8) for f, col in columns.items()})


class CompiledRuleset:
    """Compile a rule-set once; evaluate per window.

    ``df_columns`` must be the set of all extractable fields (the
    ``FieldExtractor.used_fields``), NOT the per-batch df columns — batches
    where a field has no data are handled at evaluate() time.
    """

    def __init__(self, sigmas: list[Sigma], field_mapping: FieldMapping, df_columns: set[str]):
        self.sigmas = sigmas
        self.case_ids = [s.case_id for s in sigmas]

        atom_ids: dict = {}
        self.atoms: list[Signature] = []
        self.programs: list[Program] = []
        for s in sigmas:
            prog: Program = []
            _compile_node(s.query, atom_ids, self.atoms, prog)
            self.programs.append(prog)

        n_atoms = len(self.atoms)
        self.n_atoms = n_atoms
        null_defaults = [_null_row_value(a) for a in self.atoms]

        # Atoms are grouped by the single column they read.  An atom whose Sigma
        # field resolves to MULTIPLE candidate event fields is *decomposed* into
        # one per-field sub-atom (index >= n_atoms), each registered on its
        # column so it rides the same per-column Aho-Corasick + value-dedup fast
        # path as single-field atoms; ``combine`` then OR/ANDs the sub-atom rows
        # back into the real atom's row.  Atoms whose fields are all absent from
        # the rule-field set collapse to a constant.
        self.by_column: dict[str, list[tuple[int, Signature]]] = {}
        self.constant: list[tuple[int, bool]] = []
        # combine[k] = (atom_idx, "or" | "and", [sub_slot, ...])
        self.combine: list[tuple[int, str, list[int]]] = []
        extra_null_defaults: list[bool] = []

        for i, a in enumerate(self.atoms):
            fields = sorted(a.get_event_fields(field_mapping))
            present = [f for f in fields if f in df_columns]

            if not present:
                self.constant.append((i, a.command == "equals" and a.values == [""]))
            elif len(fields) == 1:
                self.by_column.setdefault(fields[0], []).append((i, a))
            else:
                # Match legacy multi-field semantics: an atom matches if the
                # value is found in ANY candidate field (OR); ``equals [""]``
                # (field absent/empty) must hold in ALL candidates (AND).
                op = "and" if a.values == [""] else "or"
                slots: list[int] = []
                for f in fields:
                    slot = n_atoms + len(extra_null_defaults)
                    extra_null_defaults.append(null_defaults[i])
                    self.by_column.setdefault(f, []).append((slot, a))
                    slots.append(slot)
                self.combine.append((i, op, slots))

        # null_defaults for real atoms (0..n_atoms) plus decomposed sub-atoms.
        self.null_defaults = np.array(null_defaults, dtype=bool)
        self.all_null_defaults = np.array(null_defaults + extra_null_defaults, dtype=bool)
        self.n_total = n_atoms + len(extra_null_defaults)

        # Pre-split each column's atoms into AC-eligible literal atoms vs the
        # rest (regex), and prebuild pattern->atom maps.
        # ac_meta[col] = (patterns, pattern_to_atoms, required_hits, rest, default_true)
        self.ac_meta: dict[str, tuple] = {}
        for col, atom_list in self.by_column.items():
            ac_atoms: list[tuple[int, Signature]] = []
            patterns_per_atom: dict[int, list[str]] = {}
            for ai, a in atom_list:
                pats = _ac_patterns_for(a)
                if pats is not None:
                    ac_atoms.append((ai, a))
                    patterns_per_atom[ai] = pats
            if len(ac_atoms) < _AC_MIN_ATOMS:
                continue
            pattern_to_atoms: dict[str, list[int]] = {}
            required: dict[int, int] = {}
            for ai, a in ac_atoms:
                pats = patterns_per_atom[ai]
                required[ai] = len(pats) if (a.operator == "and" and len(pats) > 1) else 1
                for p in pats:
                    pattern_to_atoms.setdefault(p, []).append(ai)
            ac_ids = {ai for ai, _ in ac_atoms}
            rest = [x for x in atom_list if x[0] not in ac_ids]
            # AC scatter only SETS True; atoms whose null-default is True
            # (equals [""]) must be reset to False on non-null rows first
            default_true = [ai for ai, _ in ac_atoms if self.all_null_defaults[ai]]
            self.ac_meta[col] = (
                list(pattern_to_atoms),
                pattern_to_atoms,
                required,
                rest,
                default_true,
            )

    # ------------------------------------------------------------------

    def _eval_column_ac(
        self,
        col: str,
        sub_eval: pl.Series,
        nn_idx: np.ndarray,
        mat: np.ndarray,
        codes: np.ndarray | None,
    ) -> list[tuple[int, Signature]]:
        """One extract_many pass for the column's literal atoms.

        ``sub_eval`` is either the non-null sub-series or its unique values;
        in the latter case ``codes`` maps original rows -> unique rows.
        Returns the non-AC atoms that still need per-atom evaluation.
        """
        patterns, pattern_to_atoms, required, rest, default_true = self.ac_meta[col]
        for ai in default_true:
            mat[ai, nn_idx] = False
        wrapped = _SENT_START + sub_eval + _SENT_END
        hits = wrapped.str.extract_many(patterns, overlapping=True).to_list()
        n_eval = len(hits)
        touched: dict[int, np.ndarray] = {}
        counts: dict[tuple[int, int], int] = {}
        for eval_ri, row_hits in enumerate(hits):
            if not row_hits:
                continue
            for pattern in set(row_hits):
                for ai in pattern_to_atoms[pattern]:
                    if required[ai] == 1:
                        arr = touched.get(ai)
                        if arr is None:
                            arr = touched[ai] = np.zeros(n_eval, dtype=bool)
                        arr[eval_ri] = True
                    else:
                        k = (ai, eval_ri)
                        c = counts.get(k, 0) + 1
                        counts[k] = c
                        if c == required[ai]:
                            arr = touched.get(ai)
                            if arr is None:
                                arr = touched[ai] = np.zeros(n_eval, dtype=bool)
                            arr[eval_ri] = True
        for ai, arr in touched.items():
            mat[ai, nn_idx] = arr[codes] if codes is not None else arr
        return rest

    def _eval_atoms(self, df: pl.DataFrame) -> np.ndarray:
        """Evaluate all unique atoms -> (n_atoms, n_rows) bool matrix."""
        n_rows = df.height
        mat = np.broadcast_to(self.all_null_defaults[:, None], (self.n_total, n_rows)).copy()
        df_columns = set(df.columns)

        for col, atom_list in self.by_column.items():
            if col not in df_columns:
                continue  # no data in this batch -> null defaults apply
            s = df.get_column(col)
            nn_idx = np.nonzero(s.is_not_null().to_numpy())[0]
            if nn_idx.size == 0:
                continue
            sub = s.gather(nn_idx) if nn_idx.size < n_rows else s

            # value dedup: evaluate on unique values, expand via codes
            sub_eval, codes = sub, None
            if (
                len(atom_list) >= _DEDUP_MIN_ATOMS
                and nn_idx.size >= 2 * _DEDUP_MIN_ATOMS
                and (sub.str.len_bytes().mean() or 0.0) <= _DEDUP_MAX_AVG_LEN
            ):
                cat = sub.cast(pl.Categorical)
                uniq = cat.cat.get_categories()
                if uniq.len() * 2 <= sub.len():
                    sub_eval = uniq
                    codes = cat.to_physical().to_numpy()

            use_ac = col in self.ac_meta and not (
                sub_eval.str.contains(_SENT_START, literal=True).any()
                or sub_eval.str.contains(_SENT_END, literal=True).any()
            )
            if use_ac:
                atom_list = self._eval_column_ac(col, sub_eval, nn_idx, mat, codes)
            for ai, a in atom_list:
                res = _atom_on_series(a, sub_eval).to_numpy()
                mat[ai, nn_idx] = res[codes] if codes is not None else res

        for ai, val in self.constant:
            if val:
                mat[ai] = True

        # Recombine each decomposed multi-field atom from its per-column
        # sub-atom rows (already evaluated above via the AC/dedup fast path).
        for ai, op, slots in self.combine:
            acc = mat[slots[0]].copy()
            if op == "and":
                for s in slots[1:]:
                    acc &= mat[s]
            else:
                for s in slots[1:]:
                    acc |= mat[s]
            mat[ai] = acc

        return mat[: self.n_atoms]

    def evaluate(self, df: pl.DataFrame) -> list[list[str]]:
        """Evaluate all rules against *df*.

        Returns ``case_ids[i]`` — matched sigma case_id values for row *i*
        (same contract as the legacy ``Evaluator.evaluate``).
        """
        n_rows = df.height
        if n_rows == 0:
            return []

        mat = self._eval_atoms(df)

        case_ids: list[list[str]] = [[] for _ in range(n_rows)]
        false_vec = np.zeros(n_rows, dtype=bool)
        for rule_idx, prog in enumerate(self.programs):
            stack: list[np.ndarray] = []
            for op in prog:
                tag = op[0]
                if tag == "atom":
                    stack.append(mat[op[1]])
                elif tag == "not":
                    stack.append(~stack.pop())
                elif tag == "and":
                    n = op[1]
                    acc = stack[-n]
                    for v in stack[-n + 1 :]:
                        acc = acc & v
                    del stack[-n:]
                    stack.append(acc)
                elif tag == "or":
                    n = op[1]
                    acc = stack[-n]
                    for v in stack[-n + 1 :]:
                        acc = acc | v
                    del stack[-n:]
                    stack.append(acc)
                else:  # ("false",) — empty signature group
                    stack.append(false_vec)
            rows = np.nonzero(stack.pop())[0]
            if rows.size:
                cid = self.case_ids[rule_idx]
                for r in rows:
                    case_ids[r].append(cid)
        return case_ids
