import time
from typing import NamedTuple

import orjson
import polars as pl
from schema_parser import ParserManager

from app.config.logging import get_logger
from app.config.settings import get_settings
from app.domain.filters.loader import Filter
from app.domain.sigma_matcher.constants import FULL_EVENT_COLUMN_NAME

from .evaluator import Evaluator
from .field_mapping import FieldMapping
from .sigma_parser import Sigma, SigmaNotSupported


class ProcessEventsResult(NamedTuple):
    """Result of process_events function."""

    case_ids_per_event: list[list[str]]
    """List of matched rule IDs for each event."""
    parsed_events: list[dict]
    """List of parsed events (after schema parser transformation)."""
    schema_parser_errors: int = 0
    """Number of events that failed schema parsing."""
    prefiltered_mask: list[bool] | None = None
    """Per-event mask: True if the event was excluded by a prefilter. None when no filters applied."""


logger = get_logger(__name__)

conf = get_settings()


def _flatten_dot(d: dict, prefix: str = "") -> dict[str, str]:
    """Flatten a nested dict with dot-separated keys, lowercasing leaf values.

    ~3-5x faster than ``flatten_dict.flatten`` for typical event structures
    because it avoids generic reducer dispatch and type introspection.
    """
    out: dict[str, str] = {}
    stack = [(d, prefix)]
    while stack:
        obj, pfx = stack.pop()
        for k, v in obj.items():
            key = f"{pfx}{k}" if pfx else k
            if isinstance(v, dict):
                stack.append((v, f"{key}."))
            elif v is None:
                out[key] = ""
            else:
                out[key] = str(v).lower()
    return out


def _flatten_and_extract_fields(events: list[dict], used_fields: list[str]) -> list[list[str]]:
    """Flatten events and extract only the needed fields into column-oriented arrays.

    Pre-allocates one ``list[str]`` per field (filled with ``""``), then scatters
    values from each flattened event into the matching column.  This is faster
    than iterating ``used_fields`` per event because typical events have fewer
    flat keys (~50) than ``used_fields`` (~150).

    Returns:
        Column arrays aligned with *used_fields* (``col_arrays[i]`` corresponds
        to ``used_fields[i]``).
    """
    n_events = len(events)
    n_fields = len(used_fields)
    col_arrays: list[list[str]] = [[""] * n_events for _ in range(n_fields)]
    field_to_col_idx: dict[str, int] = {f: i for i, f in enumerate(used_fields)}

    for row_idx, event in enumerate(events):
        flat = _flatten_dot(event)
        for key, val in flat.items():
            col_idx = field_to_col_idx.get(key)
            if col_idx is not None:
                col_arrays[col_idx][row_idx] = val

    return col_arrays


def convert_events_to_df(events: list[dict], used_fields: list[str]) -> pl.DataFrame:
    """Convert events to a Polars DataFrame with selective field extraction.

    Args:
        events: List of event dictionaries
        used_fields: Fields to include in DataFrame (only these are extracted)

    Returns:
        DataFrame with extracted fields (all string values lowercased, nulls -> "")
    """
    t0 = time.time()
    col_arrays = _flatten_and_extract_fields(events, used_fields)
    logger.debug(
        "flattened_and_extracted_fields",
        duration_seconds=round(time.time() - t0, 4),
        events=len(events),
        fields=len(used_fields),
    )

    t0_df_create = time.time()
    df = pl.DataFrame(
        {f: pl.Series(f, col_arrays[i], dtype=pl.Utf8) for i, f in enumerate(used_fields)}
    )
    logger.debug(
        "dataframe_created",
        duration_seconds=round(time.time() - t0_df_create, 4),
        table_rows=len(events),
        table_columns=len(df.columns),
    )
    return df


# Boolean field added to events when schema parsing fails
SCHEMA_PARSER_ERROR_FIELD = "_detect_flow_parser_failed"


def parse_events_with_schema_parser(
    events: list[dict], parser_dict: dict | None = None
) -> tuple[list[dict], int]:
    """Parse events using schema parser configuration.

    Applies per-event error handling to prevent single bad events from
    crashing the entire window. Failed events are marked with a boolean field.

    Args:
        events: List of event dictionaries to parse
        parser_dict: Parser configuration dictionary (optional)

    Returns:
        Tuple of (parsed_events, error_count):
        - parsed_events: List of parsed events (or original events with error flag)
        - error_count: Number of events that failed to parse
    """
    if not parser_dict:
        return events, 0

    parser_manager = ParserManager()
    parsed_events = []
    error_count = 0

    for event in events:
        try:
            parsed_event = parser_manager.configured_parser(event, parser_dict)
            parsed_events.append(parsed_event)
        except Exception:
            error_count += 1
            failed_event = event.copy()
            failed_event[SCHEMA_PARSER_ERROR_FIELD] = True
            parsed_events.append(failed_event)

    return parsed_events, error_count


def prepare_df(events: list[dict], used_fields: list[str]) -> pl.DataFrame:
    """Prepare DataFrame from events with field extraction and normalization.

    Args:
        events: List of event dictionaries (already parsed)
        used_fields: Fields to include in DataFrame

    Returns:
        Normalized DataFrame ready for Sigma matching (lowercased)
    """
    t0 = time.time()

    if FULL_EVENT_COLUMN_NAME in used_fields:
        for event in events:
            event[FULL_EVENT_COLUMN_NAME] = orjson.dumps(event).decode()

        logger.debug(
            "full_event_column_added", duration_seconds=round(time.time() - t0, 4), rows=len(events)
        )

    df = convert_events_to_df(events, used_fields=used_fields)

    if FULL_EVENT_COLUMN_NAME in used_fields:
        for event in events:
            event.pop(FULL_EVENT_COLUMN_NAME, None)

    logger.debug(
        "dataframe_prepared",
        duration_seconds=round(time.time() - t0, 4),
        table_rows=len(df),
        table_columns=len(df.columns),
    )
    return df


def _convert_filters_to_sigmas(filters: list[Filter]) -> list[Sigma]:
    sigmas = []
    for f in filters:
        try:
            sigmas.append(Sigma(text=f.body, case_id=f.id))
        except SigmaNotSupported as e:
            logger.warning("Failed to parse filter", filter=f, error=str(e))
            continue
    return sigmas


def process_events(
    events: list[dict],
    sigmas: list[Sigma],
    filters: list[Filter] | None = None,
    parser_dict: dict | None = None,
    field_mapping: FieldMapping | None = None,
) -> ProcessEventsResult:
    """Process events with Sigma rules.

    Args:
        events: List of event dictionaries to match
        sigmas: List of parsed Sigma rules
        filters: List of filters
        parser_dict: parser configuration dictionary (optional)
        field_mapping: Field name mapping (optional)

    Returns:
        ProcessEventsResult with case_ids_per_event and parsed_events
    """
    sigma_filters = _convert_filters_to_sigmas(filters) if filters else []

    field_mapping = field_mapping or FieldMapping({})

    used_fields = set()
    for s in sigmas:
        used_fields.update(s.get_event_fields(field_mapping))
    for f in sigma_filters:
        used_fields.update(f.get_event_fields(field_mapping))
    used_fields = list(used_fields)

    t0_parse = time.time()
    parsed_events, schema_parser_errors = parse_events_with_schema_parser(
        events=events, parser_dict=parser_dict
    )
    logger.debug(
        "parsed_events",
        duration_seconds=round(time.time() - t0_parse, 4),
        rows=len(parsed_events),
        schema_parser_errors=schema_parser_errors,
    )

    df = prepare_df(events=parsed_events, used_fields=used_fields)

    # Store original DataFrame info for result mapping
    original_length = df.height

    # Apply prefiltering if filters are present
    prefiltered_mask: list[bool] | None = None
    if sigma_filters:
        filters_evaluator = Evaluator(sigmas=sigma_filters, field_mapping=field_mapping)
        filtered_matches = filters_evaluator.evaluate(df)
        # True = event was excluded by a prefilter
        prefiltered_mask = [bool(match_list) for match_list in filtered_matches]

        filtered_count = sum(prefiltered_mask)
        remaining_count = original_length - filtered_count

        kept_indices = [i for i, matched in enumerate(prefiltered_mask) if not matched]
        df = df[kept_indices] if kept_indices else df.head(0)

        logger.info(
            "prefiltering_completed",
            total_events=original_length,
            filtered_out=filtered_count,
            remaining=remaining_count,
            filters_applied=len(sigma_filters),
        )

        if df.height == 0:
            return ProcessEventsResult(
                case_ids_per_event=[[] for _ in range(original_length)],
                parsed_events=parsed_events,
                schema_parser_errors=schema_parser_errors,
                prefiltered_mask=prefiltered_mask,
            )

    sigmas_evaluator = Evaluator(sigmas=sigmas, field_mapping=field_mapping)
    matches = sigmas_evaluator.evaluate(df)

    # Map results back to original DataFrame indices if prefiltering was applied
    if sigma_filters and df.height < original_length:
        full_results = [[] for _ in range(original_length)]
        for i, original_idx in enumerate(kept_indices):
            full_results[original_idx] = matches[i]
        return ProcessEventsResult(
            case_ids_per_event=full_results,
            parsed_events=parsed_events,
            schema_parser_errors=schema_parser_errors,
            prefiltered_mask=prefiltered_mask,
        )

    return ProcessEventsResult(
        case_ids_per_event=matches,
        parsed_events=parsed_events,
        schema_parser_errors=schema_parser_errors,
        prefiltered_mask=prefiltered_mask,
    )
