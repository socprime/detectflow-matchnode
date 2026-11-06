"""
Flink Sigma Matcher with BroadcastState Pattern
Supports parallelism by broadcasting rules to all parallel instances
"""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import NamedTuple

import orjson
import yaml
from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import KeyedBroadcastProcessFunction
from pyflink.datastream.state import (
    BroadcastState,
    ListStateDescriptor,
    MapStateDescriptor,
    ValueStateDescriptor,
)

from app.config.logging import bind_context, get_logger
from app.config.settings import MAX_OUTPUT_DESTINATIONS, OutputDestination, destination_accepts
from app.config.version import get_version
from app.domain.filters.loader import Filter, convert_filter_kafka_event_to_filter
from app.domain.logsources.loader import LogsourceConfig, convert_kafka_event_to_logsource_config
from app.domain.rules.loader import load_sigmas_from_rules_data
from app.domain.sigma_matcher.helpers import process_events
from app.domain.sigma_matcher.sigma_parser import Sigma
from app.domain.sigma_matcher.utils import (
    _format_rfc3339_utc,
    _parse_timestamp_to_utc_datetime,
    extract_hostname_from_sigma_event,
    extract_timestamp_from_sigma_event,
)
from app.operators._pyflink_compat import apply_thread_mode_patches

# PyFlink 2.2.0 embedded (thread) mode still calls removed Java RuntimeContext
# methods; patch to TaskInfo when thread mode is used. Default is process mode.
# on_timer side-output routing for thread mode is fixed in the Dockerfile sed.
apply_thread_mode_patches()

logger = get_logger(__name__)

# Sigma levels, lowest → highest. Unknown / missing severity ranks below all of these.
_SEVERITY_RANK = {
    "informational": 0,
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}
_MISSING_UPDATED = datetime.min.replace(tzinfo=timezone.utc)


def _canonical_level(level: str | None) -> str:
    """Rule level as matched against ``OutputDestination.severities`` (``info`` alias folded)."""
    level = (level or "").lower()
    return "informational" if level == "info" else level


# ============================================================================
# Data Classes for Window Processing
# ============================================================================


@dataclass
class WindowErrors:
    """Error counters for a processing window."""

    event_parsing: int = 0
    """Errors when deserializing events from Flink state (JSON decode)."""
    schema_parsing: int = 0
    """Errors when applying schema parser transformation to events."""
    rule_parsing: int = 0
    """Errors when parsing Sigma rules."""
    matching: int = 0
    """Errors during Sigma matching phase."""

    @property
    def total(self) -> int:
        return self.event_parsing + self.schema_parsing + self.rule_parsing + self.matching

    def to_dict(self) -> dict:
        return {
            "event_parsing_errors": self.event_parsing,
            "schema_parsing_errors": self.schema_parsing,
            "rule_parsing_errors": self.rule_parsing,
            "matching_errors": self.matching,
            "total": self.total,
        }


@dataclass
class PhaseTiming:
    """Timing for a single processing phase."""

    start: float = 0.0
    end: float = 0.0

    @property
    def duration(self) -> float:
        return self.end - self.start if self.end > self.start else 0.0

    def duration_ms(self) -> int:
        return int(self.duration * 1000)


@dataclass
class WindowTimings:
    """All timing data for window processing."""

    window_start: float = 0.0  # First event arrival (from state)
    on_timer_start: float = 0.0  # on_timer() invocation
    on_timer_end: float = 0.0  # on_timer() completion

    event_parsing: PhaseTiming = field(default_factory=PhaseTiming)
    rules_loading: PhaseTiming = field(default_factory=PhaseTiming)
    rules_parsing: PhaseTiming = field(default_factory=PhaseTiming)
    matching: PhaseTiming = field(default_factory=PhaseTiming)
    enrichment_emit: PhaseTiming = field(default_factory=PhaseTiming)

    @property
    def on_timer_duration(self) -> float:
        return self.on_timer_end - self.on_timer_start if self.on_timer_end > 0 else 0.0

    @property
    def window_duration(self) -> float:
        return self.on_timer_end - self.window_start if self.on_timer_end > 0 else 0.0

    def phase_percentage(self, phase_duration: float) -> float:
        """Calculate phase percentage of total on_timer duration."""
        if self.on_timer_duration > 0:
            return (phase_duration / self.on_timer_duration) * 100
        return 0.0


@dataclass
class WindowMetrics:
    """Aggregated metrics for a processing window."""

    # Event counts
    total_events: int = 0
    matched_events: int = 0
    rule_count: int = 0

    # Output counts - events ACTUALLY written to at least one destination (counted once
    # per event, never multiplied by fan-out). output_tagged is 0 when every destination is
    # unmatched_only; output_untagged is 0 when every destination is matched_only.
    output_tagged: int = 0
    output_untagged: int = 0
    prefiltered_events: int = 0

    # Per-rule match counts
    matched_rules: dict = field(default_factory=dict)

    # Timing
    timings: WindowTimings = field(default_factory=WindowTimings)

    # Errors
    errors: WindowErrors = field(default_factory=WindowErrors)

    # Cache hits (for logging)
    rules_cache_hit: bool = False

    @property
    def match_rate(self) -> float:
        """Match rate as percentage."""
        if self.total_events > 0:
            return (self.matched_events / self.total_events) * 100
        return 0.0

    def throughput(self, duration: float) -> float:
        """Calculate events per second for given duration."""
        if duration > 0:
            return self.total_events / duration
        return 0.0


class SigmaMatchingResult(NamedTuple):
    """Result of _run_sigma_matching method."""

    case_ids_per_event: list[list[str]]
    """List of matched rule IDs for each event."""
    start_time: float
    """Start time of matching phase."""
    end_time: float
    """End time of matching phase."""
    parsed_events: list[dict]
    """List of parsed events (after schema parser transformation)."""
    schema_parser_errors: int = 0
    """Number of events that failed schema parsing."""
    prefiltered_mask: list[bool] | None = None
    """Per-event mask: True if the event was excluded by a prefilter."""


# Define broadcast state descriptor for rules
# Each rule is stored with its case_id as key
RULES_STATE_DESCRIPTOR = MapStateDescriptor(
    "sigma-rules-broadcast-state",
    Types.STRING(),  # key: rule_id (case_id)
    Types.STRING(),  # value: JSON rule data
)

# Define state descriptor for event buffer (checkpointed state)
EVENT_BUFFER_STATE_DESCRIPTOR = ListStateDescriptor(
    "event-buffer-state",
    Types.STRING(),  # JSON event strings
)

# Define state descriptor for window start time (per-key, checkpointed)
WINDOW_START_TIME_STATE_DESCRIPTOR = ValueStateDescriptor(
    "window-start-time-state",
    Types.LONG(),  # timestamp in milliseconds
)

EVENT_COUNT_STATE_DESCRIPTOR = ValueStateDescriptor(
    "event-count-state",
    Types.LONG(),  # number of buffered events in current window
)

EARLY_FLUSH_TIMER_TS_STATE_DESCRIPTOR = ValueStateDescriptor(
    "early-flush-timer-ts-state",
    Types.LONG(),  # pending early-flush processing-time timer timestamp
)

PARSER_STATE_DESCRIPTOR = MapStateDescriptor(
    "parser-state",
    Types.STRING(),  # key: "default"
    Types.STRING(),  # value: JSON parser data
)

FILTER_STATE_DESCRIPTOR = MapStateDescriptor(
    "filter-state",
    Types.STRING(),  # key: filter_id
    Types.STRING(),  # value: JSON filter data
)

CUSTOM_FIELDS_STATE_DESCRIPTOR = MapStateDescriptor(
    "custom-fields-state",
    Types.STRING(),  # key: "default"
    Types.STRING(),  # value: JSON parser data
)

# Side output tag for per-rule metrics (to Kafka metrics topic)
METRICS_OUTPUT_TAG = OutputTag("metrics", Types.STRING())
TAGGED_EVENTS_OUTPUT_TAG = OutputTag("tagged-events", Types.STRING())
# All pipeline destinations share one side output: ROW<topic, json> → one KafkaSink.
DETECTIONS_ROW_TYPE = Types.ROW([Types.STRING(), Types.STRING()])
DETECTIONS_OUTPUT_TAG = OutputTag("detections", DETECTIONS_ROW_TYPE)

# Reserved MapState key holding the JSON array of entry ids in the same state.
# Thread mode (Pemja) exposes broadcast state as a raw Java proxy with no
# .keys()/.items() iteration — only get/put/remove are safe in both execution
# modes. The index is maintained incrementally on every broadcast event, so
# reading all rules/filters never needs to iterate the state. Rule ids are
# case UUIDs and filter ids are backend-generated, so this key cannot collide.
STATE_INDEX_KEY = "__entry_ids__"


def _read_state_index(state: BroadcastState) -> list[str]:
    """Read the id index maintained next to the entries (see STATE_INDEX_KEY).

    Falls back to native iteration for state written before the index existed
    (e.g. restored from an old checkpoint); the fallback is process-mode only.
    """
    raw = state.get(STATE_INDEX_KEY)
    if raw:
        try:
            ids = orjson.loads(raw)
            if isinstance(ids, list):
                return ids
        except Exception:
            logger.warning("corrupt state index, falling back to iteration")
    try:
        return [k for k in state.keys() if k != STATE_INDEX_KEY]  # noqa: SIM118
    except Exception:
        # Thread mode without an index (old checkpoint): nothing we can list.
        logger.error("state index missing and iteration unsupported (thread mode)")
        return []


def _index_add(state: BroadcastState, entry_id: str) -> None:
    ids = _read_state_index(state)
    if entry_id not in ids:
        ids.append(entry_id)
        state.put(STATE_INDEX_KEY, orjson.dumps(ids).decode("utf-8"))


def _index_remove(state: BroadcastState, entry_id: str) -> None:
    ids = _read_state_index(state)
    if entry_id in ids:
        ids.remove(entry_id)
        state.put(STATE_INDEX_KEY, orjson.dumps(ids).decode("utf-8"))


class SigmaMatcherBroadcastFunction(KeyedBroadcastProcessFunction):
    """
    Processes events with broadcast Sigma rules.
    Rules are broadcast to all parallel instances via BroadcastState.

    Multi-tenancy support:
    - Filters rules by job_id
    - Supports per-destination output_mode (all_events / matched_only / unmatched_only)

    Metrics:
    - Flink counters available via REST API (total_events, matched_events)
    - Per-rule metrics emitted to Kafka via side output (for API tracking)
    """

    def __init__(
        self,
        window_size_seconds: int,
        window_count_threshold: int,
        job_id: str,
        destinations: list[OutputDestination],
        keep_filtered_events: bool = False,
        emit_tagged_events: bool = True,
    ):
        if not destinations:
            raise ValueError("destinations must contain at least one OutputDestination")
        if len(destinations) > MAX_OUTPUT_DESTINATIONS:
            raise ValueError(
                f"destinations supports at most {MAX_OUTPUT_DESTINATIONS} "
                f"entries (got {len(destinations)})"
            )

        self.window_size_ms = window_size_seconds * 1000
        self.window_size_seconds = window_size_seconds
        self.window_count_threshold = window_count_threshold

        # Multi-tenancy parameters
        self.job_id = job_id
        self.destinations = list(destinations)
        self._has_severity_filter = any(d.severities for d in self.destinations)
        self.keep_filtered_events = keep_filtered_events
        self.emit_tagged_events = emit_tagged_events

        # Operator-local caches (per subtask, shared across keys)
        # Rebuilt on first window after restart - fast enough
        self._sigmas_cache = None
        # SHA1 hash of all rule data from broadcast state. Skips re-parsing
        # when broadcast state was re-read but content is identical
        # (e.g. identical rule document was re-sent to Kafka).
        self._rules_signature = None
        self._rules_state_changed = True  # Start dirty so first window reads from state

        # Flink-managed state (checkpointed, per-key)
        self._event_buffer_state = None
        self._window_start_time_state = None
        self._event_count_state = None
        self._early_flush_timer_ts_state = None

        # Runtime context info (filled in open())
        self._instance_id = None
        self._runtime_context = None
        self._parallelism = None
        self._hostname = None

        # Flink metrics (registered in open(), available via REST API)
        # Cumulative counters (for Kafka metrics consumer)
        self._matched_events_count = [0]
        self._total_events_count = [0]
        # Per-key rate gauges aggregated at instance level (sum across all keys).
        # Updated at end of each window; gauge returns sum of all keys on this subtask.
        # Integer EPS: thread (Pemja) mode drops float gauge callables from the Flink REST
        # catalog, so the dashboard poller sees input_eps=0. Process mode accepts floats,
        # but int keeps both modes working (dashboard loses sub-EPS decimals).
        self._input_eps_per_key: dict[str, int] = {}
        self._output_tagged_eps_per_key: dict[str, int] = {}
        self._output_untagged_eps_per_key: dict[str, int] = {}
        # Error/processing counters (internal use only)
        self._event_parsing_errors_counter = None
        self._schema_parsing_errors_counter = None
        self._rule_parsing_errors_counter = None
        self._matching_errors_counter = None
        self._windows_processed_counter = None
        self._windows_skipped_counter = None
        self._events_discarded_counter = None
        # Distributions (for timing stats)
        self._window_duration_distribution = None
        self._matching_duration_distribution = None
        # Mutable containers for gauge values (gauges need a callable)
        self._active_rules_count = [0]
        # Init metric (job_id, instance_id, timestamp, version) sent once per instance
        self._init_metric_sent = False
        # Warn once per subtask when tagged events cannot be correlated downstream
        self._tagged_config_warned = False
        self._prefiltered_events_count = [0]
        # Per-key last window timestamp; gauge returns max (most recent) across keys
        self._last_window_ts_per_key: dict[str, int] = {}

    def open(self, runtime_context):
        """Initialize with runtime context and managed state"""
        # Store runtime context for system metrics
        self._runtime_context = runtime_context
        self._instance_id = runtime_context.get_index_of_this_subtask()
        self._parallelism = runtime_context.get_number_of_parallel_subtasks()

        # Get hostname from runtime context (best effort)
        try:
            self._hostname = runtime_context.get_task_name_with_subtasks()
        except Exception:
            self._hostname = f"subtask-{self._instance_id}"

        # Initialize Flink-managed state (checkpointed, per-key)
        self._event_buffer_state = runtime_context.get_list_state(EVENT_BUFFER_STATE_DESCRIPTOR)
        self._window_start_time_state = runtime_context.get_state(
            WINDOW_START_TIME_STATE_DESCRIPTOR
        )
        self._event_count_state = runtime_context.get_state(EVENT_COUNT_STATE_DESCRIPTOR)
        self._early_flush_timer_ts_state = runtime_context.get_state(
            EARLY_FLUSH_TIMER_TS_STATE_DESCRIPTOR
        )

        # Bind instance_id and version to all logs in this task
        # Note: contextvars don't persist across processes, so we must bind in worker
        bind_context(instance_id=self._instance_id, detectflow_matchnode_version=get_version())

        # Register custom Flink metrics (available via REST API at /jobs/{id}/vertices/{id}/subtasks/metrics)
        metrics_group = runtime_context.get_metrics_group()

        # Cumulative event counters (for Kafka metrics consumer that updates DB totals)
        self._matched_events_count = [0]
        self._total_events_count = [0]
        metrics_group.gauge("matchedEvents", lambda: self._matched_events_count[0])
        metrics_group.gauge("totalEvents", lambda: self._total_events_count[0])

        # Per-key rate gauges: each on_timer writes its key's rate, gauge sums across all keys.
        # Must return int in thread mode — see _input_eps_per_key comment above.
        self._input_eps_per_key = {}
        self._output_tagged_eps_per_key = {}
        self._output_untagged_eps_per_key = {}
        metrics_group.gauge(
            "inputEventsPerSecond",
            lambda: sum(self._input_eps_per_key.values()),
        )
        metrics_group.gauge(
            "outputTaggedPerSecond",
            lambda: sum(self._output_tagged_eps_per_key.values()),
        )
        metrics_group.gauge(
            "outputUntaggedPerSecond",
            lambda: sum(self._output_untagged_eps_per_key.values()),
        )

        # Prefiltered events counter (cumulative, like matchedEvents/totalEvents)
        self._prefiltered_events_count = [0]
        metrics_group.gauge("prefilteredEvents", lambda: self._prefiltered_events_count[0])

        # Per-key last window timestamp; gauge returns max (most recent) across all keys
        self._last_window_ts_per_key = {}
        metrics_group.gauge(
            "lastWindowTimestampMs",
            lambda: (
                max(self._last_window_ts_per_key.values()) if self._last_window_ts_per_key else 0
            ),
        )

        # Note: Version is sent via Kafka metrics (detectflow_matchnode_version field)
        # PyFlink gauges only support integers, not strings

        # Error counters (keep as counters for internal use, not exposed via REST API)
        self._event_parsing_errors_counter = metrics_group.counter("eventParsingErrors")
        self._schema_parsing_errors_counter = metrics_group.counter("schemaParsingErrors")
        self._rule_parsing_errors_counter = metrics_group.counter("ruleParsingErrors")
        self._matching_errors_counter = metrics_group.counter("matchingErrors")

        # Processing counters
        self._windows_processed_counter = metrics_group.counter("windowsProcessed")
        # Windows thrown away before matching, and the events lost with them. Without these a
        # discarded window is indistinguishable from a source that stopped reading: the events
        # are consumed and committed, but nothing downstream ever accounts for them.
        self._windows_skipped_counter = metrics_group.counter("windowsSkipped")
        self._events_discarded_counter = metrics_group.counter("eventsDiscarded")

        # Gauges (callable that returns current value)
        metrics_group.gauge("activeRulesCount", lambda: self._active_rules_count[0])

        # Distribution for timing statistics (tracks count, min, max, sum)
        self._window_duration_distribution = metrics_group.distribution("windowDurationMs")
        self._matching_duration_distribution = metrics_group.distribution("matchingDurationMs")

        # Detailed phase timing distributions (for performance analysis)
        self._event_parsing_duration_dist = metrics_group.distribution("eventParsingDurationMs")
        self._rules_loading_duration_dist = metrics_group.distribution("rulesLoadingDurationMs")
        self._rules_parsing_duration_dist = metrics_group.distribution("ruleParsingDurationMs")
        self._enrichment_emit_duration_dist = metrics_group.distribution("enrichmentEmitDurationMs")

        logger.info(
            "operator initialized",
            operator="SigmaMatcherBroadcastFunction",
            detectflow_matchnode_version=get_version(),
            job_id=self.job_id,
            window_size_seconds=self.window_size_seconds,
            window_count_threshold=self.window_count_threshold,
            destinations=[
                {
                    "topic": d.topic,
                    "output_mode": d.output_mode,
                    "apply_parser_to_output_events": d.apply_parser_to_output_events,
                    "severities": sorted(d.severities),
                }
                for d in self.destinations
            ],
            keep_filtered_events=self.keep_filtered_events,
            rules_source="broadcast_stream",
            pid=os.getpid(),
            instance_id=self._instance_id,
            event_buffer_checkpointed=True,
            custom_metrics=[
                "matchedEvents",  # Cumulative (for Kafka metrics)
                "totalEvents",  # Cumulative (for Kafka metrics)
                "prefilteredEvents",  # Cumulative (events excluded by prefilters)
                "inputEventsPerSecond",  # Dashboard: input EPS (source of truth)
                "outputTaggedPerSecond",  # Dashboard: tagged events/sec to output
                "outputUntaggedPerSecond",  # Dashboard: untagged events/sec (0 if every dest is matched_only)
                "lastWindowTimestampMs",  # Unix epoch ms when last window was processed
                "eventParsingErrors",
                "ruleParsingErrors",
                "matchingErrors",
                # Phase timing distributions
                "eventParsingDurationMs",
                "rulesLoadingDurationMs",
                "ruleParsingDurationMs",
                "matchingDurationMs",
                "enrichmentEmitDurationMs",
                "windowsProcessed",
                "windowsSkipped",
                "eventsDiscarded",
                "activeRulesCount",
                "windowDurationMs",
            ],
        )
        logger.info("tagged events emit configured", enabled=self.emit_tagged_events)

    def process_element(self, value: str, ctx):  # noqa: ANN001
        """
        Process events (non-broadcast stream)
        Collects events into windows and processes them with broadcast rules

        IMPORTANT: Uses per-key ValueState for window_start_time to ensure
        each key gets its own timer. Without this, only one key per subtask
        would trigger on_timer, causing events from other keys to be lost.
        """
        current_time = ctx.timer_service().current_processing_time()

        # Get window start time for this specific key from state
        window_start_time = self._window_start_time_state.value()

        # Initialize window for this key if not yet started
        if window_start_time is None:
            window_start_time = current_time
            self._window_start_time_state.update(window_start_time)

            # Register timer for this key
            window_end_time = window_start_time + self.window_size_ms
            ctx.timer_service().register_processing_time_timer(window_end_time)

        # Add event to checkpointed buffer state (per-key)
        # Store JSON string directly (no parsing overhead - will parse in on_timer when needed)
        self._event_buffer_state.add(value)
        event_count = (self._event_count_state.value() or 0) + 1
        self._event_count_state.update(event_count)

        # Guardrail: flush early when buffer gets too large to avoid oversized gRPC
        # payloads when Python operator reads ListState during on_timer().
        # The original window timer is deleted so it doesn't fire later and create
        # unexpectedly short follow-up windows.
        if event_count >= self.window_count_threshold:
            scheduled_early_flush_ts = self._early_flush_timer_ts_state.value()
            if scheduled_early_flush_ts is None:
                original_timer_ts = window_start_time + self.window_size_ms
                ctx.timer_service().delete_processing_time_timer(original_timer_ts)

                early_flush_ts = current_time + 1
                ctx.timer_service().register_processing_time_timer(early_flush_ts)
                self._early_flush_timer_ts_state.update(early_flush_ts)
                logger.info(
                    "window flush scheduled by count threshold",
                    threshold=self.window_count_threshold,
                    buffered_events=event_count,
                )

    def process_broadcast_element(self, value: str, ctx):  # noqa: ANN001
        """
        Process Sigma rules (broadcast stream)
        Updates broadcast state that is visible to all parallel instances

        Multi-tenancy: Filters rules by job_id to ensure each job only
        processes its own rules from the shared KAFKA_RULES_TOPIC.

        Rule lifecycle:
        - Add/update rule: Send rule with full JSON payload
        - Logical delete: Send rule with empty sigma.text field
        - Tombstones: Filtered out at stream level (using ByteArraySchema)

        Note: Uses ByteArraySchema deserializer which handles null Kafka values gracefully.
        Tombstones are filtered out before reaching this method.
        """
        try:
            event = orjson.loads(value)
            if not self._check_if_event_is_relevant_for_job(event):
                return

            # Emit init metric once per instance on first broadcast event.
            if not self._init_metric_sent:
                metric_json = self._build_init_metric_json()
                if metric_json is not None:
                    yield METRICS_OUTPUT_TAG, metric_json
                    self._init_metric_sent = True

            if self._is_rule_event(event):
                state = ctx.get_broadcast_state(RULES_STATE_DESCRIPTOR)
                self._process_rule_event(event, state)
            elif self._is_parser_event(event):
                state = ctx.get_broadcast_state(PARSER_STATE_DESCRIPTOR)
                self._process_parser_event(event, state)
            elif self._is_filter_event(event):
                state = ctx.get_broadcast_state(FILTER_STATE_DESCRIPTOR)
                self._process_filter_event(event, state)
            elif self._is_custom_fields_event(event):
                state = ctx.get_broadcast_state(CUSTOM_FIELDS_STATE_DESCRIPTOR)
                self._process_custom_fields_event(event, state)
            else:
                logger.error("Unrecognized broadcast event", event=event)

        except Exception as e:
            logger.exception("Failed to process broadcast event", error=str(e))

    def _build_init_metric_json(self) -> str | None:
        try:
            init_metric = {
                "job_id": self.job_id,
                "instance_id": self._instance_id,
                "timestamp": int(time.time() * 1000),
                "detectflow_matchnode_version": get_version(),
            }
            return orjson.dumps(init_metric).decode("utf-8")
        except Exception as e:
            logger.warning(
                "init metric emit failed",
                error=str(e),
                job_id=self.job_id,
                instance_id=self._instance_id,
            )
            return None

    def _build_window_discarded_metric_json(
        self, reason: str, discarded_events: int, current_key: object
    ) -> str | None:
        """Account for a window dropped before it could be matched.

        Dropping is intentional — there is nothing to match the buffer against — but doing it
        silently is what made a startup race look like a stalled Kafka source: the events are
        consumed, their offsets committed, and then they vanish with no metric to show for them.

        Deliberately carries no ``window_total_events``: these events were never processed, and
        anything summing window totals must not count them as if they had been. Consumers that
        do not know this shape ignore the extra fields and see a zero-event report, exactly as
        they already do for the init metric.
        """
        self._windows_skipped_counter.inc()
        self._events_discarded_counter.inc(discarded_events)
        logger.warning(
            "window discarded",
            reason=reason,
            discarded_events=discarded_events,
            key=str(current_key),
        )
        try:
            return orjson.dumps(
                {
                    "job_id": self.job_id,
                    "instance_id": self._instance_id,
                    "key": str(current_key),
                    "timestamp": int(time.time() * 1000),
                    "detectflow_matchnode_version": get_version(),
                    "window_discarded_reason": reason,
                    "window_discarded_events": discarded_events,
                }
            ).decode("utf-8")
        except Exception as e:
            logger.warning("window discarded metric emit failed", error=str(e), reason=reason)
            return None

    def _check_if_event_is_relevant_for_job(self, event: dict) -> bool:
        return event.get("job_id") == self.job_id

    @staticmethod
    def _is_rule_event(event: dict) -> bool:
        rule_id = event.get("case", {}).get("id")
        rule_id_exists = isinstance(rule_id, str) and len(rule_id) > 0
        return rule_id_exists and (event.get("type") == "rule" or not event.get("type"))

    @staticmethod
    def _is_parser_event(event: dict) -> bool:
        return event.get("type") == "parser"

    @staticmethod
    def _is_custom_fields_event(event: dict) -> bool:
        return event.get("type") == "custom_fields"

    @staticmethod
    def _is_filter_event(event: dict) -> bool:
        return event.get("type") == "filter"

    def _process_parser_event(self, event: dict, state: BroadcastState) -> None:
        if event.get("deleted") is True:
            state.clear()
            logger.info("parser deleted")
        else:
            state.put("default", orjson.dumps(event).decode("utf-8"))
            logger.info("parser updated", parser=event)

    def _process_custom_fields_event(self, event: dict, state: BroadcastState) -> None:
        if event.get("deleted") is True:
            state.clear()
            logger.info("custom fields deleted")
        else:
            state.put("default", orjson.dumps(event).decode("utf-8"))
            logger.info("custom fields updated", custom_fields=event)

    def _process_filter_event(self, event: dict, state: BroadcastState) -> None:
        filter_id = event.get("filter_id")
        if not filter_id:
            logger.warning("filter missing id", message="Received filter without filter_id")
            return

        if event.get("deleted") is True:
            if state.get(filter_id) is not None:
                state.remove(filter_id)
                _index_remove(state, filter_id)
                logger.info("filter deleted", filter_id=filter_id)
        else:
            filter_json = orjson.dumps(event).decode("utf-8")
            state.put(filter_id, filter_json)
            _index_add(state, filter_id)
            logger.info("filter updated", filter_id=filter_id)

    def _process_rule_event(self, event: dict, state: BroadcastState) -> None:
        rule_id = event.get("case", {}).get("id")

        if not rule_id:
            logger.warning("rule missing id", message="Received rule without case.id")
            return

        # Delete only when sigma.text is empty (logical delete)
        empty_sigma = not event.get("sigma", {}).get("text")
        if empty_sigma:
            if state.get(rule_id) is not None:
                state.remove(rule_id)
                _index_remove(state, rule_id)
                # Invalidate rules cache - rules changed
                self._rules_state_changed = True
                logger.info("rule deleted", rule_id=rule_id, job_id=self.job_id)
            return

        rule_json = orjson.dumps(event).decode("utf-8")
        state.put(rule_id, rule_json)
        _index_add(state, rule_id)
        # Invalidate rules cache - rules changed
        self._rules_state_changed = True
        logger.info(
            "rule updated",
            rule_id=rule_id,
            job_id=self.job_id,
            action="insert_or_update",
        )

    @staticmethod
    def _get_logsource_config_from_state(
        state: BroadcastState,
    ) -> LogsourceConfig:
        event = state.get("default")
        if not event:
            logger.warning("no logsource config found")
            return LogsourceConfig()

        try:
            event = orjson.loads(event)
            logsource_config = convert_kafka_event_to_logsource_config(event)
            logger.info("logsource config found", logsource_config=event)
            return logsource_config
        except Exception as e:
            logger.exception("failed to parse logsource config", error=str(e))
            return LogsourceConfig()

    @staticmethod
    def _get_filters_from_state(state: BroadcastState) -> list[Filter]:
        filters = []
        for filter_id in _read_state_index(state):
            filter_json = state.get(filter_id)
            if filter_json:
                f = convert_filter_kafka_event_to_filter(orjson.loads(filter_json))
                if f:
                    filters.append(f)
        return filters

    @staticmethod
    def _get_custom_fields_from_state(state: BroadcastState) -> dict | None:
        doc = state.get("default")
        if not doc:
            return None

        try:
            data = orjson.loads(doc).get("custom_fields")
            if not data:
                return None
            fields = yaml.safe_load(data)
            if isinstance(fields, dict):
                return fields
            else:
                raise ValueError("custom fields is not a dictionary")
        except Exception as e:
            logger.exception("failed to parse custom fields", error=str(e))
            return None

    def _clear_window_state(self) -> None:
        """Clear checkpointed state for next window (per-key)."""
        self._event_buffer_state.clear()
        self._window_start_time_state.clear()
        self._event_count_state.clear()
        self._early_flush_timer_ts_state.clear()

    def _extract_and_deserialize_events_from_state(self) -> tuple[list[dict], int, float]:
        """
        Extract and deserialize events from checkpointed state.

        Returns:
            Tuple of (event_buffer, parsing_errors, deserialization_duration)
        """
        start_time = time.time()
        parsing_errors = 0

        event_buffer_items = list(self._event_buffer_state.get())
        event_buffer: list[dict] = []

        for event_json in event_buffer_items:
            try:
                event_buffer.append(orjson.loads(event_json))
            except Exception as e:
                parsing_errors += 1
                logger.exception("event deserialization failed", error=str(e))

        duration = time.time() - start_time
        return event_buffer, parsing_errors, duration

    @staticmethod
    def _read_rules_from_broadcast(rules_state: BroadcastState) -> tuple[list[dict], str]:
        """Read all rules from broadcast state and compute content signature.

        Uses the incrementally-maintained id index instead of iterating the
        state — Pemja (thread mode) exposes no iteration on the Java proxy.
        """
        rules_data = []
        hasher = hashlib.sha1()

        for rule_id in sorted(_read_state_index(rules_state)):
            rule_json = rules_state.get(rule_id)
            if rule_json:
                hasher.update(rule_id.encode("utf-8"))
                hasher.update(b"\0")
                hasher.update(rule_json.encode("utf-8"))
                rules_data.append(orjson.loads(rule_json))

        return rules_data, hasher.hexdigest()

    def _load_and_parse_rules(self, ctx, metrics: WindowMetrics) -> list[Sigma]:  # noqa: ANN001
        """
        Load rules from broadcast state and parse into Sigma objects.
        Raw data and parsed results are cached internally.

        Populates metrics: timings.rules_loading, timings.rules_parsing,
        rules_cache_hit, rule_count. Also updates self._active_rules_count.

        Returns:
            List of Sigma objects (empty if no rules)
        """
        metrics.timings.rules_loading.start = time.time()

        if not self._rules_state_changed and self._sigmas_cache is not None:
            metrics.timings.rules_loading.end = time.time()
            metrics.timings.rules_parsing.start = time.time()
            metrics.timings.rules_parsing.end = time.time()
            metrics.rules_cache_hit = True
            metrics.rule_count = len(self._sigmas_cache)
            self._active_rules_count[0] = metrics.rule_count
            return self._sigmas_cache

        rules_state = ctx.get_broadcast_state(RULES_STATE_DESCRIPTOR)
        rules_data, signature = self._read_rules_from_broadcast(rules_state)
        self._rules_state_changed = False

        metrics.timings.rules_loading.end = time.time()
        metrics.timings.rules_parsing.start = time.time()

        if signature == self._rules_signature and self._sigmas_cache is not None:
            logger.debug("using cached parsed rules", signature=signature[:8])
            metrics.timings.rules_parsing.end = time.time()
            metrics.rules_cache_hit = True
            metrics.rule_count = len(self._sigmas_cache)
            self._active_rules_count[0] = metrics.rule_count
            return self._sigmas_cache

        sigmas = load_sigmas_from_rules_data(rules_data)

        self._sigmas_cache = sigmas
        self._rules_signature = signature
        metrics.rule_count = len(sigmas)
        self._active_rules_count[0] = metrics.rule_count
        logger.debug("rules cache updated", signature=signature[:8])

        metrics.timings.rules_parsing.end = time.time()
        metrics.rules_cache_hit = False
        return sigmas

    def _run_sigma_matching(
        self,
        event_buffer: list[dict],
        sigmas: list[Sigma],
        filters: list[Filter],
        logsource_config: LogsourceConfig,
    ) -> SigmaMatchingResult | None:
        """
        Execute Sigma matching on events.

        Returns:
            SigmaMatchingResult or None on error
        """
        start_time = time.time()
        try:
            result = process_events(
                events=event_buffer,
                sigmas=sigmas,
                filters=filters,
                parser_dict=logsource_config.parser_config,
                field_mapping=logsource_config.field_mapping,
            )
            end_time = time.time()

            return SigmaMatchingResult(
                case_ids_per_event=result.case_ids_per_event,
                start_time=start_time,
                end_time=end_time,
                parsed_events=result.parsed_events,
                schema_parser_errors=result.schema_parser_errors,
                prefiltered_mask=result.prefiltered_mask,
            )
        except Exception as e:
            logger.exception("sigma matching error", error=str(e))
            return None

    @staticmethod
    def _build_enrichment_map(sigmas: list[Sigma]) -> dict[str, dict]:
        return {
            e.case_id: {
                "rule_id": e.case_id,
                "rule_title": e.title,
                "severity": e.level,
                "sigma_uuid": e.sigma_uuid,
                "technique_ids": e.technique_ids,
                "techniques": e.techniques,
                "updated": e.updated,
            }
            for e in sigmas
        }

    @staticmethod
    def _detection_to_match(detection: dict) -> dict:
        """Map an internal enrichment dict to a ``detectflow.matches[]`` entry.

        Both ids are carried explicitly: ``sigma_rule_uuid`` (the Sigma rule
        UUID, used by downstream correlation as the inverted-index key) and
        ``_rule_id`` (the internal case id). This removes the old ambiguity
        where a single ``sigma_rule_ids`` field meant different ids in the
        output vs the tagged-events topic.
        """
        return {
            "sigma_rule_uuid": detection.get("sigma_uuid"),
            "_rule_id": detection.get("rule_id"),
            "rule_name": detection.get("rule_title"),
            "severity": detection.get("severity"),
            "mitre_techniques": detection.get("technique_ids", []) or [],
        }

    @staticmethod
    def _primary_detection_sort_key(detection: dict) -> tuple[int, datetime]:
        """Sort key for primary_match: highest severity, then latest ``updated``."""
        severity = (detection.get("severity") or "").lower()
        rank = _SEVERITY_RANK.get(severity, -1)
        updated = _parse_timestamp_to_utc_datetime(detection.get("updated"))
        return (rank, updated if updated is not None else _MISSING_UPDATED)

    @staticmethod
    def _select_primary_detection(detections: list[dict]) -> dict | None:
        """Pick the rule that becomes ``detectflow.primary_match``.

        Highest Sigma severity wins. Equal severity is broken by the later
        Kafka ``updated`` timestamp. Equal keys keep the first detection
        (``max`` is stable).
        """
        if not detections:
            return None
        return max(detections, key=SigmaMatcherBroadcastFunction._primary_detection_sort_key)

    @staticmethod
    def _build_detectflow(
        detections: list[dict],
        processed_at: str,
        matchnode_version: str,
    ) -> dict:
        """Build the ``detectflow`` block shared by detections and tagged-events."""
        matches = [SigmaMatcherBroadcastFunction._detection_to_match(d) for d in detections]
        detectflow: dict = {
            "processed_at": processed_at,
            "matchnode_version": matchnode_version,
        }
        primary = SigmaMatcherBroadcastFunction._select_primary_detection(detections)
        if primary is not None:
            primary_match = SigmaMatcherBroadcastFunction._detection_to_match(primary)
            detectflow["severity"] = primary_match["severity"]
            detectflow["primary_match"] = primary_match
        detectflow["matches"] = matches
        return detectflow

    def _create_tagged_event_json(
        self,
        parsed_event: dict,
        logsource_config: LogsourceConfig,
        detectflow: dict,
    ) -> str | None:
        """Build one ``detectflow.tagged-events`` envelope for a matched event.

        Emitted ONCE per matched event regardless of how many destinations the pipeline
        fans out to — the destination sinks are a separate side-output path.

        Isolation for correlation is ``pipeline_id`` (this job's ``JOB_ID``) plus the
        dedicated tagged-events topic. Destination topic names are SIEM output only and
        are not stamped on this envelope.
        """
        tagged_event = {
            "pipeline_id": self.job_id,
            "timestamp": extract_timestamp_from_sigma_event(
                parsed_event,
                logsource_config.timestamp_fields,
            ),
            "hostname": extract_hostname_from_sigma_event(
                parsed_event,
                logsource_config.hostname_fields,
            ),
            "detectflow": detectflow,
            "parsed_event": parsed_event,
        }

        try:
            return orjson.dumps(tagged_event).decode("utf-8")
        except Exception as e:
            logger.error("tagged event serialization failed", error=str(e))
            return None

    def _build_detectflow_block(
        self,
        case_ids: list[str],
        enrichment_map: dict[str, dict],
        processed_at: str,
        matchnode_version: str,
    ) -> dict | None:
        """Build the shared ``detectflow`` enrichment block for a matched event."""
        if not case_ids:
            return None
        detections = [enrichment_map[cid] for cid in case_ids if enrichment_map.get(cid)]
        return self._build_detectflow(
            detections=detections,
            processed_at=processed_at,
            matchnode_version=matchnode_version,
        )

    def _serialize_destination_payload(
        self,
        *,
        base_event: dict,
        custom_fields: dict | None,
        detectflow: dict | None,
        event_idx: int,
        destination_topic: str,
    ) -> str | None:
        """Build one destination JSON string (shallow copy + optional enrichment)."""
        event = dict(base_event)
        if custom_fields:
            event.update(custom_fields)
        if detectflow is not None:
            event["detectflow"] = detectflow
        try:
            return orjson.dumps(event).decode("utf-8")
        except Exception as e:
            logger.error(
                "event serialization failed",
                error=str(e),
                destination_topic=destination_topic,
                event_idx=event_idx,
            )
            return None

    def _iter_destination_emits(
        self,
        *,
        event_idx: int,
        case_ids: list[str],
        original_event: dict,
        parsed_event: dict,
        custom_fields: dict | None,
        detectflow: dict | None,
        event_levels: frozenset[str] = frozenset(),
    ):
        """Yield ``(DETECTIONS_OUTPUT_TAG, Row(topic, json))`` for each accepting dest.

        Applies per-destination ``output_mode``, ``severities`` (matched_only only — set
        nowhere else; a detection passes when any of its rules' levels, ``event_levels``,
        is selected) and ``apply_parser_to_output_events``.
        Payload shape depends only on the parser flag (+ shared enrichment), so JSON is
        memoized per flag — N destinations with the same flag serialize once, not N times.
        The Kafka sink routes on ``Row[0]`` (topic); ``Row[1]`` is the event JSON.
        """
        matched = bool(case_ids)
        # At most two distinct bodies per event: parsed vs raw.
        serialized_by_parser_flag: dict[bool, str | None] = {}
        for dest in self.destinations:
            if not destination_accepts(dest.output_mode, matched):
                continue
            if matched and dest.severities and dest.severities.isdisjoint(event_levels):
                continue

            use_parsed = dest.apply_parser_to_output_events
            if use_parsed not in serialized_by_parser_flag:
                base_event = parsed_event if use_parsed else original_event
                serialized_by_parser_flag[use_parsed] = self._serialize_destination_payload(
                    base_event=base_event,
                    custom_fields=custom_fields,
                    detectflow=detectflow,
                    event_idx=event_idx,
                    destination_topic=dest.topic,
                )

            payload = serialized_by_parser_flag[use_parsed]
            if payload is not None:
                yield DETECTIONS_OUTPUT_TAG, Row(dest.topic, payload)

    def _enrich_and_emit_events(
        self,
        original_event_buffer: list[dict],
        match_result: SigmaMatchingResult,
        sigmas: list[Sigma],
        custom_fields: dict | None,
        logsource_config: LogsourceConfig,
        metrics: WindowMetrics,
    ):
        """
        Enrich events and fan out to per-destination side outputs + tagged-events.

        Updates metrics in place:
        - metrics.matched_events, metrics.matched_rules
        - metrics.output_tagged, metrics.output_untagged (events written to ANY
          destination — not multiplied by fan-out)

        Yields ``(OutputTag, value)``: ``ROW<topic, json>`` on the detections tag,
        JSON strings on tagged-events.
        """
        enrichment_map = self._build_enrichment_map(sigmas)
        # Processing-time metadata stamped on the detectflow block, shared by
        # every event emitted from this window (output destinations + tagged-events).
        processed_at = _format_rfc3339_utc(datetime.now(timezone.utc))
        matchnode_version = get_version()

        # Tagged events without a hostname or a timestamp are dropped by correlation
        # (missing_hostname / bad_timestamp), while this node's own metrics stay green.
        # Both come from the pipeline's parser broadcast config, so warn once per subtask.
        if (
            self.emit_tagged_events
            and not self._tagged_config_warned
            and not (logsource_config.hostname_fields and logsource_config.timestamp_fields)
        ):
            self._tagged_config_warned = True
            logger.warning(
                "tagged events will be dropped downstream",
                reason="logsource parser config is missing hostname_fields or timestamp_fields",
                hostname_fields=logsource_config.hostname_fields,
                timestamp_fields=logsource_config.timestamp_fields,
                job_id=self.job_id,
            )

        for idx, case_ids in enumerate(match_result.case_ids_per_event):
            is_prefiltered = bool(
                match_result.prefiltered_mask and match_result.prefiltered_mask[idx]
            )

            # Match counters update BEFORE output filtering (gauges / Kafka metrics).
            if case_ids:
                for case_id in case_ids:
                    metrics.matched_rules[case_id] = metrics.matched_rules.get(case_id, 0) + 1
            self._total_events_count[0] += 1
            if case_ids:
                metrics.matched_events += 1
                self._matched_events_count[0] += 1

            if is_prefiltered:
                metrics.prefiltered_events += 1
                self._prefiltered_events_count[0] += 1
                if not self.keep_filtered_events:
                    continue

            # Shared enrichment for tagged-events + every destination that receives
            # this event. Unmatched events carry no detectflow block.
            detectflow = self._build_detectflow_block(
                case_ids, enrichment_map, processed_at, matchnode_version
            )
            # Skip building the tagged-event JSON when nothing consumes it:
            # this, not the sink, is where the per-event CPU goes.
            if detectflow is not None and self.emit_tagged_events:
                tagged_event_json = self._create_tagged_event_json(
                    parsed_event=match_result.parsed_events[idx],
                    logsource_config=logsource_config,
                    detectflow=detectflow,
                )
                if tagged_event_json is not None:
                    yield TAGGED_EVENTS_OUTPUT_TAG, tagged_event_json

            event_levels = (
                frozenset(
                    _canonical_level(enrichment_map[cid]["severity"])
                    for cid in case_ids
                    if cid in enrichment_map
                )
                if case_ids and self._has_severity_filter
                else frozenset()
            )

            emitted_to_any_destination = False
            for item in self._iter_destination_emits(
                event_idx=idx,
                case_ids=case_ids,
                original_event=original_event_buffer[idx],
                parsed_event=match_result.parsed_events[idx],
                custom_fields=custom_fields,
                detectflow=detectflow,
                event_levels=event_levels,
            ):
                emitted_to_any_destination = True
                yield item

            # Count once per event if written to any destination (not × N destinations)
            if emitted_to_any_destination:
                if case_ids:
                    metrics.output_tagged += 1
                else:
                    metrics.output_untagged += 1

    def _update_flink_metrics(self, metrics: WindowMetrics) -> None:
        """Update Flink distribution metrics (visible via REST API)."""
        t = metrics.timings
        self._window_duration_distribution.update(int(t.window_duration * 1000))
        self._matching_duration_distribution.update(t.matching.duration_ms())
        self._event_parsing_duration_dist.update(t.event_parsing.duration_ms())
        self._rules_loading_duration_dist.update(t.rules_loading.duration_ms())
        self._rules_parsing_duration_dist.update(t.rules_parsing.duration_ms())
        self._enrichment_emit_duration_dist.update(t.enrichment_emit.duration_ms())
        self._windows_processed_counter.inc()

        # Update error counters
        if metrics.errors.event_parsing > 0:
            self._event_parsing_errors_counter.inc(metrics.errors.event_parsing)
        if metrics.errors.schema_parsing > 0:
            self._schema_parsing_errors_counter.inc(metrics.errors.schema_parsing)
        if metrics.errors.rule_parsing > 0:
            self._rule_parsing_errors_counter.inc(metrics.errors.rule_parsing)
        if metrics.errors.matching > 0:
            self._matching_errors_counter.inc(metrics.errors.matching)

    def _log_window_metrics(self, metrics: WindowMetrics) -> None:
        """Log window processing metrics."""
        t = metrics.timings

        logger.info(
            "window timing",
            window_duration_seconds=round(t.window_duration, 3),
            window_throughput_eps=int(metrics.throughput(t.window_duration)),
            on_timer_duration_seconds=round(t.on_timer_duration, 3),
            on_timer_throughput_eps=int(metrics.throughput(t.on_timer_duration)),
            phase_event_parsing_sec=round(t.event_parsing.duration, 3),
            phase_rules_loading_sec=round(t.rules_loading.duration, 3),
            phase_rules_parsing_sec=round(t.rules_parsing.duration, 3),
            phase_matching_sec=round(t.matching.duration, 3),
            phase_enrichment_emit_sec=round(t.enrichment_emit.duration, 3),
            phase_event_parsing_pct=round(t.phase_percentage(t.event_parsing.duration), 1),
            phase_rules_loading_pct=round(t.phase_percentage(t.rules_loading.duration), 1),
            phase_rules_parsing_pct=round(t.phase_percentage(t.rules_parsing.duration), 1),
            phase_matching_pct=round(t.phase_percentage(t.matching.duration), 1),
            phase_enrichment_emit_pct=round(t.phase_percentage(t.enrichment_emit.duration), 1),
            matching_throughput_eps=int(metrics.throughput(t.matching.duration)),
        )

        logger.info(
            "window matches",
            matched_count=metrics.matched_events,
            total_count=metrics.total_events,
            prefiltered_count=metrics.prefiltered_events,
            match_rate_percent=round(metrics.match_rate, 1),
        )

        if metrics.matched_rules:
            top_rules = sorted(metrics.matched_rules.items(), key=lambda x: x[1], reverse=True)[:3]
            logger.info(
                "triggered rules",
                unique_rules=len(metrics.matched_rules),
                top_rules=[{"rule_id": rid, "count": cnt} for rid, cnt in top_rules],
            )

    def on_timer(self, timestamp: int, ctx):  # noqa: ANN001
        """
        Process window when timer fires.

        Pipeline: Events → Parse → Match → Enrich → Emit
        """
        metrics = WindowMetrics(timings=WindowTimings(on_timer_start=time.time()))
        current_key = ctx.get_current_key()
        bind_context(key=current_key)
        early_flush_timer_ts = self._early_flush_timer_ts_state.value()
        flushed_by_early_flush = (
            early_flush_timer_ts is not None and timestamp == early_flush_timer_ts
        )

        # ========== Parse events from state ==========
        metrics.timings.event_parsing.start = time.time()
        event_buffer, parsing_errors, _ = self._extract_and_deserialize_events_from_state()
        metrics.timings.event_parsing.end = time.time()
        metrics.errors.event_parsing = parsing_errors

        if not event_buffer:
            self._clear_window_state()
            return

        metrics.total_events = len(event_buffer)

        # ========== Load rules and config from broadcast state ==========
        logsource_config = self._get_logsource_config_from_state(
            ctx.get_broadcast_state(PARSER_STATE_DESCRIPTOR)
        )
        filters = self._get_filters_from_state(ctx.get_broadcast_state(FILTER_STATE_DESCRIPTOR))
        custom_fields = self._get_custom_fields_from_state(
            ctx.get_broadcast_state(CUSTOM_FIELDS_STATE_DESCRIPTOR)
        )
        sigmas = self._load_and_parse_rules(ctx, metrics)

        # With no rules nothing can match, so the window is only skippable when NO
        # destination wants unmatched traffic (all_events / unmatched_only would still
        # have to emit every buffered event).
        wants_unmatched = any(
            destination_accepts(d.output_mode, matched=False) for d in self.destinations
        )
        if not sigmas and not wants_unmatched:
            # Normally a startup race: the events source begins draining its backlog before the
            # rules broadcast has finished loading, so the first windows have nothing to match.
            metric_json = self._build_window_discarded_metric_json(
                "no_rules_matched_only_mode", len(event_buffer), current_key
            )
            if metric_json:
                yield METRICS_OUTPUT_TAG, metric_json
            self._clear_window_state()
            return

        logger.info(
            "window started",
            event_count=metrics.total_events,
            rule_count=metrics.rule_count,
            rules_cache_hit=metrics.rules_cache_hit,
        )

        # ========== Sigma matching ==========
        match_result = self._run_sigma_matching(event_buffer, sigmas, filters, logsource_config)

        if match_result is None:
            metrics.errors.matching += 1
            # This path returns before _update_flink_metrics, so the error counter has to be
            # advanced here or the failure never shows up anywhere.
            self._matching_errors_counter.inc(1)
            metric_json = self._build_window_discarded_metric_json(
                "matching_failed", len(event_buffer), current_key
            )
            if metric_json:
                yield METRICS_OUTPUT_TAG, metric_json
            self._clear_window_state()
            return

        metrics.timings.matching.start = match_result.start_time
        metrics.timings.matching.end = match_result.end_time
        metrics.errors.schema_parsing = match_result.schema_parser_errors

        # ========== Enrich and emit ==========
        metrics.timings.enrichment_emit.start = time.time()
        yield from self._enrich_and_emit_events(
            original_event_buffer=event_buffer,
            match_result=match_result,
            sigmas=sigmas,
            custom_fields=custom_fields,
            logsource_config=logsource_config,
            metrics=metrics,
        )
        metrics.timings.enrichment_emit.end = time.time()

        # ========== Finalize metrics ==========
        metrics.timings.on_timer_end = time.time()
        window_start_ms = self._window_start_time_state.value()
        metrics.timings.window_start = (
            window_start_ms / 1000.0 if window_start_ms else metrics.timings.on_timer_start
        )

        # Update per-key EPS gauges (instance-level gauge = sum across all keys).
        # Integer EPS for thread-mode REST visibility — see open() gauge registration.
        key = str(current_key)
        window_duration = metrics.timings.window_duration
        if window_duration > 0:
            self._input_eps_per_key[key] = int(round(metrics.total_events / window_duration))
            self._output_tagged_eps_per_key[key] = int(
                round(metrics.output_tagged / window_duration)
            )
            self._output_untagged_eps_per_key[key] = int(
                round(metrics.output_untagged / window_duration)
            )
        else:
            self._input_eps_per_key[key] = 0
            self._output_tagged_eps_per_key[key] = 0
            self._output_untagged_eps_per_key[key] = 0
        # Update per-key last window timestamp (instance-level gauge = max across keys)
        self._last_window_ts_per_key[key] = int(time.time() * 1000)

        # Update Flink metrics and log results
        self._update_flink_metrics(metrics)
        self._log_window_metrics(metrics)

        # Emit window metrics to Kafka (always when events were processed)
        if metrics.total_events > 0:
            metric_json = self._create_metrics_from_window(
                metrics,
                current_key=current_key,
                flushed_by_early_flush=flushed_by_early_flush,
            )
            if metric_json:
                yield METRICS_OUTPUT_TAG, metric_json
        # Emit init metric once per instance on first window if it wasn't sent
        elif not self._init_metric_sent:
            metric_json = self._build_init_metric_json()
            if metric_json is not None:
                yield METRICS_OUTPUT_TAG, metric_json
                self._init_metric_sent = True

        self._clear_window_state()

    def _create_metrics_from_window(
        self,
        metrics: WindowMetrics,
        current_key: str,
        flushed_by_early_flush: bool,
    ) -> str | None:
        """
        Create per-rule metrics message for Kafka (consumed by detectflow-backend).

        The detectflow-backend uses this to populate pipeline_rule_metrics table
        which tracks per-rule match counts. Only essential fields are included.

        Args:
            metrics: WindowMetrics containing match data

        Returns:
            JSON metric string for detectflow-backend, or None on error
        """
        sorted_rules = sorted(metrics.matched_rules.items(), key=lambda x: x[1], reverse=True)
        window_start_timestamp = (
            int(metrics.timings.window_start * 1000) if metrics.timings.window_start > 0 else None
        )
        window_on_timer_duration_ms = metrics.timings.event_parsing.duration_ms()
        window_on_timer_duration_ms += metrics.timings.rules_loading.duration_ms()
        window_on_timer_duration_ms += metrics.timings.rules_parsing.duration_ms()
        window_on_timer_duration_ms += metrics.timings.matching.duration_ms()
        window_on_timer_duration_ms += metrics.timings.enrichment_emit.duration_ms()

        metric = {
            # Identification
            "job_id": self.job_id,
            "instance_id": self._instance_id,
            "key": str(current_key),
            "timestamp": int(time.time() * 1000),
            "detectflow_matchnode_version": get_version(),
            # Window metrics
            # Window metrics (essential for admin-panel)
            "window_start_timestamp": window_start_timestamp,
            "window_on_timer_duration_ms": window_on_timer_duration_ms,
            "window_flushed_by_early_flush": flushed_by_early_flush,
            "window_total_events": metrics.total_events,
            "window_matched_events": metrics.matched_events,
            # Per-rule tracking (critical for pipeline_rule_metrics table)
            "rules": {
                "matched_rules": [
                    {"rule_id": rid, "window_matches": cnt} for rid, cnt in sorted_rules
                ],
            },
        }

        try:
            return orjson.dumps(metric).decode("utf-8")
        except Exception as e:
            logger.error("metric serialization failed", error=str(e), job_id=self.job_id)
            return None
