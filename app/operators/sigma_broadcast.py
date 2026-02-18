"""
Flink Sigma Matcher with BroadcastState Pattern
Supports parallelism by broadcasting rules to all parallel instances
"""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass, field
from typing import NamedTuple

import orjson
import yaml
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
from app.domain.filters.loader import Filter, convert_filter_kafka_event_to_filter
from app.domain.logsources.loader import LogsourceConfig, convert_kafka_event_to_logsource_config
from app.domain.rules.loader import (
    load_sigmas_from_tdm_dict,
)
from app.domain.sigma_matcher.helpers import process_events

logger = get_logger(__name__)


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


class SigmaMatcherBroadcastFunction(KeyedBroadcastProcessFunction):
    """
    Processes events with broadcast Sigma rules.
    Rules are broadcast to all parallel instances via BroadcastState.

    Multi-tenancy support:
    - Filters rules by job_id
    - Supports output_mode (all_events / matched_only)

    Metrics:
    - Flink counters available via REST API (total_events, matched_events)
    - Per-rule metrics emitted to Kafka via side output (for API tracking)
    """

    def __init__(
        self,
        window_size_seconds: int,
        job_id: str,
        output_mode: str,
        apply_parser_to_output_events: bool = False,
    ):
        self.window_size_ms = window_size_seconds * 1000
        self.window_size_seconds = window_size_seconds

        # Multi-tenancy parameters
        self.job_id = job_id
        self.output_mode = output_mode  # "all_events" or "matched_only"
        self.apply_parser_to_output_events = apply_parser_to_output_events

        # Operator-local caches (per subtask, shared across keys)
        # Rebuilt on first window after restart - fast enough
        # Parsed rules cache (EnrichedSigma objects - expensive to create)
        self._enriched_sigmas_cache = None
        self._enrichment_map_cache = None
        self._rules_signature = None
        # Rules data cache (raw rules from broadcast state - expensive to iterate)
        self._rules_data_cache = None
        self._rules_dirty = True  # Start dirty so first window reads from state

        # Flink-managed state (checkpointed, per-key)
        self._event_buffer_state = None
        self._window_start_time_state = None

        # Runtime context info (filled in open())
        self._instance_id = None
        self._runtime_context = None
        self._parallelism = None
        self._hostname = None

        # Flink metrics (registered in open(), available via REST API)
        # Event counters as gauges (PyFlink counters don't expose via REST API)
        self._matched_events_count = [0]
        self._total_events_count = [0]
        # Error/processing counters (internal use only)
        self._event_parsing_errors_counter = None
        self._schema_parsing_errors_counter = None
        self._rule_parsing_errors_counter = None
        self._matching_errors_counter = None
        self._windows_processed_counter = None
        # Distributions (for timing stats)
        self._window_duration_distribution = None
        self._matching_duration_distribution = None
        # Mutable container for gauge value (gauges need a callable)
        self._active_rules_count = [0]

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

        # Bind instance_id to all logs in this task
        bind_context(instance_id=self._instance_id)

        # Register custom Flink metrics (available via REST API at /jobs/{id}/vertices/{id}/subtasks/metrics)
        metrics_group = runtime_context.get_metrics_group()

        # Event counters as gauges (PyFlink counters don't expose via REST API, but gauges do)
        # Using mutable lists to allow lambda capture
        self._matched_events_count = [0]
        self._total_events_count = [0]
        metrics_group.gauge("matchedEvents", lambda: self._matched_events_count[0])
        metrics_group.gauge("totalEvents", lambda: self._total_events_count[0])

        # Error counters (keep as counters for internal use, not exposed via REST API)
        self._event_parsing_errors_counter = metrics_group.counter("eventParsingErrors")
        self._schema_parsing_errors_counter = metrics_group.counter("schemaParsingErrors")
        self._rule_parsing_errors_counter = metrics_group.counter("ruleParsingErrors")
        self._matching_errors_counter = metrics_group.counter("matchingErrors")

        # Processing counters
        self._windows_processed_counter = metrics_group.counter("windowsProcessed")

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
            job_id=self.job_id,
            window_size_seconds=self.window_size_seconds,
            output_mode=self.output_mode,
            rules_source="broadcast_stream",
            pid=os.getpid(),
            instance_id=self._instance_id,
            event_buffer_checkpointed=True,
            custom_metrics=[
                "matchedEvents",
                "totalEvents",
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
                "activeRulesCount",
                "windowDurationMs",
                "matchingDurationMs",
            ],
        )

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
            if state.contains(filter_id):
                state.remove(filter_id)
                logger.info("filter deleted", filter_id=filter_id)
        else:
            filter_json = orjson.dumps(event).decode("utf-8")
            state.put(filter_id, filter_json)
            logger.info("filter updated", filter_id=filter_id)

    def _process_rule_event(self, event: dict, state: BroadcastState) -> None:
        rule_id = event.get("case", {}).get("id")

        if not rule_id:
            logger.warning("rule missing id", message="Received rule without case.id")
            return

        # Delete only when sigma.text is empty (logical delete)
        empty_sigma = not event.get("sigma", {}).get("text")
        if empty_sigma:
            if state.contains(rule_id):
                state.remove(rule_id)
                # Invalidate rules cache - rules changed
                self._rules_dirty = True
                logger.info("rule deleted", rule_id=rule_id, job_id=self.job_id)
            return

        rule_json = orjson.dumps(event).decode("utf-8")
        state.put(rule_id, rule_json)
        # Invalidate rules cache - rules changed
        self._rules_dirty = True
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
        for filter_id in state.keys():
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

    def _load_rules_from_state(self, ctx) -> tuple[list[dict], int, str, float]:
        """
        Load rules from broadcast state with caching (P1 optimization).

        Returns:
            Tuple of (rules_data, rule_count, signature, loading_duration)
            Returns ([], 0, "", duration) on error
        """
        start_time = time.time()

        # Check if we can use cached rules_data (P1 optimization)
        use_cached = (
            not self._rules_dirty
            and self._rules_data_cache is not None
            and self._rules_signature is not None
        )

        if use_cached:
            rules_data = self._rules_data_cache
            rule_count = len(rules_data)
            signature = self._rules_signature
            logger.debug("using cached rules_data", rule_count=rule_count)
        else:
            rules_state = ctx.get_broadcast_state(RULES_STATE_DESCRIPTOR)
            rules_data = []
            rule_count = 0
            hasher = hashlib.sha1()

            try:
                rule_ids = list(rules_state.keys())
                for rule_id in sorted(rule_ids):
                    rule_json = rules_state.get(rule_id)
                    if rule_json:
                        hasher.update(rule_id.encode("utf-8"))
                        hasher.update(b"\0")
                        hasher.update(rule_json.encode("utf-8"))
                        rules_data.append(orjson.loads(rule_json))
                        rule_count += 1
            except Exception as e:
                logger.exception("broadcast state read error", error=str(e))
                return [], 0, "", time.time() - start_time

            signature = hasher.hexdigest()
            self._rules_data_cache = rules_data
            self._rules_dirty = False
            logger.debug("rules_data cache updated", rule_count=rule_count)

        duration = time.time() - start_time
        return rules_data, rule_count, signature, duration

    def _parse_rules_with_cache(
        self,
        rules_data: list[dict],
        current_signature: str,
    ) -> tuple[list, dict, list, bool] | None:
        """
        Parse rules from JSON with P0 caching optimization.

        Returns:
            Tuple of (enriched_sigmas, enrichment_map, sigma_objects, cache_hit)
            or None if parsing failed
        """
        use_cached = (
            self._rules_signature == current_signature and self._enriched_sigmas_cache is not None
        )

        if use_cached:
            enriched_sigmas = self._enriched_sigmas_cache
            enrichment_map = self._enrichment_map_cache
            sigma_objects = [enriched.sigma for enriched in enriched_sigmas]
            logger.debug("using cached rules", signature=current_signature[:8])
            return enriched_sigmas, enrichment_map, sigma_objects, True

        try:
            enriched_sigmas = load_sigmas_from_tdm_dict(rules_data)
        except Exception as e:
            logger.exception("rules parsing failed", error=str(e))
            return None

        if not enriched_sigmas:
            logger.warning("no valid rules", message="No valid rules after parsing")
            return None

        enrichment_map = {
            enriched.case_id: enriched.get_enrichment_data() for enriched in enriched_sigmas
        }
        sigma_objects = [enriched.sigma for enriched in enriched_sigmas]

        # Update cache
        self._enriched_sigmas_cache = enriched_sigmas
        self._enrichment_map_cache = enrichment_map
        self._rules_signature = current_signature
        logger.debug("rules cache updated", signature=current_signature[:8])

        return enriched_sigmas, enrichment_map, sigma_objects, False

    def _run_sigma_matching(
        self,
        event_buffer: list[dict],
        sigma_objects: list,
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
                sigmas=sigma_objects,
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
            )
        except Exception as e:
            logger.exception("sigma matching error", error=str(e))
            return None

    def _enrich_and_emit_events(
        self,
        event_buffer: list[dict],
        case_ids_per_event: list[list[str]],
        enrichment_map: dict,
        custom_fields: dict | None,
        metrics: WindowMetrics,
    ):
        """
        Enrich matched events and yield to output.

        Updates metrics.matched_events and metrics.matched_rules in place.
        Yields enriched events as JSON strings.
        """
        for event, case_ids in zip(event_buffer, case_ids_per_event, strict=False):
            if custom_fields:
                event.update(custom_fields)

            # Track matched rules
            if case_ids:
                for case_id in case_ids:
                    metrics.matched_rules[case_id] = metrics.matched_rules.get(case_id, 0) + 1

            # Update Flink metrics BEFORE output filtering (gauges exposed via REST API)
            self._total_events_count[0] += 1
            if case_ids:
                metrics.matched_events += 1
                self._matched_events_count[0] += 1

            # Output mode filtering (AFTER counter update)
            if self.output_mode == "matched_only" and not case_ids:
                continue

            # Add detection info
            if case_ids:
                event["sigma_rule_ids"] = case_ids
                event["sigma_detections"] = [
                    enrichment_map.get(cid) for cid in case_ids if enrichment_map.get(cid)
                ]
            else:
                event["sigma_rule_ids"] = []
                event["sigma_detections"] = []

            try:
                yield orjson.dumps(event).decode("utf-8")
            except Exception as e:
                logger.error("event serialization failed", error=str(e))

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
        bind_context(key=ctx.get_current_key())

        # ========== PHASE 1: Parse events from state ==========
        metrics.timings.event_parsing.start = time.time()
        event_buffer, parsing_errors, _ = self._extract_and_deserialize_events_from_state()
        metrics.timings.event_parsing.end = time.time()
        metrics.errors.event_parsing = parsing_errors

        if not event_buffer:
            self._clear_window_state()
            return

        metrics.total_events = len(event_buffer)

        # ========== PHASE 2: Load rules and config from broadcast state ==========
        metrics.timings.rules_loading.start = time.time()
        logsource_config = self._get_logsource_config_from_state(
            ctx.get_broadcast_state(PARSER_STATE_DESCRIPTOR)
        )
        filters = self._get_filters_from_state(ctx.get_broadcast_state(FILTER_STATE_DESCRIPTOR))
        custom_fields = self._get_custom_fields_from_state(
            ctx.get_broadcast_state(CUSTOM_FIELDS_STATE_DESCRIPTOR)
        )
        rules_data, rule_count, signature, _ = self._load_rules_from_state(ctx)
        metrics.timings.rules_loading.end = time.time()

        if not rules_data:
            logger.warning(
                "window skipped", reason="waiting_for_rules", buffered_events=len(event_buffer)
            )
            self._clear_window_state()
            return

        metrics.rule_count = rule_count
        self._active_rules_count[0] = rule_count

        # ========== PHASE 3: Parse rules (with P0 caching) ==========
        metrics.timings.rules_parsing.start = time.time()
        parse_result = self._parse_rules_with_cache(rules_data, signature)
        metrics.timings.rules_parsing.end = time.time()

        if parse_result is None:
            metrics.errors.rule_parsing += 1
            self._clear_window_state()
            return

        _, enrichment_map, sigma_objects, cache_hit = parse_result
        metrics.rules_cache_hit = cache_hit

        logger.info(
            "window started",
            event_count=metrics.total_events,
            rule_count=rule_count,
            rules_cache_hit=cache_hit,
        )

        # ========== PHASE 4: Sigma matching ==========
        match_result = self._run_sigma_matching(
            event_buffer, sigma_objects, filters, logsource_config
        )

        if match_result is None:
            metrics.errors.matching += 1
            self._clear_window_state()
            return

        metrics.timings.matching.start = match_result.start_time
        metrics.timings.matching.end = match_result.end_time
        metrics.errors.schema_parsing = match_result.schema_parser_errors

        # ========== PHASE 5: Enrich and emit ==========
        metrics.timings.enrichment_emit.start = time.time()
        yield from self._enrich_and_emit_events(
            event_buffer=match_result.parsed_events
            if self.apply_parser_to_output_events
            else event_buffer,
            case_ids_per_event=match_result.case_ids_per_event,
            enrichment_map=enrichment_map,
            custom_fields=custom_fields,
            metrics=metrics,
        )
        metrics.timings.enrichment_emit.end = time.time()

        # ========== Finalize metrics ==========
        metrics.timings.on_timer_end = time.time()
        window_start_ms = self._window_start_time_state.value()
        metrics.timings.window_start = (
            window_start_ms / 1000.0 if window_start_ms else metrics.timings.on_timer_start
        )

        # Update Flink metrics and log results
        self._update_flink_metrics(metrics)
        self._log_window_metrics(metrics)

        # Emit per-rule metrics to Kafka (if there were matches)
        if metrics.matched_rules:
            metric_json = self._create_metrics_from_window(metrics)
            if metric_json:
                yield METRICS_OUTPUT_TAG, metric_json

        self._clear_window_state()

    def _create_metrics_from_window(self, metrics: WindowMetrics) -> str | None:
        """
        Create per-rule metrics message for Kafka (consumed by admin-panel-backend).

        The admin-panel-backend uses this to populate pipeline_rule_metrics table
        which tracks per-rule match counts. The critical field is rules.matched_rules.

        Args:
            metrics: WindowMetrics containing all timing and match data

        Returns:
            JSON metric string matching KafkaMetricMessage schema, or None on error
        """
        t = metrics.timings
        sorted_rules = sorted(metrics.matched_rules.items(), key=lambda x: x[1], reverse=True)

        # Calculate per-rule timing approximation
        avg_time_per_rule_ms = (
            (t.matching.duration * 1000 / metrics.rule_count) if metrics.rule_count > 0 else 0
        )

        # Build slow rules estimation (rules that matched most = proxy for slowest)
        # TODO: this metric is not correct, delete it
        slow_rules_top_3 = [
            {
                "rule_id": rid,
                "match_count": cnt,
                "estimated_time_ms": round(avg_time_per_rule_ms * (cnt / metrics.matched_events), 2)
                if metrics.matched_events > 0
                else 0,
            }
            for rid, cnt in sorted_rules[:3]
        ]

        metric = {
            # Identification
            "job_id": self.job_id,
            "instance_id": self._instance_id,
            "timestamp": int(time.time() * 1000),
            # Window metrics
            "window_total_events": metrics.total_events,
            "window_matched_events": metrics.matched_events,
            "window_match_rate_percent": round(metrics.match_rate, 2),
            "window_start_time": round(t.window_start, 3),
            "window_end_time": round(t.on_timer_end, 3),
            "window_duration_seconds": round(t.window_duration, 3),
            "window_throughput_eps": int(metrics.throughput(t.window_duration)),
            # on_timer timing
            "on_timer_start_time": round(t.on_timer_start, 3),
            "on_timer_end_time": round(t.on_timer_end, 3),
            "on_timer_duration_seconds": round(t.on_timer_duration, 3),
            "on_timer_throughput_eps": int(metrics.throughput(t.on_timer_duration)),
            # Phase timings (all 5 phases)
            "phase_event_parsing_seconds": round(t.event_parsing.duration, 3),
            "phase_event_parsing_pct": round(t.phase_percentage(t.event_parsing.duration), 1),
            "phase_rules_loading_seconds": round(t.rules_loading.duration, 3),
            "phase_rules_loading_pct": round(t.phase_percentage(t.rules_loading.duration), 1),
            "phase_rules_parsing_seconds": round(t.rules_parsing.duration, 3),
            "phase_rules_parsing_pct": round(t.phase_percentage(t.rules_parsing.duration), 1),
            "phase_matching_seconds": round(t.matching.duration, 3),
            "phase_matching_pct": round(t.phase_percentage(t.matching.duration), 1),
            "phase_matching_throughput_eps": int(metrics.throughput(t.matching.duration)),
            "phase_enrichment_emit_seconds": round(t.enrichment_emit.duration, 3),
            "phase_enrichment_emit_pct": round(t.phase_percentage(t.enrichment_emit.duration), 1),
            # Errors
            "errors": metrics.errors.to_dict(),
            # Rules - critical for admin-panel-backend per-rule tracking
            "rules": {
                "total": metrics.rule_count,
                "triggered_unique": len(metrics.matched_rules),
                "top_by_matches": [{"rule_id": rid, "count": cnt} for rid, cnt in sorted_rules[:3]],
                "slow_rules_top_3": slow_rules_top_3,
                "avg_time_per_rule_ms": round(avg_time_per_rule_ms, 2),
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
