import re
from unittest.mock import patch

import orjson

from app.config.settings import OutputDestination
from app.config.version import get_version
from app.domain.logsources.loader import LogsourceConfig
from app.domain.sigma_matcher.sigma_parser import Sigma
from app.operators import sigma_broadcast as sigma_broadcast_module
from app.operators.sigma_broadcast import (
    DETECTIONS_OUTPUT_TAG,
    TAGGED_EVENTS_OUTPUT_TAG,
    SigmaMatcherBroadcastFunction,
    SigmaMatchingResult,
    WindowMetrics,
    WindowTimings,
)

VALID_SIGMA_TEXT = """detection:
  condition: selection
  selection:
    EventID: 4624
"""


class FakeBroadcastState:
    def __init__(self, data: dict[str, str]):
        self._data = data

    def keys(self):
        return list(self._data.keys())

    def get(self, key: str):
        return self._data.get(key)


class FakeContext:
    def __init__(self, state: FakeBroadcastState):
        self._state = state

    def get_broadcast_state(self, _descriptor):
        return self._state


def _make_rule_json(case_id: str, technique=None) -> str:
    rule = {
        "case": {"id": case_id, "name": f"Rule {case_id}"},
        "sigma": {"text": VALID_SIGMA_TEXT, "level": "medium"},
    }
    if technique is not None:
        rule["tags"] = {"technique": technique}
    return orjson.dumps(rule).decode("utf-8")


def _make_operator(
    destinations: list[OutputDestination] | None = None,
    keep_filtered_events: bool = False,
    emit_tagged_events: bool = True,
) -> SigmaMatcherBroadcastFunction:
    op = SigmaMatcherBroadcastFunction(
        window_size_seconds=60,
        window_count_threshold=100,
        job_id="job-1",
        destinations=destinations
        or [
            OutputDestination(
                topic="detectflow.detections",
                output_mode="all_events",
            )
        ],
        keep_filtered_events=keep_filtered_events,
        emit_tagged_events=emit_tagged_events,
    )
    # In runtime this is registered in open(); for unit tests we emulate it.
    op._active_rules_count = [0]
    return op


def _destination_emits(out: list) -> list[tuple[str, str]]:
    """``(topic, json)`` pairs from the shared detections side output."""
    return [(row[0], row[1]) for tag, row in out if tag == DETECTIONS_OUTPUT_TAG]


def _destination_payloads(out: list) -> list[str]:
    """JSON payloads emitted to the detections side output (not tagged-events)."""
    return [payload for _, payload in _destination_emits(out)]


def _payloads_by_topic(out: list) -> dict[str, list[dict]]:
    by_topic: dict[str, list[dict]] = {}
    for topic, payload in _destination_emits(out):
        by_topic.setdefault(topic, []).append(orjson.loads(payload))
    return by_topic


def _tagged_payloads(out: list) -> list[tuple]:
    return [item for item in out if item[0] is TAGGED_EVENTS_OUTPUT_TAG]


def _make_logsource_config() -> LogsourceConfig:
    return LogsourceConfig(
        hostname_fields=["host.name"],
        timestamp_fields=["@timestamp"],
    )


def _make_match_result(
    events: list[dict],
    case_ids_per_event: list[list[str]],
    prefiltered_mask: list[bool] | None = None,
    parsed_events: list[dict] | None = None,
) -> SigmaMatchingResult:
    return SigmaMatchingResult(
        case_ids_per_event=case_ids_per_event,
        start_time=0.0,
        end_time=0.0,
        parsed_events=(
            parsed_events if parsed_events is not None else [event.copy() for event in events]
        ),
        prefiltered_mask=prefiltered_mask,
    )


def test_read_rules_from_broadcast_signature_is_order_independent():
    state_a = FakeBroadcastState(
        {
            "b": _make_rule_json("b"),
            "a": _make_rule_json("a"),
        }
    )
    state_b = FakeBroadcastState(
        {
            "a": _make_rule_json("a"),
            "b": _make_rule_json("b"),
        }
    )

    rules_a, signature_a = SigmaMatcherBroadcastFunction._read_rules_from_broadcast(state_a)
    rules_b, signature_b = SigmaMatcherBroadcastFunction._read_rules_from_broadcast(state_b)

    assert signature_a == signature_b
    assert len(rules_a) == len(rules_b) == 2


def test_load_and_parse_rules_uses_hot_cache_when_state_unchanged():
    op = _make_operator()
    ctx = FakeContext(FakeBroadcastState({"r1": _make_rule_json("r1")}))

    first_metrics = WindowMetrics(timings=WindowTimings())
    first_sigmas = op._load_and_parse_rules(ctx, first_metrics)

    assert len(first_sigmas) == 1
    assert first_metrics.rules_cache_hit is False
    assert first_metrics.rule_count == 1
    assert op._active_rules_count[0] == 1

    second_metrics = WindowMetrics(timings=WindowTimings())
    second_sigmas = op._load_and_parse_rules(ctx, second_metrics)

    assert second_sigmas is first_sigmas
    assert second_metrics.rules_cache_hit is True
    assert second_metrics.rule_count == 1
    assert op._active_rules_count[0] == 1


def test_load_and_parse_rules_reuses_cache_for_same_signature_after_state_change():
    op = _make_operator()
    state = FakeBroadcastState({"r1": _make_rule_json("r1")})
    ctx = FakeContext(state)

    first_metrics = WindowMetrics(timings=WindowTimings())
    first_sigmas = op._load_and_parse_rules(ctx, first_metrics)
    assert len(first_sigmas) == 1

    # Emulate rule update signal with content unchanged.
    op._rules_state_changed = True

    second_metrics = WindowMetrics(timings=WindowTimings())
    second_sigmas = op._load_and_parse_rules(ctx, second_metrics)

    assert second_sigmas is first_sigmas
    assert second_metrics.rules_cache_hit is True
    assert second_metrics.rule_count == 1


def test_load_and_parse_rules_allows_empty_rules_in_all_events_flow():
    op = _make_operator()
    ctx = FakeContext(FakeBroadcastState({}))

    metrics = WindowMetrics(timings=WindowTimings())
    sigmas = op._load_and_parse_rules(ctx, metrics)

    assert sigmas == []
    assert metrics.rule_count == 0
    assert op._active_rules_count[0] == 0


# =============================================================================
# Filtered events behavior (keep_filtered_events, prefiltered_events metric)
# =============================================================================


def test_enrich_and_emit_drops_prefiltered_when_keep_filtered_events_false():
    """When keep_filtered_events=False, prefiltered events are not yielded."""
    op = _make_operator(keep_filtered_events=False)

    events = [
        {"e": "matched"},
        {"e": "filtered"},
        {"e": "unmatched"},
    ]
    case_ids_per_event = [["r1"], [], []]  # 0=matched, 1=filtered, 2=unmatched
    prefiltered_mask = [False, True, False]
    metrics = WindowMetrics(timings=WindowTimings())
    sigmas = []  # no rules needed for this test

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=case_ids_per_event,
                prefiltered_mask=prefiltered_mask,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    main_outputs = _destination_payloads(out)
    assert len(main_outputs) == 2  # matched + unmatched, filtered dropped
    assert metrics.prefiltered_events == 1
    assert op._prefiltered_events_count[0] == 1
    # First emitted is matched (carries detectflow), second is untagged (none)
    assert "detectflow" in orjson.loads(main_outputs[0])
    assert "detectflow" not in orjson.loads(main_outputs[1])


def test_enrich_and_emit_keeps_prefiltered_when_keep_filtered_events_true():
    """When keep_filtered_events=True, prefiltered events are yielded as untagged."""
    op = _make_operator(keep_filtered_events=True)

    events = [
        {"e": "matched"},
        {"e": "filtered"},
        {"e": "unmatched"},
    ]
    case_ids_per_event = [["r1"], [], []]
    prefiltered_mask = [False, True, False]
    metrics = WindowMetrics(timings=WindowTimings())
    sigmas = []

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=case_ids_per_event,
                prefiltered_mask=prefiltered_mask,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    main_outputs = _destination_payloads(out)
    assert len(main_outputs) == 3
    assert metrics.prefiltered_events == 1
    assert op._prefiltered_events_count[0] == 1
    assert "detectflow" in orjson.loads(main_outputs[0])
    assert "detectflow" not in orjson.loads(main_outputs[1])  # prefiltered but kept
    assert "detectflow" not in orjson.loads(main_outputs[2])


def test_enrich_and_emit_prefiltered_events_metric_with_no_prefiltered_mask():
    """prefiltered_events stays 0 when prefiltered_mask is None."""
    op = _make_operator()
    events = [{"e": "a"}, {"e": "b"}]
    case_ids_per_event = [[], []]
    metrics = WindowMetrics(timings=WindowTimings())

    list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=case_ids_per_event,
            ),
            sigmas=[],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    assert metrics.prefiltered_events == 0
    assert op._prefiltered_events_count[0] == 0


def test_enrich_and_emit_adds_sigma_uuid_to_tagged_events():
    op = _make_operator()
    events = [{"e": "matched"}]
    parsed_events = [{"event": {"code": "1"}, "host": {"name": "h-1"}, "e": "parsed"}]
    case_ids_per_event = [["r1"]]
    metrics = WindowMetrics(timings=WindowTimings())
    sigmas = [
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r1",
            title="Rule r1",
            level="high",
            sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
        )
    ]

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=case_ids_per_event,
                parsed_events=parsed_events,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    main_outputs = _destination_payloads(out)
    assert len(main_outputs) == 1
    result = orjson.loads(main_outputs[0])
    match = result["detectflow"]["matches"][0]
    assert match["_rule_id"] == "r1"
    assert match["sigma_rule_uuid"] == "2f958f09-99cf-4af8-8d26-7de834541f07"

    tagged_outputs = _tagged_payloads(out)
    assert len(tagged_outputs) == 1
    _, payload = tagged_outputs[0]
    envelope = orjson.loads(payload)
    assert envelope["parsed_event"] == parsed_events[0]


def test_detectflow_block_schema_metadata_and_techniques():
    """detectflow block: exact match-field set (rename + techniques[{id,name}] drop),
    mitre_techniques mapping + [] default, severity/rule_name, processed_at shared per
    window, and matchnode_version from the package."""
    op = _make_operator()
    events = [{"e": "with-tech"}, {"e": "no-tech"}]
    parsed_events = [
        {"event": {"code": "1"}, "host": {"name": "h-1"}, "@timestamp": "2026-05-26T00:00:00Z"},
        {"event": {"code": "2"}, "host": {"name": "h-2"}, "@timestamp": "2026-05-26T00:00:01Z"},
    ]
    case_ids_per_event = [["r1"], ["r2"]]
    sigmas = [
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r1",
            title="Rule r1",
            level="high",
            techniques=[{"id": "T1003", "name": "OS Credential Dumping"}],
            sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
        ),
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r2",
            title="Rule r2",
            level="low",
            sigma_uuid="3a0699aa-1234-4af8-8d26-7de834541f08",
        ),
    ]

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=case_ids_per_event,
                parsed_events=parsed_events,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )

    main_outputs = [orjson.loads(item) for item in _destination_payloads(out)]
    assert len(main_outputs) == 2
    df1 = main_outputs[0]["detectflow"]
    df2 = main_outputs[1]["detectflow"]

    # Exact match schema — locks the rename and the drop of the old techniques[{id,name}] list.
    m1 = df1["matches"][0]
    assert set(m1) == {"sigma_rule_uuid", "_rule_id", "rule_name", "severity", "mitre_techniques"}
    assert "techniques" not in m1
    assert m1["_rule_id"] == "r1"
    assert m1["rule_name"] == "Rule r1"
    assert m1["severity"] == "high"
    assert m1["mitre_techniques"] == ["T1003"]

    # A rule without techniques → mitre_techniques defaults to [].
    m2 = df2["matches"][0]
    assert m2["mitre_techniques"] == []
    assert m2["severity"] == "low"

    # processed_at is computed once per window (shared by every event); version from package.
    assert df1["processed_at"] == df2["processed_at"]
    assert df1["matchnode_version"] == get_version()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", df1["processed_at"])

    # Single-rule events: primary_match is that rule; detectflow.severity mirrors it.
    assert set(df1) == {
        "processed_at",
        "matchnode_version",
        "severity",
        "primary_match",
        "matches",
    }
    assert df1["severity"] == "high"
    assert df1["primary_match"] == m1
    assert df2["severity"] == "low"
    assert df2["primary_match"] == m2


def test_emit_tagged_events_outputs_only_matched_compact_payload():
    op = _make_operator()
    event_buffer = [
        {"source": "raw", "event": 1, "message": "original"},
        {"source": "raw", "event": 2},
    ]
    # parsed_events differ from the raw buffer so we can assert the envelope
    # carries the ECS-normalized parsed form, not the raw winlogbeat blob.
    parsed_events = [
        {"event": {"code": "1"}, "host": {"name": "h-1"}, "@timestamp": "2026-05-26T00:00:00Z"},
        {"event": {"code": "2"}, "host": {"name": "h-2"}},
    ]
    case_ids_per_event = [["r1"], []]  # second event is untagged
    sigmas = [
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r1",
            title="Rule r1",
            level="high",
            sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
        )
    ]

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=event_buffer,
            match_result=_make_match_result(
                events=event_buffer,
                case_ids_per_event=case_ids_per_event,
                parsed_events=parsed_events,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )

    tagged_outputs = _tagged_payloads(out)
    assert len(tagged_outputs) == 1
    output_tag, payload = tagged_outputs[0]
    assert output_tag is TAGGED_EVENTS_OUTPUT_TAG
    result = orjson.loads(payload)
    assert set(result) == {
        "parsed_event",
        "pipeline_id",
        "timestamp",
        "hostname",
        "detectflow",
    }
    assert result["parsed_event"] == parsed_events[0]
    assert "source_topic" not in result
    assert "source_topics" not in result
    assert result["pipeline_id"] == "job-1"
    assert result["timestamp"].startswith("2026-05-26T00:00:00")
    assert result["hostname"] == "h-1"
    detectflow = result["detectflow"]
    assert set(detectflow) == {
        "processed_at",
        "matchnode_version",
        "severity",
        "primary_match",
        "matches",
    }
    match = detectflow["matches"][0]
    assert match["sigma_rule_uuid"] == "2f958f09-99cf-4af8-8d26-7de834541f07"
    assert match["_rule_id"] == "r1"
    assert match["rule_name"] == "Rule r1"


def test_no_tagged_output_when_emit_tagged_events_false():
    """No tagged-events side output at all, but the main detections output is
    byte-identical: the detectflow block is still built and attached."""
    op = _make_operator(emit_tagged_events=False)
    event_buffer = [
        {"source": "raw", "event": 1, "message": "original"},
        {"source": "raw", "event": 2},
    ]
    parsed_events = [
        {"event": {"code": "1"}, "host": {"name": "h-1"}, "@timestamp": "2026-05-26T00:00:00Z"},
        {"event": {"code": "2"}, "host": {"name": "h-2"}},
    ]
    case_ids_per_event = [["r1"], []]
    sigmas = [
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r1",
            title="Rule r1",
            level="high",
            sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
        )
    ]

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=event_buffer,
            match_result=_make_match_result(
                events=event_buffer,
                case_ids_per_event=case_ids_per_event,
                parsed_events=parsed_events,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )

    assert _tagged_payloads(out) == []

    # Main detections output unchanged (hard constraint 2).
    first_document = orjson.loads(_destination_payloads(out)[0])
    detectflow = first_document["detectflow"]
    assert set(detectflow) == {
        "processed_at",
        "matchnode_version",
        "severity",
        "primary_match",
        "matches",
    }
    assert detectflow["matches"][0]["sigma_rule_uuid"] == "2f958f09-99cf-4af8-8d26-7de834541f07"


def test_tagged_event_json_not_built_when_disabled(mocker):
    """The saving is the serialization, not only the sink: the per-event JSON
    document must never be built when the side output is disabled."""
    op = _make_operator(emit_tagged_events=False)
    create_json = mocker.patch.object(op, "_create_tagged_event_json")
    event_buffer = [{"source": "raw", "event": 1}]
    parsed_events = [{"event": {"code": "1"}, "host": {"name": "h-1"}}]
    sigmas = [
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r1",
            title="Rule r1",
            level="high",
            sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
        )
    ]

    list(
        op._enrich_and_emit_events(
            original_event_buffer=event_buffer,
            match_result=_make_match_result(
                events=event_buffer,
                case_ids_per_event=[["r1"]],
                parsed_events=parsed_events,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )

    create_json.assert_not_called()


def test_warns_once_when_hostname_fields_missing():
    """Tagged events without hostname/timestamp are dropped by correlation while
    this node's metrics stay green, so warn — but only once per subtask."""
    op = _make_operator()
    logsource_config = LogsourceConfig(hostname_fields=[], timestamp_fields=[])
    event_buffer = [{"source": "raw", "event": 1}]
    parsed_events = [{"event": {"code": "1"}}]
    sigmas = [
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r1",
            title="Rule r1",
            level="high",
            sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
        )
    ]

    def _drain_one_window() -> None:
        list(
            op._enrich_and_emit_events(
                original_event_buffer=event_buffer,
                match_result=_make_match_result(
                    events=event_buffer,
                    case_ids_per_event=[["r1"]],
                    parsed_events=parsed_events,
                ),
                sigmas=sigmas,
                custom_fields=None,
                logsource_config=logsource_config,
                metrics=WindowMetrics(timings=WindowTimings()),
            )
        )

    assert op._tagged_config_warned is False
    _drain_one_window()
    assert op._tagged_config_warned is True

    with patch.object(sigma_broadcast_module.logger, "warning") as warning:
        _drain_one_window()
    warning.assert_not_called()


def test_tagged_event_carries_parse_failed_flag_when_parser_errors():
    """When schema-parser fails on an event, the matcher passes the original
    dict marked with `_detect_flow_parser_failed: True` through to
    tagged-events. Downstream consumers detect this via the boolean flag
    rather than the envelope changing shape."""
    op = _make_operator()
    event_buffer = [{"source": "raw", "broken": "yes"}]
    # parsed_events[0] is what parse_events_with_schema_parser returns on
    # failure: the original event copy + _detect_flow_parser_failed=True.
    parsed_events = [
        {"source": "raw", "broken": "yes", "_detect_flow_parser_failed": True},
    ]
    case_ids_per_event = [["r1"]]
    sigmas = [
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r1",
            title="Rule r1",
            level="high",
            sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
        )
    ]

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=event_buffer,
            match_result=_make_match_result(
                events=event_buffer,
                case_ids_per_event=case_ids_per_event,
                parsed_events=parsed_events,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )

    tagged_outputs = _tagged_payloads(out)
    assert len(tagged_outputs) == 1
    _, payload = tagged_outputs[0]
    result = orjson.loads(payload)
    assert result["parsed_event"] == parsed_events[0]
    assert result["parsed_event"]["_detect_flow_parser_failed"] is True


def _detection(
    rule_id: str,
    severity: str,
    updated: str | None = None,
    sigma_uuid: str | None = None,
) -> dict:
    return {
        "rule_id": rule_id,
        "rule_title": f"Rule {rule_id}",
        "severity": severity,
        "sigma_uuid": sigma_uuid or f"uuid-{rule_id}",
        "technique_ids": [],
        "updated": updated,
    }


def test_select_primary_detection_picks_highest_severity():
    older_critical = _detection("crit-old", "critical", "2020-01-01T00:00:00Z")
    newer_high = _detection("high-new", "high", "2026-06-01T00:00:00Z")
    picked = SigmaMatcherBroadcastFunction._select_primary_detection([newer_high, older_critical])
    assert picked is older_critical


def test_select_primary_detection_breaks_severity_tie_by_later_updated():
    older = _detection("crit-old", "critical", "2026-01-01T00:00:00Z")
    newer = _detection("crit-new", "critical", "2026-06-18T14:32:01.600Z")
    picked = SigmaMatcherBroadcastFunction._select_primary_detection([older, newer])
    assert picked is newer


def test_select_primary_detection_prefers_dated_rule_over_missing_updated():
    dated = _detection("dated", "high", "2026-01-01T00:00:00Z")
    undated = _detection("undated", "high", None)
    picked = SigmaMatcherBroadcastFunction._select_primary_detection([undated, dated])
    assert picked is dated


def test_select_primary_detection_returns_none_for_empty_list():
    assert SigmaMatcherBroadcastFunction._select_primary_detection([]) is None


def test_enrich_and_emit_sets_primary_match_from_highest_severity():
    op = _make_operator()
    events = [{"e": "multi"}]
    parsed_events = [
        {"event": {"code": "1"}, "host": {"name": "h-1"}, "@timestamp": "2026-05-26T00:00:00Z"},
    ]
    sigmas = [
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r-high",
            title="Suspicious PowerShell Encoded Command",
            level="high",
            techniques=[{"id": "T1059.001"}, {"id": "T1027"}],
            sigma_uuid="5f1e4f7a-1c3b-4e8d-9a2f-3c7b6d0e4f1a",
            updated="2026-06-18T00:00:00Z",
        ),
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r-crit",
            title="Suspicious Behavior",
            level="critical",
            techniques=[{"id": "T1059.002"}, {"id": "T1017"}],
            sigma_uuid="111114f7a-1c3b-4e8d-9a2f-3c7b61111111",
            updated="2026-01-01T00:00:00Z",
        ),
        Sigma(
            text=VALID_SIGMA_TEXT,
            case_id="r-crit-newer",
            title="Newer Critical Twin",
            level="critical",
            techniques=[{"id": "T1003"}],
            sigma_uuid="222224f7a-1c3b-4e8d-9a2f-3c7b62222222",
            updated="2026-06-18T14:32:01.600Z",
        ),
    ]

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=[["r-high", "r-crit", "r-crit-newer"]],
                parsed_events=parsed_events,
            ),
            sigmas=sigmas,
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )

    main_outputs = [orjson.loads(item) for item in _destination_payloads(out)]
    assert len(main_outputs) == 1
    detectflow = main_outputs[0]["detectflow"]

    assert detectflow["severity"] == "critical"
    assert detectflow["primary_match"] == {
        "sigma_rule_uuid": "222224f7a-1c3b-4e8d-9a2f-3c7b62222222",
        "_rule_id": "r-crit-newer",
        "rule_name": "Newer Critical Twin",
        "severity": "critical",
        "mitre_techniques": ["T1003"],
    }
    assert [m["_rule_id"] for m in detectflow["matches"]] == [
        "r-high",
        "r-crit",
        "r-crit-newer",
    ]
    assert "updated" not in detectflow["primary_match"]
    assert all("updated" not in m for m in detectflow["matches"])

    tagged_outputs = _tagged_payloads(out)
    assert len(tagged_outputs) == 1
    tagged = orjson.loads(tagged_outputs[0][1])
    assert tagged["detectflow"]["primary_match"] == detectflow["primary_match"]
    assert tagged["detectflow"]["severity"] == "critical"


# =============================================================================
# Multi-destination fan-out
# =============================================================================


def test_multi_destination_matched_only_vs_all_events():
    """Unmatched events go only to all_events destinations."""
    dest_matched = OutputDestination(topic="matched-only-topic", output_mode="matched_only")
    dest_all = OutputDestination(topic="all-events-topic", output_mode="all_events")
    op = _make_operator(destinations=[dest_matched, dest_all])

    events = [{"e": "matched"}, {"e": "unmatched"}]
    case_ids_per_event = [["r1"], []]
    metrics = WindowMetrics(timings=WindowTimings())

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=case_ids_per_event,
            ),
            sigmas=[],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    by_topic = _payloads_by_topic(out)
    assert len(by_topic["matched-only-topic"]) == 1  # matched only
    assert "detectflow" in by_topic["matched-only-topic"][0]
    assert len(by_topic["all-events-topic"]) == 2  # matched + unmatched
    assert metrics.output_tagged == 1
    assert metrics.output_untagged == 1  # counted once, not × destinations


def test_unmatched_only_destination_receives_all_events_minus_matches():
    """`unmatched_only` is exactly `all_events` minus the detections.

    Same window, three modes side by side: the matched event goes to matched_only and
    all_events; the unmatched one goes to all_events and unmatched_only.
    """
    op = _make_operator(
        destinations=[
            OutputDestination(topic="matched-topic", output_mode="matched_only"),
            OutputDestination(topic="all-topic", output_mode="all_events"),
            OutputDestination(topic="noise-topic", output_mode="unmatched_only"),
        ]
    )

    events = [{"e": "matched"}, {"e": "unmatched"}]
    metrics = WindowMetrics(timings=WindowTimings())

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(events=events, case_ids_per_event=[["r1"], []]),
            sigmas=[],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    by_topic = _payloads_by_topic(out)
    assert [e["e"] for e in by_topic["matched-topic"]] == ["matched"]
    assert [e["e"] for e in by_topic["all-topic"]] == ["matched", "unmatched"]
    assert [e["e"] for e in by_topic["noise-topic"]] == ["unmatched"]
    # Unmatched events carry no detectflow block on any destination
    assert "detectflow" not in by_topic["noise-topic"][0]
    # Counted once per event regardless of fan-out
    assert metrics.output_tagged == 1
    assert metrics.output_untagged == 1


def test_unmatched_only_alone_still_emits_when_no_rules_are_loaded():
    """The empty-rules window skip must not fire when a destination wants unmatched traffic.

    With zero rules nothing matches, so a matched_only-only pipeline can skip the window —
    but an unmatched_only destination must receive every buffered event.
    """
    op = _make_operator(
        destinations=[OutputDestination(topic="noise-topic", output_mode="unmatched_only")]
    )

    events = [{"e": "a"}, {"e": "b"}]
    metrics = WindowMetrics(timings=WindowTimings())

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(events=events, case_ids_per_event=[[], []]),
            sigmas=[],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    assert len(_destination_payloads(out)) == 2
    assert _tagged_payloads(out) == []  # no matches → nothing on the attack-chains tech topic
    assert metrics.output_tagged == 0
    assert metrics.output_untagged == 2


def test_matched_event_is_not_written_to_unmatched_only_destination():
    op = _make_operator(
        destinations=[OutputDestination(topic="noise-topic", output_mode="unmatched_only")]
    )

    events = [{"e": "matched"}]
    metrics = WindowMetrics(timings=WindowTimings())

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(events=events, case_ids_per_event=[["r1"]]),
            sigmas=[
                Sigma(
                    text=VALID_SIGMA_TEXT,
                    case_id="r1",
                    title="Rule r1",
                    level="high",
                    sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
                )
            ],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    assert _destination_payloads(out) == []
    # The attack-chains tech topic is independent of the destination modes — the match is
    # still reported there.
    assert len(_tagged_payloads(out)) == 1
    assert metrics.output_tagged == 0  # written nowhere
    assert metrics.matched_events == 1  # but still counted as a detection


def _sigma(case_id: str, level: str | None) -> Sigma:
    return Sigma(text=VALID_SIGMA_TEXT, case_id=case_id, title=f"Rule {case_id}", level=level)


def test_severity_filter_per_destination():
    """A detection reaches a filtered matched_only destination when ANY of its rules' levels
    is selected; all_events ignores the filter; tagged-events are never filtered."""
    op = _make_operator(
        destinations=[
            OutputDestination(topic="hot", severities={"high", "critical"}),
            OutputDestination(topic="info", severities={"informational"}),
            OutputDestination(topic="all", output_mode="all_events", severities={"critical"}),
            OutputDestination(topic="unfiltered"),
        ]
    )

    events = [{"e": "low"}, {"e": "low+high"}, {"e": "info-alias"}, {"e": "no-level"}, {"e": "u"}]
    metrics = WindowMetrics(timings=WindowTimings())
    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=[["r-low"], ["r-low", "r-high"], ["r-info"], ["r-none"], []],
            ),
            sigmas=[
                _sigma("r-low", "low"),
                _sigma("r-high", "High"),
                _sigma("r-info", "info"),
                _sigma("r-none", None),
            ],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    by_topic = _payloads_by_topic(out)
    assert [e["e"] for e in by_topic["hot"]] == ["low+high"]
    assert [e["e"] for e in by_topic["info"]] == ["info-alias"]
    # all_events ignores severities: every event, detections of any level included
    assert [e["e"] for e in by_topic["all"]] == [e["e"] for e in events]
    assert [e["e"] for e in by_topic["unfiltered"]] == [
        "low",
        "low+high",
        "info-alias",
        "no-level",
    ]
    # Full matches block is kept, not trimmed to the selected levels
    assert len(by_topic["hot"][0]["detectflow"]["matches"]) == 2
    assert len(_tagged_payloads(out)) == 4
    assert metrics.matched_events == 4
    assert metrics.output_tagged == 4
    assert metrics.output_untagged == 1


def test_severity_filter_drops_event_from_every_destination():
    op = _make_operator(destinations=[OutputDestination(topic="hot", severities={"critical"})])

    events = [{"e": "medium"}]
    metrics = WindowMetrics(timings=WindowTimings())
    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(events=events, case_ids_per_event=[["r1"]]),
            sigmas=[_sigma("r1", "medium")],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    assert _destination_payloads(out) == []
    assert len(_tagged_payloads(out)) == 1
    assert metrics.matched_events == 1
    assert metrics.output_tagged == 0


def test_multi_destination_parser_flag_selects_buffer():
    """Each destination independently chooses parsed vs original payload."""
    dest_raw = OutputDestination(
        topic="raw-out",
        output_mode="matched_only",
        apply_parser_to_output_events=False,
    )
    dest_parsed = OutputDestination(
        topic="parsed-out",
        output_mode="matched_only",
        apply_parser_to_output_events=True,
    )
    op = _make_operator(destinations=[dest_raw, dest_parsed])

    events = [{"source": "raw", "e": 1}]
    parsed_events = [{"source": "parsed", "event": {"code": "1"}}]
    case_ids_per_event = [["r1"]]
    metrics = WindowMetrics(timings=WindowTimings())

    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=case_ids_per_event,
                parsed_events=parsed_events,
            ),
            sigmas=[
                Sigma(
                    text=VALID_SIGMA_TEXT,
                    case_id="r1",
                    title="Rule r1",
                    level="high",
                    sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
                )
            ],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=metrics,
        )
    )

    by_topic = _payloads_by_topic(out)
    raw_payload = by_topic["raw-out"][0]
    parsed_payload = by_topic["parsed-out"][0]
    assert raw_payload["source"] == "raw"
    assert parsed_payload["source"] == "parsed"
    assert "detectflow" in raw_payload and "detectflow" in parsed_payload
    # Metrics not multiplied by fan-out
    assert metrics.output_tagged == 1
    assert metrics.output_untagged == 0


def test_multi_destination_same_parser_flag_reuses_serialized_payload():
    """Destinations that share apply_parser_to_output_events must share one JSON string.

    Memoization is the multi-destination emit hot path: without it, N identical destinations
    pay N× orjson + shallow-copy cost for byte-identical bodies.
    """
    destinations = [
        OutputDestination(
            topic=f"dest-{i}",
            output_mode="matched_only",
            apply_parser_to_output_events=True,
        )
        for i in range(3)
    ]
    op = _make_operator(destinations=destinations)

    events = [{"e": 1}]
    parsed_events = [{"source": "parsed", "event": {"code": "1"}}]
    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=[["r1"]],
                parsed_events=parsed_events,
            ),
            sigmas=[
                Sigma(
                    text=VALID_SIGMA_TEXT,
                    case_id="r1",
                    title="Rule r1",
                    level="high",
                    sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
                )
            ],
            custom_fields={"cf": "x"},
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )

    dest_emits = _destination_emits(out)
    assert [topic for topic, _ in dest_emits] == ["dest-0", "dest-1", "dest-2"]
    dest_payloads = [payload for _, payload in dest_emits]
    assert dest_payloads[0] is dest_payloads[1] is dest_payloads[2]
    body = orjson.loads(dest_payloads[0])
    assert body["source"] == "parsed"
    assert body["cf"] == "x"
    assert "detectflow" in body


def test_tagged_events_carry_pipeline_id_not_destination_topics():
    """Correlation contract: one envelope per match, identity is pipeline_id.

    Destination topics are SIEM output only. Isolation is the dedicated tagged-events
    topic plus ``pipeline_id`` (JOB_ID).
    """
    op = _make_operator(
        destinations=[
            OutputDestination(topic="first-dest", output_mode="matched_only"),
            OutputDestination(topic="second-dest", output_mode="all_events"),
            OutputDestination(topic="third-dest", output_mode="matched_only"),
        ]
    )

    events = [{"e": "matched"}]
    parsed_events = [
        {"event": {"code": "1"}, "host": {"name": "h-1"}, "@timestamp": "2026-05-26T00:00:00Z"}
    ]
    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=[["r1"]],
                parsed_events=parsed_events,
            ),
            sigmas=[
                Sigma(
                    text=VALID_SIGMA_TEXT,
                    case_id="r1",
                    title="Rule r1",
                    level="high",
                    sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
                )
            ],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )

    tagged = _tagged_payloads(out)
    # Still exactly ONE envelope for three destinations — the tech topic is not fanned out
    assert len(tagged) == 1
    envelope = orjson.loads(tagged[0][1])
    assert envelope["pipeline_id"] == "job-1"
    assert "source_topic" not in envelope
    assert "source_topics" not in envelope


def test_tagged_events_first_destination_is_siem_output_only():
    op = _make_operator(
        destinations=[
            OutputDestination(topic="first-dest", output_mode="matched_only"),
            OutputDestination(topic="second-dest", output_mode="matched_only"),
        ]
    )

    events = [{"e": "matched"}]
    parsed_events = [
        {"event": {"code": "1"}, "host": {"name": "h-1"}, "@timestamp": "2026-05-26T00:00:00Z"}
    ]
    out = list(
        op._enrich_and_emit_events(
            original_event_buffer=events,
            match_result=_make_match_result(
                events=events,
                case_ids_per_event=[["r1"]],
                parsed_events=parsed_events,
            ),
            sigmas=[
                Sigma(
                    text=VALID_SIGMA_TEXT,
                    case_id="r1",
                    title="Rule r1",
                    level="high",
                    sigma_uuid="2f958f09-99cf-4af8-8d26-7de834541f07",
                )
            ],
            custom_fields=None,
            logsource_config=_make_logsource_config(),
            metrics=WindowMetrics(timings=WindowTimings()),
        )
    )
    tagged = _tagged_payloads(out)
    assert "source_topic" not in orjson.loads(tagged[0][1])
    assert orjson.loads(tagged[0][1])["pipeline_id"] == "job-1"
