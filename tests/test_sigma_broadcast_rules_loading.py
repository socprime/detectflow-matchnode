import orjson

from app.operators.sigma_broadcast import (
    SigmaMatcherBroadcastFunction,
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


def _make_operator() -> SigmaMatcherBroadcastFunction:
    op = SigmaMatcherBroadcastFunction(
        window_size_seconds=60,
        window_count_threshold=100,
        job_id="job-1",
        output_mode="all_events",
    )
    # In runtime this is registered in open(); for unit tests we emulate it.
    op._active_rules_count = [0]
    return op


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
    op = SigmaMatcherBroadcastFunction(
        window_size_seconds=60,
        window_count_threshold=100,
        job_id="job-1",
        output_mode="all_events",
        keep_filtered_events=False,
    )
    op._active_rules_count = [0]

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
            event_buffer=events,
            case_ids_per_event=case_ids_per_event,
            sigmas=sigmas,
            custom_fields=None,
            metrics=metrics,
            prefiltered_mask=prefiltered_mask,
        )
    )

    assert len(out) == 2  # matched + unmatched, filtered dropped
    assert metrics.prefiltered_events == 1
    assert op._prefiltered_events_count[0] == 1
    # First emitted is matched, second is untagged
    assert "sigma_rule_ids" in orjson.loads(out[0])
    assert orjson.loads(out[0])["sigma_rule_ids"] == ["r1"]
    assert orjson.loads(out[1])["sigma_rule_ids"] == []


def test_enrich_and_emit_keeps_prefiltered_when_keep_filtered_events_true():
    """When keep_filtered_events=True, prefiltered events are yielded as untagged."""
    op = SigmaMatcherBroadcastFunction(
        window_size_seconds=60,
        window_count_threshold=100,
        job_id="job-1",
        output_mode="all_events",
        keep_filtered_events=True,
    )
    op._active_rules_count = [0]

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
            event_buffer=events,
            case_ids_per_event=case_ids_per_event,
            sigmas=sigmas,
            custom_fields=None,
            metrics=metrics,
            prefiltered_mask=prefiltered_mask,
        )
    )

    assert len(out) == 3
    assert metrics.prefiltered_events == 1
    assert op._prefiltered_events_count[0] == 1
    assert orjson.loads(out[0])["sigma_rule_ids"] == ["r1"]
    assert orjson.loads(out[1])["sigma_rule_ids"] == []  # prefiltered but kept
    assert orjson.loads(out[2])["sigma_rule_ids"] == []


def test_enrich_and_emit_prefiltered_events_metric_with_no_prefiltered_mask():
    """prefiltered_events stays 0 when prefiltered_mask is None."""
    op = _make_operator()
    events = [{"e": "a"}, {"e": "b"}]
    case_ids_per_event = [[], []]
    metrics = WindowMetrics(timings=WindowTimings())

    list(
        op._enrich_and_emit_events(
            event_buffer=events,
            case_ids_per_event=case_ids_per_event,
            sigmas=[],
            custom_fields=None,
            metrics=metrics,
            prefiltered_mask=None,
        )
    )

    assert metrics.prefiltered_events == 0
    assert op._prefiltered_events_count[0] == 0
