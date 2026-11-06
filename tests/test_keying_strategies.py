"""Tests for keying strategy helpers.

``compute_subtask_keys`` requires a live py4j gateway (Flink's KeyGroupRangeAssignment) and is
exercised by the integration tests, so the unit tests here focus on the gateway-free extractors.
"""

import json

from app.domain.keying.strategies import (
    calculate_hash_bucket_count,
    create_balanced_key_extractor,
    create_hash_key_extractor,
)


def _make_event(i: int) -> str:
    return json.dumps({"id": i, "msg": f"event-{i}"})


# --- balanced ---


def test_extractor_returns_only_provided_keys() -> None:
    """Every produced key must be one of the precomputed subtask keys."""
    subtask_keys = [7, 42, 100]
    extractor = create_balanced_key_extractor(subtask_keys)
    produced = {extractor(_make_event(i)) for i in range(1000)}
    assert produced.issubset(set(subtask_keys))


def test_extractor_is_deterministic() -> None:
    """The same event always maps to the same key."""
    extractor = create_balanced_key_extractor([1, 2, 3, 4])
    event = _make_event(123)
    assert extractor(event) == extractor(event)


def test_extractor_distributes_evenly() -> None:
    """Uniform hashing should spread events approximately evenly across buckets."""
    subtask_keys = [10, 20, 30, 40]
    extractor = create_balanced_key_extractor(subtask_keys)

    counts = dict.fromkeys(subtask_keys, 0)
    total = 40_000
    for i in range(total):
        counts[extractor(_make_event(i))] += 1

    expected = total / len(subtask_keys)
    # Allow generous slack; crc32 distribution is uniform enough for bucketing.
    for key, count in counts.items():
        assert abs(count - expected) < expected * 0.1, (key, count, expected)


def test_single_subtask_returns_constant_key() -> None:
    """parallelism=1 collapses to a single key for all events."""
    extractor = create_balanced_key_extractor([0])
    produced = {extractor(_make_event(i)) for i in range(100)}
    assert produced == {0}


# --- hash (legacy multi-bucket-per-slot) ---


def test_calculate_hash_bucket_count_small_parallelism() -> None:
    """Keep single-task deployments at one bucket."""
    assert calculate_hash_bucket_count(1, 3) == 1


def test_calculate_hash_bucket_count_larger_parallelism() -> None:
    """Use key_groups_per_task as the multiplier for parallel jobs."""
    assert calculate_hash_bucket_count(2, 3) == 6
    assert calculate_hash_bucket_count(3, 3) == 9
    assert calculate_hash_bucket_count(4, 2) == 8
    assert calculate_hash_bucket_count(8, 1) == 8


def test_hash_extractor_returns_hash_n_keys() -> None:
    extractor = create_hash_key_extractor(6)
    produced = {extractor(_make_event(i)) for i in range(500)}
    assert produced.issubset({f"hash-{i}" for i in range(6)})


def test_hash_extractor_single_bucket_is_constant() -> None:
    extractor = create_hash_key_extractor(1)
    produced = {extractor(_make_event(i)) for i in range(50)}
    assert produced == {"hash-0"}


def test_hash_extractor_is_deterministic() -> None:
    extractor = create_hash_key_extractor(9)
    event = _make_event(99)
    assert extractor(event) == extractor(event)
