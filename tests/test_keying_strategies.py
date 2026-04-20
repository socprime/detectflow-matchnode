"""Tests for keying strategy helpers."""

from app.domain.keying.strategies import calculate_hash_bucket_count


def test_calculate_hash_bucket_count_small_parallelism() -> None:
    """Keep single-task deployments at one bucket."""
    assert calculate_hash_bucket_count(1, 3) == 1


def test_calculate_hash_bucket_count_larger_parallelism() -> None:
    """Use key_groups_per_task as the multiplier for parallel jobs."""
    assert calculate_hash_bucket_count(2, 3) == 6
    assert calculate_hash_bucket_count(3, 3) == 9
    assert calculate_hash_bucket_count(4, 2) == 8
    assert calculate_hash_bucket_count(8, 1) == 8
