"""
Event keying strategies for horizontal scaling

This module provides keying functions to distribute events across
parallel Flink tasks for true horizontal scalability.
"""

import hashlib
from collections.abc import Callable

import orjson

from app.config.logging import get_logger

logger = get_logger(__name__)


def create_key_extractor(
    strategy: str = "hash",
    num_buckets: int | None = None,
) -> Callable[[str], str]:
    """
    Create a key extraction function based on strategy

    Args:
        strategy: Keying strategy to use:
            - "hash": Hash-based distribution (recommended, default)
            - "computer": Key by Computer/hostname field (legacy)
            - "round_robin": Simple round-robin distribution (legacy)
        num_buckets: Number of key buckets for hash/round_robin strategies.
            If None, uses raw hash (infinite keys - high overhead!).
            For production, should be: parallelism × key_groups_per_task
            Example: parallelism=10, key_groups=3 → num_buckets=30

    Returns:
        Function that takes event JSON string and returns key string
    """
    if strategy == "hash":
        if num_buckets:
            # Return bucketed hash for controlled number of keys
            return lambda event_json: key_by_hash(event_json, num_buckets)
        else:
            # Return raw hash for infinite keys (high overhead, not recommended)
            logger.warning(
                "hash keying without num_buckets",
                warning="Using raw hash creates unique key per event - high overhead!",
                recommendation="Set num_buckets = parallelism × key_groups_per_task",
            )
            return key_by_hash_raw
    elif strategy == "computer":
        return key_by_computer
    elif strategy == "round_robin":
        if num_buckets:
            return lambda event_json: key_by_round_robin(event_json, num_buckets)
        else:
            # Default to 100 buckets for round-robin if not specified
            return lambda event_json: key_by_round_robin(event_json, 100)
    else:
        logger.warning("unknown keying strategy", strategy=strategy, fallback="hash")
        if num_buckets:
            return lambda event_json: key_by_hash(event_json, num_buckets)
        else:
            return key_by_hash_raw


def key_by_computer(event_json: str) -> str:
    """
    Key events by Computer/hostname field

    This provides good locality - all events from same host are processed
    together, which can help with correlation and reduces shuffle overhead.

    Fallback: If Computer field is missing, uses hash-based distribution

    Args:
        event_json: JSON string of the event

    Returns:
        Computer name or hashed key
    """
    try:
        event = orjson.loads(event_json)

        # Try common field names for hostname
        computer = (
            event.get("Computer")
            or event.get("host")
            or event.get("hostname")
            or event.get("winlog", {}).get("computer_name")
            or event.get("host", {}).get("name")
        )

        if computer:
            # Normalize to lowercase for consistent keying
            return str(computer).lower()

        # Fallback to hash if no computer field
        return key_by_hash(event_json)

    except Exception as e:
        logger.error("key extraction error", error=str(e), key_type="computer")
        return "hash-0"


def key_by_hash(event_json: str, num_buckets: int) -> str:
    """
    Key events by hash with fixed number of buckets

    Provides even distribution with controlled number of unique keys.
    Each bucket will trigger separate on_timer() call in windowing.

    IMPORTANT: num_buckets should be = parallelism × key_groups_per_task
    This balances between:
    - Too few buckets: uneven load when scaling
    - Too many buckets: excessive on_timer() overhead

    Args:
        event_json: JSON string of the event
        num_buckets: Number of hash buckets (keys)

    Returns:
        Bucket key as string (e.g., "hash-42")
    """
    try:
        # Hash entire event JSON without parsing
        hash_value = hashlib.md5(event_json.encode()).hexdigest()
        bucket = int(hash_value, 16) % num_buckets
        return f"hash-{bucket}"
    except Exception as e:
        logger.error("key extraction error", error=str(e), key_type="hash")
        return "hash-0"


def key_by_hash_raw(event_json: str) -> str:
    """
    Key events by raw hash (infinite unique keys)

    WARNING: Creates unique key for each unique event!
    This causes high overhead:
    - Each unique event = separate on_timer() call
    - Each on_timer() loads and parses all Sigma rules
    - 10K events = 10K on_timer() calls = MASSIVE overhead

    Only use for testing or when you explicitly want per-event processing.
    For production, use key_by_hash() with num_buckets instead.

    Args:
        event_json: JSON string of the event

    Returns:
        Raw hash string (32 hex characters)
    """
    try:
        # Hash entire event JSON without parsing
        hash_value = hashlib.md5(event_json.encode()).hexdigest()
        return hash_value
    except Exception as e:
        logger.error("key extraction error", error=str(e), key_type="hash_raw")
        return "0"


def key_by_round_robin(event_json: str, num_buckets: int = 100) -> str:
    """
    Simple round-robin distribution using event hash

    Args:
        event_json: JSON string of the event
        num_buckets: Number of buckets to distribute across

    Returns:
        Bucket number as string (0 to num_buckets-1)
    """
    try:
        # Use hash of entire event for distribution
        hash_value = hashlib.md5(event_json.encode()).hexdigest()
        bucket = int(hash_value, 16) % num_buckets
        return f"bucket-{bucket}"
    except Exception as e:
        logger.error("key extraction error", error=str(e), key_type="round_robin")
        return "bucket-0"
