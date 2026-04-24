import hashlib
from collections.abc import Callable

from app.config.logging import get_logger

logger = get_logger(__name__)


def calculate_hash_bucket_count(target_parallelism: int, key_groups_per_task: int) -> int:
    """
    Calculate hash bucket count for a given target parallelism.

    Heuristic:
    - parallelism=1 -> 1 bucket
    - parallelism>1 -> parallelism * key_groups_per_task buckets

    This keeps single-task deployments cheap while allowing configurable fan-out
    for larger jobs.
    """
    if target_parallelism == 1:
        return 1
    return target_parallelism * key_groups_per_task


def create_hash_key_extractor(num_buckets: int) -> Callable[[str], str]:
    """Return a key function: event JSON string -> stable bucket key string."""
    return lambda event_json: key_by_hash(event_json, num_buckets)


def key_by_hash(event_json: str, num_buckets: int) -> str:
    """
    Key events by hash with a fixed number of buckets.

    num_buckets should be derived from the target parallelism used for keying.
    """
    try:
        hash_value = hashlib.md5(event_json.encode()).hexdigest()
        bucket = int(hash_value, 16) % num_buckets
        return f"hash-{bucket}"
    except Exception as e:
        logger.error("key extraction error", error=str(e), key_type="hash")
        return "hash-0"
