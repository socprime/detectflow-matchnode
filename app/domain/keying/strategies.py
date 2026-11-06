import zlib
from collections.abc import Callable

from app.config.logging import get_logger

logger = get_logger(__name__)


class SubtaskKeyResolutionError(RuntimeError):
    """Raised when we cannot find a distinct routing key for every subtask."""


# ---------------------------------------------------------------------------
# balanced — one evenly-filled bucket per subtask (default)
# ---------------------------------------------------------------------------


def compute_subtask_keys(
    gateway, parallelism: int, max_parallelism: int, scan_limit: int | None = None
) -> list[int]:
    """Return one integer key per subtask, routed there by Flink's own key-group logic.

    Flink assigns a key to a subtask via
    ``KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, parallelism)``.
    This returns a list ``subtask_keys`` where ``subtask_keys[s]`` is guaranteed to be routed to
    subtask ``s``. ``max_parallelism`` (the number of key groups) is fixed for the lifetime of the
    job's state so parallelism can be changed / rescaled within ``[1, max_parallelism]`` while each
    subtask keeps exactly one bucket. Even event distribution does NOT depend on
    ``max_parallelism == parallelism``; it comes from the uniform ``crc32 % parallelism`` bucketing
    in :func:`create_balanced_key_extractor`.

    The routing key at runtime is the ``Row(Long)`` produced by PyFlink's ``KeyByKeySelector``
    when ``key_by`` is called with ``key_type=Types.LONG()``. We reproduce that exact object here
    (``Row(1)`` with field 0 set to a ``java.lang.Long``) and call Flink's real routing method
    through the py4j gateway, so the mapping is correct regardless of hash internals or Flink
    version.

    Ref: org.apache.flink.runtime.state.KeyGroupRangeAssignment (flink-dist)
    Ref: org.apache.flink.streaming.api.functions.python.KeyByKeySelector (flink-python)

    Args:
        gateway: py4j gateway (``pyflink.java_gateway.get_gateway()``).
        parallelism: The operator parallelism (number of buckets / subtasks).
        max_parallelism: Flink maxParallelism (number of key groups); must be >= parallelism.
        scan_limit: Max integers to probe before giving up. Defaults to a generous
            coupon-collector bound so even skewed hashing terminates.

    Returns:
        List of length ``parallelism`` mapping subtask index -> routing key.

    Raises:
        SubtaskKeyResolutionError: If a key could not be found for every subtask.
    """
    if parallelism < 1:
        raise ValueError(f"parallelism must be >= 1, got {parallelism}")
    if max_parallelism < parallelism:
        raise ValueError(
            f"max_parallelism ({max_parallelism}) must be >= parallelism ({parallelism})"
        )

    if parallelism == 1:
        return [0]

    jvm = gateway.jvm
    key_group_assignment = jvm.org.apache.flink.runtime.state.KeyGroupRangeAssignment
    j_row = jvm.org.apache.flink.types.Row
    j_long = jvm.java.lang.Long

    if scan_limit is None:
        # Coupon-collector expectation is ~P*ln(P); use a large multiple as a safe cap.
        scan_limit = max(10_000, parallelism * parallelism * 100)

    subtask_keys: list[int | None] = [None] * parallelism
    remaining = parallelism

    for candidate in range(scan_limit):
        # Reproduce the exact runtime routing key: Row(1) with a single Long field.
        row = j_row(1)
        row.setField(0, j_long(candidate))
        subtask = key_group_assignment.assignKeyToParallelOperator(
            row, max_parallelism, parallelism
        )
        if subtask_keys[subtask] is None:
            subtask_keys[subtask] = candidate
            remaining -= 1
            if remaining == 0:
                break

    if remaining != 0:
        missing = [s for s, k in enumerate(subtask_keys) if k is None]
        raise SubtaskKeyResolutionError(
            f"could not resolve routing keys for subtasks {missing} within "
            f"scan_limit={scan_limit} (parallelism={parallelism}, max_parallelism={max_parallelism})"
        )

    return [int(k) for k in subtask_keys]  # type: ignore[arg-type]


def create_balanced_key_extractor(subtask_keys: list[int]) -> Callable[[str], int]:
    """Return a key function mapping event JSON -> a routing key, one bucket per subtask.

    Events are hashed uniformly into ``len(subtask_keys)`` buckets and each bucket returns the
    precomputed key for a distinct subtask. This yields approximately even distribution with
    exactly one keyed window (and one ``on_timer()``) per subtask, avoiding the oversharding
    overhead of the multi-bucket-per-slot (``hash``) strategy.

    Bucketing uses crc32 rather than md5: the extractor runs on every event, and crc32 is ~10x
    cheaper on typical 2-3 KiB payloads with equivalent distribution quality for this use.

    The returned closure captures only a plain list of ints so it stays picklable for shipping
    to the Python workers.
    """
    num_buckets = len(subtask_keys)
    keys = list(subtask_keys)

    if num_buckets == 1:
        # Single-task deployments pay no hashing at all.
        sole = keys[0]
        return lambda _event_json: sole

    def key_extractor(event_json: str) -> int:
        try:
            bucket = zlib.crc32(event_json.encode()) % num_buckets
            return keys[bucket]
        except Exception as e:
            logger.error("key extraction error", error=str(e), key_type="balanced")
            return keys[0]

    return key_extractor


# ---------------------------------------------------------------------------
# hash — legacy oversharding (parallelism × KEY_GROUPS_PER_TASK string buckets)
# ---------------------------------------------------------------------------


def calculate_hash_bucket_count(target_parallelism: int, key_groups_per_task: int) -> int:
    """Calculate hash bucket count for a given target parallelism.

    Heuristic:
    - parallelism=1 -> 1 bucket
    - parallelism>1 -> parallelism * key_groups_per_task buckets

    This keeps single-task deployments cheap while allowing configurable fan-out
    for larger jobs. Used only when ``KEYING_STRATEGY=hash``.
    """
    if target_parallelism == 1:
        return 1
    return target_parallelism * key_groups_per_task


def key_by_hash(event_json: str, num_buckets: int) -> str:
    """Key events by hash with a fixed number of ``hash-{n}`` string buckets."""
    try:
        bucket = zlib.crc32(event_json.encode()) % num_buckets
        return f"hash-{bucket}"
    except Exception as e:
        logger.error("key extraction error", error=str(e), key_type="hash")
        return "hash-0"


def create_hash_key_extractor(num_buckets: int) -> Callable[[str], str]:
    """Return a key function: event JSON string -> stable bucket key string.

    The key extractor runs as a Python operator on EVERY event, so it must be
    cheap: crc32, and a constant key when there is only one bucket.
    """
    if num_buckets == 1:
        return lambda _event_json: "hash-0"
    return lambda event_json: key_by_hash(event_json, num_buckets)
