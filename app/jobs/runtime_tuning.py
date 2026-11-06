"""Companion Flink knobs that must follow PYTHON_EXECUTION_MODE / STATE_BACKEND.

TaskManager memory pools and checkpoint behaviour are tightly coupled to those two
switches. Callers that change either setting should apply these companions so local
runs, ConfigMap overrides, and K8s templates stay aligned.

Control-plane mirror (Jinja source of truth for FlinkDeployment):
``../detectflow-backend/apps/providers/matcher_runtime_tuning.py``
"""

from __future__ import annotations

from typing import Any, Literal

from app.config.logging import get_logger

logger = get_logger(__name__)

# Process mode: Python workers are separate OS processes; Flink's default overhead floor
# is enough. Thread/Pemja embeds CPython+Polars in the JVM process and needs more.
JVM_OVERHEAD_MIN_PROCESS = "192m"
JVM_OVERHEAD_MIN_THREAD = "512m"

# RocksDB claims managed memory for block cache / write buffers. Hashmap keeps ListState
# on-heap — reserving managed memory only shrinks the heap that holds the event buffer.
MANAGED_FRACTION_ROCKSDB = "0.2"
MANAGED_FRACTION_HASHMAP = "0.0"

# With hashmap, shrink network buffers so more of Flink memory stays on the heap.
NETWORK_FRACTION_HASHMAP = "0.05"
NETWORK_MAX_HASHMAP = "64m"


def companion_config_for(
    *,
    state_backend: Literal["memory", "rocksdb"],
    python_execution_mode: Literal["process", "thread"],
) -> dict[str, str]:
    """Return Flink configuration keys that must track the two runtime switches."""
    conf: dict[str, str] = {
        "python.fn-execution.memory.managed": "false",
    }

    if state_backend == "rocksdb":
        conf["execution.checkpointing.incremental"] = "true"
        conf["taskmanager.memory.managed.fraction"] = MANAGED_FRACTION_ROCKSDB
    else:
        conf["execution.checkpointing.incremental"] = "false"
        conf["taskmanager.memory.managed.fraction"] = MANAGED_FRACTION_HASHMAP
        conf["taskmanager.memory.network.fraction"] = NETWORK_FRACTION_HASHMAP
        conf["taskmanager.memory.network.max"] = NETWORK_MAX_HASHMAP

    if python_execution_mode == "thread":
        conf["taskmanager.memory.jvm-overhead.min"] = JVM_OVERHEAD_MIN_THREAD
    else:
        conf["taskmanager.memory.jvm-overhead.min"] = JVM_OVERHEAD_MIN_PROCESS

    return conf


def apply_companion_config(config: Any, settings) -> dict[str, str]:
    """Write companion knobs onto a Flink ``Configuration`` (Java or PyFlink).

    ``config`` must expose ``setString(key, value)`` (Java Configuration via py4j, or a
    compatible test double).
    """
    companions = companion_config_for(
        state_backend=settings.state_backend,
        python_execution_mode=settings.python_execution_mode,
    )
    for key, value in companions.items():
        config.setString(key, value)

    logger.info(
        "runtime companion config applied",
        state_backend=settings.state_backend,
        python_execution_mode=settings.python_execution_mode,
        **{k.replace(".", "_"): v for k, v in companions.items()},
    )
    return companions
