"""Tests for companion Flink knobs tied to STATE_BACKEND / PYTHON_EXECUTION_MODE."""

from app.jobs.runtime_tuning import (
    JVM_OVERHEAD_MIN_PROCESS,
    JVM_OVERHEAD_MIN_THREAD,
    MANAGED_FRACTION_HASHMAP,
    MANAGED_FRACTION_ROCKSDB,
    apply_companion_config,
    companion_config_for,
)


def test_rocksdb_process_companions() -> None:
    conf = companion_config_for(state_backend="rocksdb", python_execution_mode="process")
    assert conf["execution.checkpointing.incremental"] == "true"
    assert conf["taskmanager.memory.managed.fraction"] == MANAGED_FRACTION_ROCKSDB
    assert conf["taskmanager.memory.jvm-overhead.min"] == JVM_OVERHEAD_MIN_PROCESS
    assert "taskmanager.memory.network.fraction" not in conf


def test_hashmap_thread_companions() -> None:
    conf = companion_config_for(state_backend="memory", python_execution_mode="thread")
    assert conf["execution.checkpointing.incremental"] == "false"
    assert conf["taskmanager.memory.managed.fraction"] == MANAGED_FRACTION_HASHMAP
    assert conf["taskmanager.memory.network.fraction"] == "0.05"
    assert conf["taskmanager.memory.network.max"] == "64m"
    assert conf["taskmanager.memory.jvm-overhead.min"] == JVM_OVERHEAD_MIN_THREAD


def test_apply_companion_config_writes_all_keys() -> None:
    class _FakeSettings:
        state_backend = "memory"
        python_execution_mode = "process"

    written: dict[str, str] = {}

    class _FakeConfig:
        def setString(self, key: str, value: str) -> None:  # noqa: N802 — mirrors Java Configuration
            written[key] = value

    apply_companion_config(_FakeConfig(), _FakeSettings())
    assert written == companion_config_for(state_backend="memory", python_execution_mode="process")
