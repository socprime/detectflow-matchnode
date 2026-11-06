"""Compatibility patches for PyFlink 2.2.0 thread (embedded/Pemja) mode.

PyFlink's embedded ``StreamingRuntimeContext`` still calls Java
``RuntimeContext`` methods that Flink 2.0 removed (they moved to
``RuntimeContext.getTaskInfo()``): ``getIndexOfThisSubtask()``,
``getNumberOfParallelSubtasks()``, ``getTaskNameWithSubtasks()`` etc.
In thread mode those resolve against the real Java object via Pemja and fail
with ``AttributeError: 'pemja.PyJObject' object has no attribute ...``.
Process mode is unaffected (its runtime context is built from serialized
task info, not the live Java object).

This module patches the embedded wrapper to fall back to ``getTaskInfo()``
when the direct (removed) method is unavailable. Importing it is a no-op for
process mode and for environments without the embedded module.

The KEYED_CO_BROADCAST ``on_timer`` side-output bug (ClassCastException
Object[] -> String) is fixed at image build time in the Dockerfile (``sed``
on installed ``operations.py``), not here.

Upstream: ``pyflink/fn_execution/datastream/embedded/runtime_context.py``
(apache-flink 2.2.0); revisit when upgrading PyFlink.
"""

from __future__ import annotations

from app.config.logging import get_logger

logger = get_logger(__name__)

_PATCHED = False


def _make_fallback(direct_name: str, task_info_name: str):
    def method(self):
        ctx = self._runtime_context
        try:
            return getattr(ctx, direct_name)()
        except AttributeError:
            return getattr(ctx.getTaskInfo(), task_info_name)()

    method.__name__ = direct_name
    return method


def patch_embedded_runtime_context() -> None:
    """Idempotently patch the embedded StreamingRuntimeContext (thread mode)."""
    global _PATCHED
    if _PATCHED:
        return
    _PATCHED = True

    try:
        from pyflink.fn_execution.datastream.embedded.runtime_context import (
            StreamingRuntimeContext,
        )
    except ImportError:  # embedded runtime not available — nothing to patch
        return

    fallbacks = {
        "get_index_of_this_subtask": ("getIndexOfThisSubtask", "getIndexOfThisSubtask"),
        "get_number_of_parallel_subtasks": (
            "getNumberOfParallelSubtasks",
            "getNumberOfParallelSubtasks",
        ),
        "get_max_number_of_parallel_subtasks": (
            "getMaxNumberOfParallelSubtasks",
            "getMaxNumberOfParallelSubtasks",
        ),
        "get_attempt_number": ("getAttemptNumber", "getAttemptNumber"),
        "get_task_name": ("getTaskName", "getTaskName"),
        "get_task_name_with_subtasks": ("getTaskNameWithSubtasks", "getTaskNameWithSubtasks"),
    }
    for py_name, (direct, via_task_info) in fallbacks.items():
        setattr(StreamingRuntimeContext, py_name, _make_fallback(direct, via_task_info))

    logger.info("patched embedded StreamingRuntimeContext for Flink 2.x TaskInfo API")


def apply_thread_mode_patches() -> None:
    """Apply PyFlink 2.2.0 embedded (thread) mode compatibility patches."""
    patch_embedded_runtime_context()
