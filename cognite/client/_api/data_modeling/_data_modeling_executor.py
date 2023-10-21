from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from cognite.client.utils._concurrency import ConcurrencySettings, MainThreadExecutor, TaskExecutor

_THREAD_POOL_EXECUTOR_SINGLETON: ThreadPoolExecutor
_MAIN_THREAD_EXECUTOR_SINGLETON = MainThreadExecutor()
_MAX_WORKERS = 1


def get_data_modeling_executor() -> TaskExecutor:
    """
    The data modeling backend has different concurrency limits in the backend compared to the rest of CDF.
    Thus, we use a dedicated executor for these endpoints to match the backend.

    Returns:
        TaskExecutor: The data modeling executor.
    """
    global _THREAD_POOL_EXECUTOR_SINGLETON

    if ConcurrencySettings.executor_type == "threadpool":
        try:
            executor: TaskExecutor = _THREAD_POOL_EXECUTOR_SINGLETON
        except NameError:
            # TPE has not been initialized
            executor = _THREAD_POOL_EXECUTOR_SINGLETON = ThreadPoolExecutor(_MAX_WORKERS)
    elif ConcurrencySettings.executor_type == "mainthread":
        executor = _MAIN_THREAD_EXECUTOR_SINGLETON
    else:
        raise RuntimeError(f"Invalid executor type '{ConcurrencySettings.executor_type}'")
    return executor
