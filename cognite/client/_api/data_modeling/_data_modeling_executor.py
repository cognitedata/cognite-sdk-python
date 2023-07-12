from concurrent.futures import ThreadPoolExecutor

from cognite.client.utils._concurrency import ConcurrencySettings, MainThreadExecutor, TaskExecutor

_THREAD_POOL_EXECUTOR_SINGLETON: ThreadPoolExecutor
_MAIN_THREAD_EXECUTOR_SINGLETON = MainThreadExecutor()


def get_data_modeling_executor() -> TaskExecutor:
    global _THREAD_POOL_EXECUTOR_SINGLETON

    if ConcurrencySettings.executor_type == "threadpool":
        try:
            executor: TaskExecutor = _THREAD_POOL_EXECUTOR_SINGLETON
        except NameError:
            # TPE has not been initialized
            executor = _THREAD_POOL_EXECUTOR_SINGLETON = ThreadPoolExecutor(2)
    elif ConcurrencySettings.executor_type == "mainthread":
        executor = _MAIN_THREAD_EXECUTOR_SINGLETON
    else:
        raise RuntimeError(f"Invalid executor type '{ConcurrencySettings.executor_type}'")
    return executor
