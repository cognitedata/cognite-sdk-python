from concurrent.futures import ThreadPoolExecutor

from cognite.client.utils._concurrency import MainThreadExecutor, Settings, execute_tasks_concurrently, get_executor


class TestExecutor:
    def test_set_and_get_executor(self) -> None:
        executor = get_executor(1)
        assert isinstance(executor, ThreadPoolExecutor)

        Settings.executor_type = "mainthread"
        executor = get_executor(1)
        assert isinstance(executor, MainThreadExecutor)

        Settings.executor_type = "threadpool"
        executor = get_executor(1)
        assert isinstance(executor, ThreadPoolExecutor)

    def test_main_thread_executor(self) -> None:
        def foo(i: int) -> int:
            return i

        Settings.executor_type = "mainthread"
        task_summary = execute_tasks_concurrently(foo, [(i,) for i in range(10)], 10)

        assert task_summary.results == [i for i in range(10)]
