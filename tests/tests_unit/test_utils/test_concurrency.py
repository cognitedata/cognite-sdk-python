import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import no_op
from cognite.client.utils._concurrency import (
    ConcurrencySettings,
    MainThreadExecutor,
    execute_tasks,
    execute_tasks_serially,
)


def i_dont_like_5(i):
    if i == 5:
        raise CogniteAPIError("no", 5)
    else:
        # Yield thread control if we are not to fail - to avoid task i=5 to randomly be executed last
        time.sleep(0.01)
    return i


class TestExecutor:
    def test_set_and_get_executor(self) -> None:
        executor = ConcurrencySettings.get_executor(1)
        assert isinstance(executor, ThreadPoolExecutor)

        ConcurrencySettings.executor_type = "mainthread"
        executor = ConcurrencySettings.get_executor(1)
        assert isinstance(executor, MainThreadExecutor)

        ConcurrencySettings.executor_type = "threadpool"
        executor = ConcurrencySettings.get_executor(1)
        assert isinstance(executor, ThreadPoolExecutor)

    @pytest.mark.parametrize(
        "executor",
        (ConcurrencySettings.get_mainthread_executor(), ConcurrencySettings.get_thread_pool_executor(2)),
    )
    def test_executors(self, executor) -> None:
        assert ConcurrencySettings.executor_type == "threadpool"

        task_summary = execute_tasks(no_op, [(i,) for i in range(10)], 10, executor=executor)
        task_summary.raise_compound_exception_if_failed_tasks()

        assert sorted(task_summary.results) == list(range(10))

    @pytest.mark.parametrize("fail_fast", (False, True))
    def test_fail_fast__execute_tasks_serially(self, fail_fast):
        tasks = list(zip(range(10)))
        task_summary = execute_tasks_serially(i_dont_like_5, tasks, fail_fast=fail_fast)
        with pytest.raises(CogniteAPIError) as err:
            task_summary.raise_compound_exception_if_failed_tasks()

        assert err.value.failed == [(5,)]
        if fail_fast:
            assert err.value.successful == tasks[:5]
            assert err.value.skipped == tasks[6:]
        else:
            assert err.value.successful == tasks[:5] + tasks[6:]
            assert err.value.skipped == []

    @pytest.mark.parametrize("fail_fast", (False, True))
    def test_fail_fast__execute_tasks_with_threads(self, fail_fast):
        assert ConcurrencySettings.executor_type == "threadpool"
        task_summary = execute_tasks(
            i_dont_like_5,
            list(zip(range(10))),
            max_workers=3,
            fail_fast=fail_fast,
        )
        with pytest.raises(CogniteAPIError) as err:
            task_summary.raise_compound_exception_if_failed_tasks()

        assert err.value.failed == [(5,)]
        if fail_fast:
            assert err.value.skipped
            assert 9 == len(err.value.successful) + len(err.value.skipped)
        else:
            assert not err.value.skipped
            assert 9 == len(err.value.successful)
