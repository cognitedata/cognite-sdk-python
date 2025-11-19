import asyncio
import random

import pytest

from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._concurrency import (
    AsyncSDKTask,
    ConcurrencySettings,
    EventLoopThreadExecutor,
    execute_async_tasks,
)


async def i_dont_like_5(i: int) -> int:
    if i == 5:
        raise CogniteAPIError("no", 5, cluster="testcluster", project="testproject")
    else:
        # Yield control if we are not to fail - to avoid task i=5 to randomly be executed last
        await asyncio.sleep(0.01)
    return i


class TestExecutor:
    def test_get_event_loop_executor(self) -> None:
        executor = ConcurrencySettings._get_event_loop_executor()
        assert isinstance(executor, EventLoopThreadExecutor)

    @pytest.mark.asyncio
    async def test_async_tasks__results_ordering_match_tasks(self) -> None:
        async def async_task(i: int) -> int:
            await asyncio.sleep(random.random() / 50)
            return i

        # We use a task that yields control and use N tasks >> max_workers to really test ordering:
        tasks = [AsyncSDKTask(async_task, i) for i in range(50)]
        task_summary = await execute_async_tasks(tasks)
        task_summary.raise_compound_exception_if_failed_tasks()

        assert task_summary.results == list(range(50))

    @pytest.mark.asyncio
    async def test_async_tasks__results_ordering_match_tasks_even_with_failures(self) -> None:
        async def test_fn(i: int) -> int:
            await asyncio.sleep(random.random() / 50)
            if i in range(20, 23):
                raise ValueError
            return i

        tasks = [AsyncSDKTask(test_fn, i) for i in range(50)]
        task_summary = await execute_async_tasks(tasks)
        exp_res = [*range(20), *range(23, 50)]
        assert task_summary.results == exp_res
        assert len(task_summary.successful_tasks) == len(exp_res)
        assert len(task_summary.failed_tasks) == 3

        with pytest.raises(ValueError):
            task_summary.raise_compound_exception_if_failed_tasks()

    @pytest.mark.parametrize("fail_fast", (False, True))
    @pytest.mark.asyncio
    async def test_fail_fast__execute_async_tasks(self, fail_fast: bool) -> None:
        tasks = [AsyncSDKTask(i_dont_like_5, i) for i in range(10)]
        task_summary = await execute_async_tasks(tasks, fail_fast=fail_fast)

        with pytest.raises(CogniteAPIError) as err:
            task_summary.raise_compound_exception_if_failed_tasks()

        assert len(err.value.failed) == 1  # Task with i=5 failed
        if fail_fast:
            assert len(err.value.successful) == 5  # Tasks 0-4 completed
            assert len(err.value.skipped) == 4  # Tasks 6-9 were skipped
        else:
            assert len(err.value.successful) == 9  # All tasks except i=5 completed
            assert len(err.value.skipped) == 0  # No tasks were skipped
