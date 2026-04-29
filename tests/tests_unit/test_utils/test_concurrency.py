from __future__ import annotations

import asyncio
import random
from collections.abc import Iterator
from typing import ClassVar

import pytest

from cognite.client import global_config
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._concurrency import (
    AsyncSDKTask,
    ConcurrencyConfig,
    ConcurrencySettings,
    EventLoopThreadExecutor,
    _get_event_loop_executor,
    execute_async_tasks,
)
from tests.utils import fresh_concurrency_state


@pytest.fixture
def fresh_unfrozen_global_concurrency() -> Iterator[None]:
    with fresh_concurrency_state():
        yield


class TestConcurrencySettingsConfig:
    def test_setters_work_before_freeze(self) -> None:
        cs = ConcurrencySettings()
        cs.general.read = 10
        cs.general.delete = 3
        assert cs.general.read == 10
        assert cs.general.delete == 3

    def test_data_modeling_extra_setters_work_before_freeze(self) -> None:
        cs = ConcurrencySettings()
        cs.data_modeling.search = 5
        cs.data_modeling.read_schema = 6
        cs.data_modeling.write_schema = 3
        assert cs.data_modeling.search == 5
        assert cs.data_modeling.read_schema == 6
        assert cs.data_modeling.write_schema == 3

    def test_is_frozen_starts_false(self) -> None:
        cs = ConcurrencySettings()
        assert cs.is_frozen is False

    def test_freeze_sets_is_frozen(self) -> None:
        cs = ConcurrencySettings()
        cs._freeze()
        assert cs.is_frozen is True

    @pytest.mark.parametrize(
        "sub_config_name, attr",
        [
            ("general", "delete"),
            ("raw", "delete"),
            ("datapoints", "write"),
            ("data_modeling", "search"),
        ],
    )
    def test_setter_raises_after_freeze(self, sub_config_name: str, attr: str) -> None:
        cs = ConcurrencySettings()
        cs._freeze()
        sub = getattr(cs, sub_config_name)
        with pytest.raises(RuntimeError, match="Cannot modify"):
            setattr(sub, attr, 99)

    def test_repr_unfrozen(self) -> None:
        cs = ConcurrencySettings()
        r = repr(cs)
        assert "ConcurrencySettings(" in r
        assert "general=" in r
        assert "frozen" not in r

    def test_repr_frozen(self) -> None:
        cs = ConcurrencySettings()
        cs._freeze()
        r = repr(cs)
        assert "(frozen)" in r

    def test_error_message_names_api_and_attribute(self) -> None:
        cs = ConcurrencySettings()
        cs._freeze()
        with pytest.raises(RuntimeError, match=r"general\.read"):
            cs.general.read = 1
        with pytest.raises(RuntimeError, match=r"data_modeling\.search"):
            cs.data_modeling.search = 1

    def test_all_sub_configs_inherit_from_concurrency_config(self) -> None:
        cs = ConcurrencySettings()
        assert cs._all_concurrency_configs, "expected at least one sub-config"
        for sub in cs._all_concurrency_configs:
            assert isinstance(sub, ConcurrencyConfig)


@pytest.mark.usefixtures("fresh_unfrozen_global_concurrency")
class TestSemaphoreFactory:
    cs: ClassVar[ConcurrencySettings] = global_config.concurrency_settings

    async def test_returns_bounded_semaphore_with_correct_value(self) -> None:
        sem = self.cs.general._semaphore_factory("read", "proj-a")
        assert isinstance(sem, asyncio.BoundedSemaphore)
        assert sem._value == self.cs.general.read

    async def test_cache_hit_returns_same_object(self) -> None:
        sem1 = self.cs.general._semaphore_factory("read", "proj-a")
        sem2 = self.cs.general._semaphore_factory("read", "proj-a")
        assert sem1 is sem2

    async def test_different_project_returns_different_semaphore(self) -> None:
        sem_a = self.cs.general._semaphore_factory("read", "proj-a")
        sem_b = self.cs.general._semaphore_factory("read", "proj-b")
        assert sem_a is not sem_b

    async def test_different_operation_returns_different_semaphore(self) -> None:
        sem_r = self.cs.general._semaphore_factory("read", "proj-a")
        sem_w = self.cs.general._semaphore_factory("write", "proj-a")
        sem_d = self.cs.general._semaphore_factory("delete", "proj-a")
        assert len({sem_r, sem_w, sem_d}) == 3

    async def test_semaphore_value_reflects_configured_limit(self) -> None:
        self.cs.general.read = 7
        sem = self.cs.general._semaphore_factory("read", "proj-x")
        assert sem._value == 7

    async def test_factory_call_freezes_global_config(self) -> None:
        assert not self.cs.is_frozen
        self.cs.general._semaphore_factory("read", "proj-a")
        assert self.cs.is_frozen

    async def test_invalid_operation_hits_assert_never(self) -> None:
        with pytest.raises(AssertionError):
            self.cs.general._semaphore_factory("totally_invalid", "proj-a")  # type: ignore[arg-type]


async def i_dont_like_5(i: int) -> int:
    if i < 5:
        return i
    elif i == 5:
        raise CogniteAPIError("no", 5, cluster="testcluster", project="testproject")
    else:
        await asyncio.sleep(0.01)
        return i


class TestExecutor:
    def test_get_event_loop_executor(self) -> None:
        executor = _get_event_loop_executor()
        assert isinstance(executor, EventLoopThreadExecutor)

    def test_get_event_loop_executor_is_singleton(self) -> None:
        assert _get_event_loop_executor() is _get_event_loop_executor()

    def test_executor_thread_is_alive_and_daemon(self) -> None:
        executor = _get_event_loop_executor()
        assert executor.is_alive()
        assert executor.daemon

    async def test_async_tasks__results_ordering_match_tasks(self) -> None:
        async def async_task(i: int) -> int:
            # Ensure tasks complete out of order:
            await asyncio.sleep(random.random() / 50)
            return i

        # We use a task that yields control and use N tasks >> concurrency_limit to really test ordering:
        tasks = [AsyncSDKTask(async_task, i) for i in range(50)]
        task_summary = await execute_async_tasks(tasks)
        task_summary.raise_compound_exception_if_failed_tasks()

        assert task_summary.results == list(range(50))

    async def test_async_tasks__results_ordering_match_tasks_even_with_failures(self) -> None:
        async def test_fn(i: int) -> int:
            # Ensure tasks complete out of order:
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
