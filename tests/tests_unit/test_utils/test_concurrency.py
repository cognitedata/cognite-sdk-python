from __future__ import annotations

import asyncio
import random
from collections.abc import Iterator
from typing import ClassVar

import pytest

from cognite.client import AsyncCogniteClient, global_config
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._concurrency import (
    AsyncSDKTask,
    ConcurrencyConfig,
    ConcurrencySettings,
    EventLoopThreadExecutor,
    HierarchicalBoundedSemaphore,
    RecordsConcurrencyOperation,
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


class TestRecordsConcurrencyConfig:
    def test_defaults(self) -> None:
        cs = ConcurrencySettings()
        assert cs.records.write == 20
        assert cs.records.query_mutable == 30
        assert cs.records.query_immutable == 10
        assert cs.records.retrieve_mutable == 20
        assert cs.records.retrieve_immutable == 10
        assert cs.records.aggregate_mutable == 10
        assert cs.records.aggregate_immutable == 5

    def test_setters_work_before_freeze(self) -> None:
        cs = ConcurrencySettings()
        cs.records.write = 10
        cs.records.retrieve_mutable = 12
        cs.records.retrieve_immutable = 4
        cs.records.aggregate_mutable = 8
        cs.records.aggregate_immutable = 3
        cs.records.query_mutable = 15
        cs.records.query_immutable = 5
        assert cs.records.write == 10
        assert cs.records.query_mutable == 15
        assert cs.records.query_immutable == 5
        assert cs.records.retrieve_mutable == 12
        assert cs.records.retrieve_immutable == 4
        assert cs.records.aggregate_mutable == 8
        assert cs.records.aggregate_immutable == 3

    @pytest.mark.parametrize(
        "attr",
        [
            "write",
            "query_mutable",
            "query_immutable",
            "retrieve_mutable",
            "retrieve_immutable",
            "aggregate_mutable",
            "aggregate_immutable",
        ],
    )
    def test_setter_raises_after_freeze(self, attr: str) -> None:
        cs = ConcurrencySettings()
        cs._freeze()
        with pytest.raises(RuntimeError, match="Cannot modify"):
            setattr(cs.records, attr, 1)

    def test_repr(self) -> None:
        cs = ConcurrencySettings()
        r = repr(cs.records)
        assert "write=20" in r
        assert "query_mutable=30" in r
        assert "query_immutable=10" in r
        assert "retrieve_mutable=20" in r
        assert "retrieve_immutable=10" in r
        assert "aggregate_mutable=10" in r
        assert "aggregate_immutable=5" in r

    @pytest.mark.parametrize(
        "dedicated, shared",
        [
            ("retrieve_mutable", "query_mutable"),
            ("retrieve_immutable", "query_immutable"),
            ("aggregate_mutable", "query_mutable"),
            ("aggregate_immutable", "query_immutable"),
        ],
    )
    def test_dedicated_exceeding_shared_raises_on_init(self, dedicated: str, shared: str) -> None:
        cs = ConcurrencySettings()
        defaults = {
            "write": 20,
            "query_mutable": 30,
            "query_immutable": 10,
            "retrieve_mutable": 20,
            "retrieve_immutable": 10,
            "aggregate_mutable": 10,
            "aggregate_immutable": 5,
        }
        shared_val = defaults[shared]
        defaults[dedicated] = shared_val + 1
        from cognite.client.utils._concurrency import RecordsGlobalConcurrencyConfig

        with pytest.raises(ValueError, match="Dedicated budget must be <= shared query budget"):
            RecordsGlobalConcurrencyConfig(cs, **defaults)

    @pytest.mark.parametrize(
        "dedicated, shared",
        [
            ("retrieve_mutable", "query_mutable"),
            ("retrieve_immutable", "query_immutable"),
            ("aggregate_mutable", "query_mutable"),
            ("aggregate_immutable", "query_immutable"),
        ],
    )
    def test_dedicated_exceeding_shared_raises_on_setter(self, dedicated: str, shared: str) -> None:
        cs = ConcurrencySettings()
        shared_val = getattr(cs.records, shared)
        with pytest.raises(ValueError, match="Dedicated budget must be <= shared query budget"):
            setattr(cs.records, dedicated, shared_val + 1)

    def test_lowering_shared_below_dedicated_raises(self) -> None:
        cs = ConcurrencySettings()
        with pytest.raises(ValueError, match="Dedicated budget must be <= shared query budget"):
            cs.records.query_mutable = 5  # retrieve_mutable=20 > 5

    def test_dedicated_equal_to_shared_is_valid(self) -> None:
        cs = ConcurrencySettings()
        cs.records.retrieve_mutable = 30  # equal to query_mutable=30, should be fine
        assert cs.records.retrieve_mutable == 30


@pytest.mark.usefixtures("fresh_unfrozen_global_concurrency")
class TestRecordsSemaphoreFactory:
    cs: ClassVar[ConcurrencySettings] = global_config.concurrency_settings

    @pytest.mark.parametrize(
        "operation, expected_value",
        [
            (RecordsConcurrencyOperation.WRITE, 20),
            (RecordsConcurrencyOperation.QUERY_MUTABLE, 30),
            (RecordsConcurrencyOperation.QUERY_IMMUTABLE, 10),
            (RecordsConcurrencyOperation.RETRIEVE_MUTABLE, 20),
            (RecordsConcurrencyOperation.RETRIEVE_IMMUTABLE, 10),
            (RecordsConcurrencyOperation.AGGREGATE_MUTABLE, 10),
            (RecordsConcurrencyOperation.AGGREGATE_IMMUTABLE, 5),
        ],
    )
    async def test_semaphore_values(self, operation: RecordsConcurrencyOperation, expected_value: int) -> None:
        sem = self.cs.records._semaphore_factory(operation, "proj-a")
        assert sem._value == expected_value

    async def test_all_operations_produce_distinct_semaphores(self) -> None:
        sems = {op: self.cs.records._semaphore_factory(op, "proj-a") for op in RecordsConcurrencyOperation}
        assert len(set(id(s) for s in sems.values())) == len(RecordsConcurrencyOperation)

    async def test_cache_hit(self) -> None:
        sem1 = self.cs.records._semaphore_factory(RecordsConcurrencyOperation.QUERY_MUTABLE, "proj-a")
        sem2 = self.cs.records._semaphore_factory(RecordsConcurrencyOperation.QUERY_MUTABLE, "proj-a")
        assert sem1 is sem2

    async def test_different_project_different_semaphore(self) -> None:
        sem_a = self.cs.records._semaphore_factory(RecordsConcurrencyOperation.QUERY_MUTABLE, "proj-a")
        sem_b = self.cs.records._semaphore_factory(RecordsConcurrencyOperation.QUERY_MUTABLE, "proj-b")
        assert sem_a is not sem_b


class TestHierarchicalBoundedSemaphore:
    async def test_acquires_both_semaphores(self) -> None:
        outer = asyncio.BoundedSemaphore(2)
        inner = asyncio.BoundedSemaphore(3)
        h = HierarchicalBoundedSemaphore(outer, inner)
        async with h:
            assert outer._value == 1
            assert inner._value == 2

    async def test_releases_both_on_exit(self) -> None:
        outer = asyncio.BoundedSemaphore(1)
        inner = asyncio.BoundedSemaphore(1)
        h = HierarchicalBoundedSemaphore(outer, inner)
        async with h:
            pass
        assert outer._value == 1
        assert inner._value == 1

    async def test_releases_on_exception(self) -> None:
        outer = asyncio.BoundedSemaphore(1)
        inner = asyncio.BoundedSemaphore(1)
        h = HierarchicalBoundedSemaphore(outer, inner)
        with pytest.raises(ValueError):
            async with h:
                raise ValueError("boom")
        assert outer._value == 1
        assert inner._value == 1

    async def test_limits_concurrency_to_min(self) -> None:
        dedicated = asyncio.BoundedSemaphore(2)
        query = asyncio.BoundedSemaphore(5)
        entered = asyncio.Event()
        hold = asyncio.Event()

        async def worker() -> None:
            async with HierarchicalBoundedSemaphore(dedicated, query):
                entered.set()
                await hold.wait()

        tasks = [asyncio.create_task(worker()) for _ in range(3)]
        await asyncio.sleep(0.01)
        assert dedicated._value == 0
        assert query._value == 3
        hold.set()
        await asyncio.gather(*tasks)


class TestHierarchicalBoundedSemaphoreAdversarial:
    """Adversarial tests targeting real failure modes in HierarchicalBoundedSemaphore.

    Two confirmed bugs are documented in the tests below:

    BUG-1 (semaphore leak on cancellation): When a task is cancelled while
    __aenter__ is blocked waiting on the second semaphore, the first semaphore
    has already been acquired but __aexit__ is never called, so it leaks.

    BUG-2 (incomplete release on mid-exit exception): When __aexit__ iterates
    in reversed order and one semaphore's release raises, the remaining
    semaphores (earlier in the list) are never released.
    """

    # --- BUG-1: Cancellation during acquisition leaks already-acquired semaphores ---

    async def test_bug1_cancellation_while_waiting_on_second_semaphore_leaks_first(self) -> None:
        """BUG: If cancelled between acquiring sem[0] and sem[1], sem[0] is never released.

        Root cause: __aenter__ acquires semaphores in a plain for-loop with no
        try/except around individual acquisitions. A CancelledError raised inside
        sem[1].__aenter__() (while it is blocked) unwinds the coroutine without
        giving __aexit__ a chance to run, so sem[0] stays acquired forever.
        """
        dedicated = asyncio.BoundedSemaphore(1)
        query = asyncio.BoundedSemaphore(0)  # permanently blocked
        h = HierarchicalBoundedSemaphore(dedicated, query)

        async def worker() -> None:
            async with h:
                pass

        task = asyncio.create_task(worker())
        await asyncio.sleep(0.02)  # let it acquire dedicated and block on query

        assert dedicated._value == 0, "dedicated should be held at this point"

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # BUG: dedicated._value is 0, not 1 — it was never released
        assert dedicated._value == 1, (
            "BUG-1: dedicated semaphore leaked after cancellation. "
            "sem[0] was acquired by __aenter__ but CancelledError prevented __aexit__ from running."
        )

    async def test_bug1_two_tasks_cancelled_both_leak(self) -> None:
        """BUG: Each cancelled task leaks one slot; with two tasks both slots are gone."""
        dedicated = asyncio.BoundedSemaphore(2)
        query = asyncio.BoundedSemaphore(0)  # permanently blocked
        h = HierarchicalBoundedSemaphore(dedicated, query)

        async def worker() -> None:
            async with h:
                pass

        t1 = asyncio.create_task(worker())
        t2 = asyncio.create_task(worker())
        await asyncio.sleep(0.02)

        t1.cancel()
        t2.cancel()
        await asyncio.gather(t1, t2, return_exceptions=True)

        # BUG: both slots leaked — dedicated is exhausted even though no work was done
        assert dedicated._value == 2, (
            "BUG-1: both dedicated slots leaked; subsequent real work can never acquire the semaphore."
        )

    async def test_bug1_wait_for_timeout_leaks_first_semaphore(self) -> None:
        """BUG: asyncio.wait_for cancels the coroutine on timeout, triggering the same leak."""
        dedicated = asyncio.BoundedSemaphore(1)
        query = asyncio.BoundedSemaphore(0)  # permanently blocked
        h = HierarchicalBoundedSemaphore(dedicated, query)

        async def worker() -> None:
            async with h:
                pass

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(worker(), timeout=0.05)

        # BUG: dedicated._value remains 0 after the timeout
        assert dedicated._value == 1, (
            "BUG-1: dedicated semaphore leaked after asyncio.wait_for timeout. "
            "Timeout internally cancels the task, hitting the same code path."
        )

    async def test_bug1_cancellation_with_three_semaphores_leaks_two(self) -> None:
        """BUG: With three semaphores, cancellation while waiting on sem[2] leaks both sem[0] and sem[1]."""
        s0 = asyncio.BoundedSemaphore(1)
        s1 = asyncio.BoundedSemaphore(1)
        s2 = asyncio.BoundedSemaphore(0)  # permanently blocked
        h = HierarchicalBoundedSemaphore(s0, s1, s2)

        async def worker() -> None:
            async with h:
                pass

        task = asyncio.create_task(worker())
        await asyncio.sleep(0.02)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert s0._value == 1, "BUG-1: s0 leaked (first semaphore)"
        assert s1._value == 1, "BUG-1: s1 leaked (second semaphore)"

    # --- BUG-2: Exception in __aexit__ of one semaphore skips releasing earlier ones ---

    async def test_bug2_exception_in_middle_release_skips_earlier_releases(self) -> None:
        """BUG: If releasing sem[1] raises, sem[0] is never released.

        __aexit__ iterates with a plain for-loop over reversed semaphores.
        An exception from any intermediate release propagates immediately,
        abandoning all remaining release calls.
        """

        class BoomSemaphore:
            """Always acquires fine; always raises on release."""

            async def __aenter__(self) -> None:
                pass

            async def __aexit__(self, *exc: object) -> None:
                raise RuntimeError("boom on release")

        sem0 = asyncio.BoundedSemaphore(1)
        sem1 = BoomSemaphore()
        sem2 = asyncio.BoundedSemaphore(1)
        # Acquire order: sem0, sem1, sem2
        # Release order (reversed): sem2, sem1 (boom!), sem0 — sem0 is never reached
        h = HierarchicalBoundedSemaphore(sem0, sem1, sem2)  # type: ignore[arg-type]

        with pytest.raises(RuntimeError, match="boom on release"):
            async with h:
                pass

        assert sem0._value == 1, (
            "BUG-2: sem0 was not released because the exception from sem1's __aexit__ "
            "aborted the release loop before sem0's turn."
        )
        assert sem2._value == 1, "sem2 (released before the bomb) should be fine"

    async def test_bug2_exception_in_first_release_skips_all_remaining(self) -> None:
        """BUG: Exception from the first release (last-acquired semaphore) skips all others."""

        class BoomSemaphore:
            async def __aenter__(self) -> None:
                pass

            async def __aexit__(self, *exc: object) -> None:
                raise RuntimeError("boom")

        sem0 = asyncio.BoundedSemaphore(1)
        sem1 = asyncio.BoundedSemaphore(1)
        sem2 = BoomSemaphore()  # last acquired = first released = first to explode
        h = HierarchicalBoundedSemaphore(sem0, sem1, sem2)  # type: ignore[arg-type]

        with pytest.raises(RuntimeError, match="boom"):
            async with h:
                pass

        assert sem0._value == 1, "BUG-2: sem0 leaked because release loop aborted at sem2"
        assert sem1._value == 1, "BUG-2: sem1 leaked because release loop aborted at sem2"

    # --- Non-bug adversarial cases (expected to pass) ---

    async def test_zero_semaphores_is_a_noop(self) -> None:
        """Edge case: constructing with no semaphores should be a transparent noop."""
        h = HierarchicalBoundedSemaphore()
        async with h:
            pass  # should not raise or block

    async def test_single_semaphore_behaves_like_plain_async_with(self) -> None:
        sem = asyncio.BoundedSemaphore(1)
        h = HierarchicalBoundedSemaphore(sem)
        async with h:
            assert sem._value == 0
        assert sem._value == 1

    async def test_nested_usage_deadlocks_with_value_one(self) -> None:
        """Nested async with on the same HierarchicalBoundedSemaphore(value=1) deadlocks.

        This is expected — BoundedSemaphore is not reentrant. The test confirms
        that the implementation does NOT protect against reentrancy.
        """
        sem = asyncio.BoundedSemaphore(1)
        h = HierarchicalBoundedSemaphore(sem)

        inner_started = False

        async def nested_worker() -> None:
            nonlocal inner_started
            async with h:
                inner_started = True
                # Attempt reentrant acquire — will deadlock
                async with h:
                    pass  # unreachable

        task = asyncio.create_task(nested_worker())
        await asyncio.sleep(0.05)

        assert inner_started, "outer context should have been entered"
        assert not task.done(), "task should still be blocked waiting on inner acquire"

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_release_on_exception_inside_context_body_both_semaphores_freed(self) -> None:
        """Normal exception inside the body (not in __aexit__) must release all semaphores."""
        sem0 = asyncio.BoundedSemaphore(1)
        sem1 = asyncio.BoundedSemaphore(1)
        h = HierarchicalBoundedSemaphore(sem0, sem1)

        with pytest.raises(ValueError, match="body exception"):
            async with h:
                raise ValueError("body exception")

        assert sem0._value == 1
        assert sem1._value == 1

    async def test_concurrent_retrieve_and_list_complete_without_deadlock(self) -> None:
        """retrieve (HierarchicalBoundedSemaphore) and list (plain semaphore) sharing
        the query semaphore must not deadlock each other."""
        dedicated = asyncio.BoundedSemaphore(2)
        query = asyncio.BoundedSemaphore(3)
        h_retrieve = HierarchicalBoundedSemaphore(dedicated, query)

        completed: list[str] = []

        async def retrieve(name: str) -> None:
            async with h_retrieve:
                await asyncio.sleep(0.01)
                completed.append(f"retrieve_{name}")

        async def list_op(name: str) -> None:
            async with query:
                await asyncio.sleep(0.01)
                completed.append(f"list_{name}")

        await asyncio.gather(
            asyncio.create_task(retrieve("A")),
            asyncio.create_task(retrieve("B")),
            asyncio.create_task(list_op("C")),
            asyncio.create_task(list_op("D")),
            asyncio.create_task(list_op("E")),
        )

        assert len(completed) == 5
        assert all(name in completed for name in ["retrieve_A", "retrieve_B", "list_C", "list_D", "list_E"])

    async def test_semaphores_fully_restored_after_many_sequential_uses(self) -> None:
        """Semaphore values must be fully restored after many normal acquire/release cycles."""
        sem0 = asyncio.BoundedSemaphore(3)
        sem1 = asyncio.BoundedSemaphore(5)
        h = HierarchicalBoundedSemaphore(sem0, sem1)

        for _ in range(20):
            async with h:
                pass

        assert sem0._value == 3
        assert sem1._value == 5


@pytest.mark.usefixtures("fresh_unfrozen_global_concurrency")
class TestRecordsGetSemaphore:
    """Tests that RecordsAPI._get_semaphore returns the right semaphore type and composition."""

    async def test_write_returns_plain_semaphore(self, async_client: AsyncCogniteClient) -> None:
        sem = async_client.data_modeling.records._get_semaphore("write")
        assert isinstance(sem, asyncio.BoundedSemaphore)

    async def test_delete_returns_plain_semaphore(self, async_client: AsyncCogniteClient) -> None:
        sem = async_client.data_modeling.records._get_semaphore("delete")
        assert isinstance(sem, asyncio.BoundedSemaphore)

    async def test_query_returns_plain_semaphore(self, async_client: AsyncCogniteClient) -> None:
        sem = async_client.data_modeling.records._get_semaphore("query", "mutable")
        assert isinstance(sem, asyncio.BoundedSemaphore)

    async def test_retrieve_returns_hierarchical(self, async_client: AsyncCogniteClient) -> None:
        sem = async_client.data_modeling.records._get_semaphore("retrieve", "mutable")
        assert isinstance(sem, HierarchicalBoundedSemaphore)

    async def test_aggregate_returns_hierarchical(self, async_client: AsyncCogniteClient) -> None:
        sem = async_client.data_modeling.records._get_semaphore("aggregate", "immutable")
        assert isinstance(sem, HierarchicalBoundedSemaphore)

    async def test_retrieve_hierarchical_wraps_correct_semaphores(self, async_client: AsyncCogniteClient) -> None:
        sem = async_client.data_modeling.records._get_semaphore("retrieve", "mutable")
        assert isinstance(sem, HierarchicalBoundedSemaphore)
        dedicated, query = sem._semaphores
        assert dedicated._value == 20  # retrieve_mutable default
        assert query._value == 30  # query_mutable default

    async def test_aggregate_immutable_wraps_correct_semaphores(self, async_client: AsyncCogniteClient) -> None:
        sem = async_client.data_modeling.records._get_semaphore("aggregate", "immutable")
        assert isinstance(sem, HierarchicalBoundedSemaphore)
        dedicated, query = sem._semaphores
        assert dedicated._value == 5  # aggregate_immutable default
        assert query._value == 10  # query_immutable default

    async def test_retrieve_and_query_share_query_semaphore(self, async_client: AsyncCogniteClient) -> None:
        retrieve_sem = async_client.data_modeling.records._get_semaphore("retrieve", "mutable")
        query_sem = async_client.data_modeling.records._get_semaphore("query", "mutable")
        assert isinstance(retrieve_sem, HierarchicalBoundedSemaphore)
        assert retrieve_sem._semaphores[1] is query_sem


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
