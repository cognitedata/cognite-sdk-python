from __future__ import annotations

import functools
from collections import UserList
from collections.abc import Callable, Sequence
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, as_completed
from typing import (
    Any,
    Literal,
    NoReturn,
    Protocol,
    TypeVar,
)

from cognite.client._constants import _RUNNING_IN_BROWSER
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils._auxiliary import no_op


class TasksSummary:
    def __init__(
        self,
        successful_tasks: list,
        unknown_tasks: list,
        failed_tasks: list,
        skipped_tasks: list,
        results: list,
        exceptions: list,
    ) -> None:
        self.successful_tasks = successful_tasks
        self.unknown_tasks = unknown_tasks
        self.failed_tasks = failed_tasks
        self.skipped_tasks = skipped_tasks
        self.results = results

        self.not_found_error: Exception | None = None
        self.duplicated_error: Exception | None = None
        self.unknown_error: Exception | None = None
        self.missing, self.duplicated, self.cluster = self._inspect_exceptions(exceptions)

    def joined_results(self, unwrap_fn: Callable = no_op) -> list:
        joined_results: list = []
        for result in self.results:
            unwrapped = unwrap_fn(result)
            if isinstance(unwrapped, (list, UserList)):
                joined_results.extend(unwrapped)
            else:
                joined_results.append(unwrapped)
        return joined_results

    @property
    def raw_api_responses(self) -> list[dict[str, Any]]:
        return [res.json() for res in self.results]

    def raise_compound_exception_if_failed_tasks(
        self,
        task_unwrap_fn: Callable = no_op,
        task_list_element_unwrap_fn: Callable | None = None,
        str_format_element_fn: Callable = no_op,
    ) -> None:
        if not (self.unknown_error or self.not_found_error or self.duplicated_error):
            return None

        if task_list_element_unwrap_fn is None:
            successful = [task_unwrap_fn(t) for t in self.successful_tasks]
            unknown = [task_unwrap_fn(t) for t in self.unknown_tasks]
            failed = [task_unwrap_fn(t) for t in self.failed_tasks]
            skipped = [task_unwrap_fn(t) for t in self.skipped_tasks]
        else:
            successful = [task_list_element_unwrap_fn(el) for t in self.successful_tasks for el in task_unwrap_fn(t)]
            unknown = [task_list_element_unwrap_fn(el) for t in self.unknown_tasks for el in task_unwrap_fn(t)]
            failed = [task_list_element_unwrap_fn(el) for t in self.failed_tasks for el in task_unwrap_fn(t)]
            skipped = [task_list_element_unwrap_fn(el) for t in self.skipped_tasks for el in task_unwrap_fn(t)]

        task_lists = dict(successful=successful, failed=failed, unknown=unknown, skipped=skipped)
        if self.unknown_error:
            self._raise_basic_api_error(str_format_element_fn, **task_lists)
        if self.not_found_error:
            self._raise_not_found_error(str_format_element_fn, **task_lists)
        if self.duplicated_error:
            self._raise_duplicated_error(str_format_element_fn, **task_lists)

    def _inspect_exceptions(self, exceptions: list[Exception]) -> tuple[list, list, str | None]:
        cluster, missing, duplicated = None, [], []
        for exc in exceptions:
            if not isinstance(exc, CogniteAPIError):
                self.unknown_error = exc
                continue

            cluster = cluster or exc.cluster
            if exc.code in (400, 422) and exc.missing is not None:
                missing.extend(exc.missing)
                self.not_found_error = exc

            elif exc.code == 409 and exc.duplicated is not None:
                duplicated.extend(exc.duplicated)
                self.duplicated_error = exc
            else:
                self.unknown_error = exc
        return missing, duplicated, cluster

    def _raise_basic_api_error(self, unwrap_fn: Callable, **task_lists: list) -> NoReturn:
        if isinstance(self.unknown_error, CogniteAPIError) and (task_lists["failed"] or task_lists["unknown"]):
            raise CogniteAPIError(
                message=self.unknown_error.message,
                code=self.unknown_error.code,
                x_request_id=self.unknown_error.x_request_id,
                missing=self.missing,
                duplicated=self.duplicated,
                extra=self.unknown_error.extra,
                unwrap_fn=unwrap_fn,
                cluster=self.cluster,
                **task_lists,
            )
        raise self.unknown_error  # type: ignore [misc]

    def _raise_not_found_error(self, unwrap_fn: Callable, **task_lists: list) -> NoReturn:
        raise CogniteNotFoundError(self.missing, unwrap_fn=unwrap_fn, **task_lists) from self.not_found_error

    def _raise_duplicated_error(self, unwrap_fn: Callable, **task_lists: list) -> NoReturn:
        raise CogniteDuplicatedError(self.duplicated, unwrap_fn=unwrap_fn, **task_lists) from self.duplicated_error


T_Result = TypeVar("T_Result", covariant=True)


class TaskExecutor(Protocol):
    def submit(self, fn: Callable[..., T_Result], /, *args: Any, **kwargs: Any) -> TaskFuture[T_Result]: ...


class TaskFuture(Protocol[T_Result]):
    def result(self) -> T_Result: ...


class SyncFuture(TaskFuture[T_Result]):
    def __init__(self, fn: Callable[..., T_Result], *args: Any, **kwargs: Any) -> None:
        self._task = functools.partial(fn, *args, **kwargs)
        self._result: T_Result | None = None

    def result(self) -> T_Result:
        if self._result is None:
            self._result = self._task()
        return self._result


class MainThreadExecutor(TaskExecutor):
    """
    In order to support executing sdk methods in the browser using pyodide (a port of CPython to webassembly),
    we need to be able to turn off the usage of threading. So we have this executor which implements the Executor
    protocol but just executes everything serially in the main thread.
    """

    def __init__(self) -> None:
        # This "queue" is not used, but currently needed because of the datapoints logic that
        # decides when to add new tasks to the task executor task pool.
        class AlwaysEmpty:
            def empty(self) -> Literal[True]:
                return True

        self._work_queue = AlwaysEmpty()

    def submit(self, fn: Callable[..., T_Result], /, *args: Any, **kwargs: Any) -> SyncFuture:
        return SyncFuture(fn, *args, **kwargs)


_DATA_MODELING_MAX_WORKERS = 1
_THREAD_POOL_EXECUTOR_SINGLETON: ThreadPoolExecutor
_MAIN_THREAD_EXECUTOR_SINGLETON = MainThreadExecutor()
_DATA_MODELING_THREAD_POOL_EXECUTOR_SINGLETON: ThreadPoolExecutor


class ConcurrencySettings:
    executor_type: Literal["threadpool", "mainthread"] = "threadpool"

    @classmethod
    def uses_threadpool(cls) -> bool:
        return cls.executor_type == "threadpool"

    @classmethod
    def uses_mainthread(cls) -> bool:
        return cls.executor_type == "mainthread"

    @classmethod
    def get_executor(cls, max_workers: int) -> TaskExecutor:
        if cls.uses_threadpool():
            return cls.get_thread_pool_executor(max_workers)
        elif cls.uses_mainthread():
            return cls.get_mainthread_executor()
        raise RuntimeError(f"Invalid executor type '{cls.executor_type}'")

    @classmethod
    def get_mainthread_executor(cls) -> TaskExecutor:
        return _MAIN_THREAD_EXECUTOR_SINGLETON

    @classmethod
    def get_thread_pool_executor(cls, max_workers: int) -> ThreadPoolExecutor:
        assert cls.uses_threadpool(), "use get_executor instead"
        global _THREAD_POOL_EXECUTOR_SINGLETON

        if max_workers < 1:
            raise RuntimeError(f"Number of workers should be >= 1, was {max_workers}")
        try:
            executor = _THREAD_POOL_EXECUTOR_SINGLETON
        except NameError:
            # TPE has not been initialized
            executor = _THREAD_POOL_EXECUTOR_SINGLETON = ThreadPoolExecutor(max_workers)
        return executor

    @classmethod
    def get_thread_pool_executor_or_raise(cls, max_workers: int) -> ThreadPoolExecutor:
        if cls.uses_threadpool():
            return cls.get_thread_pool_executor(max_workers)

        if _RUNNING_IN_BROWSER:
            raise RuntimeError("The method you tried to use is not available in Pyodide/WASM")
        raise RuntimeError(
            "The method you tried to use requires a version of Python with a working implementation of threads."
        )

    @classmethod
    def get_data_modeling_executor(cls) -> TaskExecutor:
        """
        The data modeling backend has different concurrency limits compared with the rest of CDF.
        Thus, we use a dedicated executor for these endpoints to match the backend requirements.

        Returns:
            TaskExecutor: The data modeling executor.
        """
        if cls.uses_mainthread():
            return cls.get_mainthread_executor()

        global _DATA_MODELING_THREAD_POOL_EXECUTOR_SINGLETON
        try:
            executor = _DATA_MODELING_THREAD_POOL_EXECUTOR_SINGLETON
        except NameError:
            # TPE has not been initialized
            executor = _DATA_MODELING_THREAD_POOL_EXECUTOR_SINGLETON = ThreadPoolExecutor(_DATA_MODELING_MAX_WORKERS)
        return executor


def execute_tasks_serially(
    func: Callable[..., T_Result],
    tasks: Sequence[tuple | dict],
    fail_fast: bool = False,
) -> TasksSummary:
    results, exceptions = [], []
    successful_tasks, failed_tasks, unknown_result_tasks, skipped_tasks = [], [], [], []

    for i, task in enumerate(tasks):
        try:
            if isinstance(task, dict):
                results.append(func(**task))
            elif isinstance(task, tuple):
                results.append(func(*task))
            else:
                raise TypeError(f"invalid task type: {type(task)}")
            successful_tasks.append(task)

        except Exception as err:
            exceptions.append(err)
            if classify_error(err) == "failed":
                failed_tasks.append(task)
            else:
                unknown_result_tasks.append(task)

            if fail_fast:
                skipped_tasks = list(tasks[i + 1 :])
                break

    return TasksSummary(successful_tasks, unknown_result_tasks, failed_tasks, skipped_tasks, results, exceptions)


def execute_tasks(
    func: Callable[..., T_Result],
    tasks: Sequence[tuple | dict],
    max_workers: int,
    fail_fast: bool = False,
    executor: TaskExecutor | None = None,
) -> TasksSummary:
    """
    Will use a default executor if one is not passed explicitly. The default executor type uses a thread pool but can
    be changed using ConcurrencySettings.executor_type.

    Results are returned in the same order as that given by tasks.
    """
    if ConcurrencySettings.uses_mainthread() or isinstance(executor, MainThreadExecutor):
        return execute_tasks_serially(func, tasks, fail_fast)
    elif isinstance(executor, ThreadPoolExecutor) or executor is None:
        pass
    else:
        raise TypeError("executor must be a ThreadPoolExecutor or MainThreadExecutor")

    executor = executor or ConcurrencySettings.get_thread_pool_executor(max_workers)
    task_order = [id(task) for task in tasks]

    futures_dct: dict[Future, tuple | dict] = {}
    for task in tasks:
        if isinstance(task, dict):
            futures_dct[executor.submit(func, **task)] = task
        elif isinstance(task, tuple):
            futures_dct[executor.submit(func, *task)] = task
        else:
            raise TypeError(f"invalid task type: {type(task)}")

    results: dict[int, tuple | dict] = {}
    successful_tasks: dict[int, tuple | dict] = {}
    failed_tasks, unknown_result_tasks, skipped_tasks, exceptions = [], [], [], []

    for fut in as_completed(futures_dct):
        task = futures_dct[fut]
        try:
            res = fut.result()
            results[id(task)] = task
            successful_tasks[id(task)] = res
        except CancelledError:
            # In fail-fast mode, after an error has been raised, we attempt to cancel and skip tasks:
            skipped_tasks.append(task)
            continue

        except Exception as err:
            exceptions.append(err)
            if classify_error(err) == "failed":
                failed_tasks.append(task)
            else:
                unknown_result_tasks.append(task)

            if fail_fast:
                for fut in futures_dct:
                    fut.cancel()

    ordered_successful_tasks = [results[task_id] for task_id in task_order if task_id in results]
    ordered_results = [successful_tasks[task_id] for task_id in task_order if task_id in successful_tasks]
    return TasksSummary(
        ordered_successful_tasks,
        unknown_result_tasks,
        failed_tasks,
        skipped_tasks,
        ordered_results,
        exceptions,
    )


def classify_error(err: Exception) -> Literal["failed", "unknown"]:
    if isinstance(err, CogniteAPIError) and err.code >= 500:
        return "unknown"
    return "failed"
