from __future__ import annotations

from collections import UserList
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Sequence, Tuple, TypeVar, Union

from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError


class TasksSummary:
    def __init__(
        self, successful_tasks: List, unknown_tasks: List, failed_tasks: List, results: List, exceptions: List
    ):
        self.successful_tasks = successful_tasks
        self.unknown_tasks = unknown_tasks
        self.failed_tasks = failed_tasks
        self.results = results
        self.exceptions = exceptions

    def joined_results(self, unwrap_fn: Optional[Callable] = None) -> list:
        unwrap_fn = unwrap_fn or (lambda x: x)
        joined_results: list = []
        for result in self.results:
            unwrapped = unwrap_fn(result)
            if isinstance(unwrapped, (list, UserList)):
                joined_results.extend(unwrapped)
            else:
                joined_results.append(unwrapped)
        return joined_results

    def raise_compound_exception_if_failed_tasks(
        self,
        task_unwrap_fn: Optional[Callable] = None,
        task_list_element_unwrap_fn: Optional[Callable] = None,
        str_format_element_fn: Optional[Callable] = None,
    ) -> None:
        if not self.exceptions:
            return None
        task_unwrap_fn = (lambda x: x) if task_unwrap_fn is None else task_unwrap_fn
        if task_list_element_unwrap_fn is not None:
            successful = []
            for t in self.successful_tasks:
                successful.extend([task_list_element_unwrap_fn(el) for el in task_unwrap_fn(t)])
            unknown = []
            for t in self.unknown_tasks:
                unknown.extend([task_list_element_unwrap_fn(el) for el in task_unwrap_fn(t)])
            failed = []
            for t in self.failed_tasks:
                failed.extend([task_list_element_unwrap_fn(el) for el in task_unwrap_fn(t)])
        else:
            successful = [task_unwrap_fn(t) for t in self.successful_tasks]
            unknown = [task_unwrap_fn(t) for t in self.unknown_tasks]
            failed = [task_unwrap_fn(t) for t in self.failed_tasks]

        collect_exc_info_and_raise(
            self.exceptions, successful=successful, failed=failed, unknown=unknown, unwrap_fn=str_format_element_fn
        )


def collect_exc_info_and_raise(
    exceptions: List[Exception],
    successful: Optional[List] = None,
    failed: Optional[List] = None,
    unknown: Optional[List] = None,
    unwrap_fn: Optional[Callable] = None,
) -> None:
    missing: List = []
    duplicated: List = []
    missing_exc = None
    dup_exc = None
    unknown_exc: Optional[Exception] = None
    for exc in exceptions:
        if isinstance(exc, CogniteAPIError):
            if exc.code in [400, 422] and exc.missing is not None:
                missing.extend(exc.missing)
                missing_exc = exc
            elif exc.code == 409 and exc.duplicated is not None:
                duplicated.extend(exc.duplicated)
                dup_exc = exc
            else:
                unknown_exc = exc
        else:
            unknown_exc = exc

    if unknown_exc:
        if isinstance(unknown_exc, CogniteAPIError) and (failed or unknown):
            raise CogniteAPIError(
                message=unknown_exc.message,
                code=unknown_exc.code,
                x_request_id=unknown_exc.x_request_id,
                missing=missing,
                duplicated=duplicated,
                successful=successful,
                failed=failed,
                unknown=unknown,
                unwrap_fn=unwrap_fn,
                extra=unknown_exc.extra,
            )
        raise unknown_exc

    if missing_exc:
        raise CogniteNotFoundError(
            not_found=missing, successful=successful, failed=failed, unknown=unknown, unwrap_fn=unwrap_fn
        ) from missing_exc

    if dup_exc:
        raise CogniteDuplicatedError(
            duplicated=duplicated, successful=successful, failed=failed, unknown=unknown, unwrap_fn=unwrap_fn
        ) from dup_exc


T_Result = TypeVar("T_Result", covariant=True)


class TaskExecutor(Protocol):
    def submit(self, fn: Callable[..., T_Result], *args: Any, **kwargs: Any) -> TaskFuture[T_Result]:
        ...


class TaskFuture(Protocol[T_Result]):
    def result(self) -> T_Result:
        ...


class SyncFuture(TaskFuture):
    def __init__(self, fn: Callable[..., T_Result], *args: Any, **kwargs: Any):
        self.__fn = fn
        self.__args = args
        self.__kwargs = kwargs

    def result(self) -> T_Result:
        return self.__fn(*self.__args, **self.__kwargs)


class MainThreadExecutor(TaskExecutor):
    """
    In order to support executing sdk methods in the browser using pyodide (a port of CPython to webassembly),
    we need to be able to turn off the usage of threading. So we have this executor which implements the Executor
    protocol but just executes everything serially in the main thread.
    """

    def submit(self, fn: Callable, *args: Any, **kwargs: Any) -> SyncFuture:
        return SyncFuture(fn, *args, **kwargs)


_THREAD_POOL_EXECUTOR_SINGLETON: ThreadPoolExecutor
_MAIN_THREAD_EXECUTOR_SINGLETON = MainThreadExecutor()


class ConcurrencySettings:
    executor_type: Literal["threadpool", "mainthread"] = "threadpool"


def get_executor(max_workers: int) -> TaskExecutor:
    global _THREAD_POOL_EXECUTOR_SINGLETON

    if max_workers < 1:
        raise RuntimeError(f"Number of workers should be >= 1, was {max_workers}")

    if ConcurrencySettings.executor_type == "threadpool":
        try:
            executor: TaskExecutor = _THREAD_POOL_EXECUTOR_SINGLETON
        except NameError:
            # TPE has not been initialized
            executor = _THREAD_POOL_EXECUTOR_SINGLETON = ThreadPoolExecutor(max_workers)
    elif ConcurrencySettings.executor_type == "mainthread":
        executor = _MAIN_THREAD_EXECUTOR_SINGLETON
    else:
        raise RuntimeError(f"Invalid executor type '{ConcurrencySettings.executor_type}'")
    return executor


def execute_tasks(
    func: Callable[..., T_Result],
    tasks: Union[Sequence[Tuple], List[Dict]],
    max_workers: int,
    executor: Optional[TaskExecutor] = None,
) -> TasksSummary:
    """
    Will use a default executor if one is not passed explicitly. The default executor type uses a thread pool but can
    be changed using ExecutorSettings.executor_type.
    """
    executor = executor or get_executor(max_workers)
    futures = []
    for task in tasks:
        if isinstance(task, dict):
            futures.append(executor.submit(func, **task))
        elif isinstance(task, tuple):
            futures.append(executor.submit(func, *task))

    successful_tasks = []
    failed_tasks = []
    unknown_result_tasks = []
    results = []
    exceptions = []
    for i, f in enumerate(futures):
        try:
            res = f.result()
            successful_tasks.append(tasks[i])
            results.append(res)
        except Exception as e:
            exceptions.append(e)
            if isinstance(e, CogniteAPIError):
                if e.code < 500:
                    failed_tasks.append(tasks[i])
                else:
                    unknown_result_tasks.append(tasks[i])
            else:
                failed_tasks.append(tasks[i])

    return TasksSummary(successful_tasks, unknown_result_tasks, failed_tasks, results, exceptions)
