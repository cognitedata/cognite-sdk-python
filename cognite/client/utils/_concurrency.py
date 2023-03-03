import functools
import inspect
from collections import UserList
from concurrent.futures import CancelledError
from concurrent.futures.thread import ThreadPoolExecutor
from typing import TypeVar

from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils._priority_tpe import PriorityThreadPoolExecutor


class TasksSummary:
    def __init__(self, successful_tasks, unknown_tasks, failed_tasks, results, exceptions):
        self.successful_tasks = successful_tasks
        self.unknown_tasks = unknown_tasks
        self.failed_tasks = failed_tasks
        self.results = results
        self.exceptions = exceptions

    def joined_results(self, unwrap_fn=None):
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
        self, task_unwrap_fn=None, task_list_element_unwrap_fn=None, str_format_element_fn=None
    ):
        if not self.exceptions:
            return None
        task_unwrap_fn = (lambda x: x) if (task_unwrap_fn is None) else task_unwrap_fn
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


def collect_exc_info_and_raise(exceptions, successful=None, failed=None, unknown=None, unwrap_fn=None):
    missing: List = []
    duplicated: List = []
    missing_exc = None
    dup_exc = None
    unknown_exc: Optional[Exception] = None
    for exc in exceptions:
        if isinstance(exc, CogniteAPIError):
            if (exc.code in [400, 422]) and (exc.missing is not None):
                missing.extend(exc.missing)
                missing_exc = exc
            elif (exc.code == 409) and (exc.duplicated is not None):
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


class TaskExecutor:
    def submit(self, fn, *args, **kwargs):
        ...


class TaskFuture:
    def result(self):
        ...


class SyncFuture(TaskFuture):
    def __init__(self, fn, *args, **kwargs):
        self.__fn = fn
        self.__args = args
        self.__kwargs = kwargs

    def result(self):
        return self.__fn(*self.__args, **self.__kwargs)


class MainThreadExecutor(TaskExecutor):
    def submit(self, fn, *args, **kwargs):
        return SyncFuture(fn, *args, **kwargs)


class ExtendedSyncFuture(TaskFuture):
    def __init__(self, fn, *args, **kwargs):
        self._task = functools.partial(fn, *args, **kwargs)
        self._result = None
        self._is_cancelled = False

    def result(self):
        if self._is_cancelled:
            raise CancelledError
        if self._result is None:
            self._result = self._task()
        return self._result

    def cancel(self):
        self._is_cancelled = True


class ExtendedMainThreadExecutor(TaskExecutor):
    __doc__ = MainThreadExecutor.__doc__

    def submit(self, fn, *args, **kwargs):
        if "priority" in inspect.signature(fn).parameters:
            raise TypeError(f"Given function {fn} cannot accept reserved parameter name `priority`")
        kwargs.pop("priority", None)
        return ExtendedSyncFuture(fn, *args, **kwargs)

    def shutdown(self, wait=False):
        return None

    @staticmethod
    def as_completed(dct):
        return iter(dct.copy())

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.shutdown()


_THREAD_POOL_EXECUTOR_SINGLETON: ThreadPoolExecutor
_MAIN_THREAD_EXECUTOR_SINGLETON = MainThreadExecutor()


class ConcurrencySettings:
    executor_type = "threadpool"
    priority_executor_type = "priority_threadpool"


def get_executor(max_workers):
    global _THREAD_POOL_EXECUTOR_SINGLETON
    if max_workers < 1:
        raise RuntimeError(f"Number of workers should be >= 1, was {max_workers}")
    if ConcurrencySettings.executor_type == "threadpool":
        try:
            executor: TaskExecutor = _THREAD_POOL_EXECUTOR_SINGLETON
        except NameError:
            executor = _THREAD_POOL_EXECUTOR_SINGLETON = ThreadPoolExecutor(max_workers)
    elif ConcurrencySettings.executor_type == "mainthread":
        executor = _MAIN_THREAD_EXECUTOR_SINGLETON
    else:
        raise RuntimeError(f"Invalid executor type '{ConcurrencySettings.executor_type}'")
    return executor


def get_priority_executor(max_workers):
    if max_workers < 1:
        raise RuntimeError(f"Number of workers should be >= 1, was {max_workers}")
    if ConcurrencySettings.priority_executor_type == "priority_threadpool":
        return PriorityThreadPoolExecutor(max_workers)
    elif ConcurrencySettings.priority_executor_type == "mainthread":
        return ExtendedMainThreadExecutor()
    raise RuntimeError(f"Invalid priority-queue executor type '{ConcurrencySettings.priority_executor_type}'")


def execute_tasks(func, tasks, max_workers, executor=None):
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
    for (i, f) in enumerate(futures):
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
