from __future__ import annotations

import asyncio
import threading
import warnings
from collections import UserList
from collections.abc import Callable, Coroutine
from functools import cache
from typing import (
    Any,
    NoReturn,
    TypeVar,
    cast,
)

from cognite.client._constants import _RUNNING_IN_BROWSER
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteImportError, CogniteNotFoundError
from cognite.client.utils._auxiliary import no_op


# We add the 'project' argument to make sure concurrency limits are applied per project:
@cache
def get_global_semaphore(project: str) -> asyncio.BoundedSemaphore:
    from cognite.client import global_config

    return asyncio.BoundedSemaphore(global_config.max_workers)


@cache
def get_global_datapoints_semaphore(project: str) -> asyncio.BoundedSemaphore:
    from cognite.client import global_config

    return asyncio.BoundedSemaphore(global_config.max_workers)


@cache
def get_global_data_modeling_semaphore(project: str) -> asyncio.BoundedSemaphore:
    return asyncio.BoundedSemaphore(2)


class AsyncSDKTask:
    """
    This class stores info about a task that should be run asynchronously (like what function to call,
    and with what arguments). This is quite useful when we later need to map e.g. which input arguments
    (typically, which identifiers) resulted in some failed API request.

    It has a special ``__getitem__`` method for easy argument access:

        >>> task = AsyncSDKTask(some_fn, 123, payload={"a": 1, "b": 2})
        >>> task[0]  # gets the first positional argument
        123
        >>> task["payload"]  # gets the "payload" keyword argument
        {"a": 1, "b": 2}
    """

    def __init__(self, fn: Callable[..., Coroutine], /, *args: Any, **kwargs: Any) -> None:
        self.fn = fn
        self._args = args
        self._kwargs = kwargs
        self._async_task: asyncio.Task | None = None

    def __getitem__(self, item: int | str) -> Any:
        match item:
            case int():
                return self._args[item]
            case str():
                return self._kwargs[item]
            case _:
                raise TypeError("Use int to get 'args' and str to get 'kwargs', e.g. task[0], task['payload']")

    def schedule(self) -> asyncio.Task:
        """Schedule the task for execution. Can only be called once."""
        if self._async_task is not None:
            raise RuntimeError("Task has already been scheduled")

        self._async_task = asyncio.create_task(self.fn(*self._args, **self._kwargs))
        return self._async_task

    @property
    def async_task(self) -> asyncio.Task:
        if self._async_task is None:
            raise RuntimeError("Task has not been scheduled yet")
        return self._async_task

    def exception(self) -> BaseException | None:
        return self.async_task.exception()

    def result(self) -> Any:
        return self.async_task.result()

    def cancelled(self) -> bool:
        return self.async_task.cancelled()


class TasksSummary:
    def __init__(
        self,
        results: list[Any],
        successful_tasks: list[AsyncSDKTask],
        unsuccessful_tasks: list[AsyncSDKTask] | None = None,
        skipped_tasks: list[AsyncSDKTask] | None = None,
        exceptions: list[BaseException] | None = None,
    ) -> None:
        self.results = results
        self.successful_tasks = successful_tasks
        self.unknown_tasks, self.failed_tasks = self._categorize_failed_vs_unknown(unsuccessful_tasks or [])
        self.skipped_tasks = skipped_tasks or []

        self.not_found_error: CogniteNotFoundError | None = None
        self.duplicated_error: CogniteDuplicatedError | None = None
        self.unknown_error: BaseException | None = None
        self.missing, self.duplicated, self.cluster, self.project = self._inspect_exceptions(exceptions or [])

    @staticmethod
    def _categorize_failed_vs_unknown(
        unsuccessful_tasks: list[AsyncSDKTask],
    ) -> tuple[list[AsyncSDKTask], list[AsyncSDKTask]]:
        from cognite.client._basic_api_client import FailedRequestHandler

        unknown_and_failed: tuple[list[AsyncSDKTask], list[AsyncSDKTask]] = [], []
        for task in unsuccessful_tasks:
            err = cast(BaseException, task.exception())  # Task is unsuccessful exactly because this is set
            is_failed = FailedRequestHandler.classify_error(err) == "failed"
            unknown_and_failed[is_failed].append(task)
        return unknown_and_failed

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
        self, task_unwrap_fn: Callable = no_op, task_list_element_unwrap_fn: Callable | None = None
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

        if self.unknown_error:
            self._raise_basic_api_error(successful=successful, failed=failed, unknown=unknown, skipped=skipped)

        if self.not_found_error:
            self._raise_specific_error(
                cause=self.not_found_error,
                error=CogniteNotFoundError,
                successful=successful,
                failed=failed,
                unknown=unknown,
                skipped=skipped,
            )
        if self.duplicated_error:
            self._raise_specific_error(
                cause=self.duplicated_error,
                error=CogniteDuplicatedError,
                successful=successful,
                failed=failed,
                unknown=unknown,
                skipped=skipped,
            )

    def _inspect_exceptions(self, exceptions: list[BaseException]) -> tuple[list, list, str | None, str | None]:
        cluster = None
        project = None
        missing: list[dict] = []
        duplicated: list[dict] = []
        for exc in exceptions:
            match exc:
                case CogniteNotFoundError():
                    missing.extend(exc.missing)
                    self.not_found_error = exc
                case CogniteDuplicatedError():
                    duplicated.extend(exc.duplicated)
                    self.duplicated_error = exc
                case CogniteAPIError():
                    self.unknown_error = exc
                case _:
                    self.unknown_error = exc
                    continue

            cluster = cluster or exc.cluster
            project = project or exc.project

        return missing, duplicated, cluster, project

    def _raise_basic_api_error(self, successful: list, failed: list, unknown: list, skipped: list) -> NoReturn:
        if isinstance(self.unknown_error, CogniteAPIError) and (failed or unknown):
            raise CogniteAPIError(
                message=self.unknown_error.message,
                code=self.unknown_error.code,
                x_request_id=self.unknown_error.x_request_id,
                missing=self.missing,
                duplicated=self.duplicated,
                extra=self.unknown_error.extra,
                cluster=self.cluster,
                project=self.project,
                successful=successful,
                failed=failed,
                unknown=unknown,
                skipped=skipped,
            )
        raise self.unknown_error  # type: ignore [misc]

    def _raise_specific_error(
        self,
        cause: CogniteAPIError,
        error: type[CogniteNotFoundError | CogniteDuplicatedError],
        successful: list,
        failed: list,
        unknown: list,
        skipped: list,
    ) -> NoReturn:
        raise error(
            message=cause.message,
            code=cause.code,
            x_request_id=cause.x_request_id,
            missing=self.missing,
            duplicated=self.duplicated,
            extra=cause.extra,
            cluster=self.cluster,
            project=self.project,
            successful=successful,
            failed=failed,
            unknown=unknown,
            skipped=skipped,
        ) from cause


_T = TypeVar("_T")


class EventLoopThreadExecutor(threading.Thread):
    def __init__(self, loop: asyncio.AbstractEventLoop | None = None, daemon: bool = True) -> None:
        super().__init__(name=type(self).__name__, daemon=daemon)
        self._inside_jupyter = self._detect_jupyter()
        self._event_loop = self._patch_loop_for_jupyter(loop) or asyncio.new_event_loop()

    @staticmethod
    def _detect_jupyter() -> bool:
        try:
            from IPython import get_ipython  # type: ignore [attr-defined]

            return "IPKernelApp" in get_ipython().config
        except Exception:
            return False

    def _patch_loop_for_jupyter(self, loop: asyncio.AbstractEventLoop | None) -> asyncio.AbstractEventLoop | None:
        """
        From the 'nest_asyncio' package: By design asyncio does not allow its event loop to be nested. This presents a
        practical problem: When in an environment where the event loop is already running it's impossible to run
        tasks and wait for the result.
        """
        if not self._inside_jupyter:
            return loop

        if loop is None:
            try:
                import nest_asyncio  # type: ignore [import-not-found]
            except ImportError:
                raise CogniteImportError(
                    module="nest_asyncio",
                    message="Inside Jupyter notebooks, the 'nest_asyncio' package is required if you want to use the "
                    '"non-async" CogniteClient. This is because Jupyter already runs an event loop that we need '
                    "to patch (`pip install nest_asyncio`). Alternatively, you can use the AsyncCogniteClient "
                    "which does not require any extra packages, but requires the use of 'await', e.g.: "
                    "`dps = await async_client.time_series.data.retrieve(...)`",
                ) from None
            try:
                # Jupyter: reuse the already running loop but patch it:
                loop = asyncio.get_running_loop()
                nest_asyncio.apply(loop)
                return loop
            except RuntimeError:
                return None  # this would be very unexpected
        else:
            warnings.warn(
                RuntimeWarning(
                    "Overriding the event loop is not recommended inside Jupyter notebooks "
                    "since Jupyter already runs an event loop. Proceeding with the provided loop anyway, "
                    "beware of potential issues."
                )
            )
            return loop

    def run(self) -> None:
        if not self._inside_jupyter:
            asyncio.set_event_loop(self._event_loop)
            self._event_loop.run_forever()

    def stop(self) -> None:
        if not self._inside_jupyter:
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)
            self.join()

    def run_coro(self, coro: Coroutine[_T, Any, _T], timeout: float | None = None) -> _T:
        if self._inside_jupyter:
            return asyncio.get_event_loop().run_until_complete(coro)
        else:
            return asyncio.run_coroutine_threadsafe(coro, self._event_loop).result(timeout)


class _PyodideEventLoopExecutor:
    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        import pyodide  # type: ignore [import-not-found]

        if loop is not None:
            raise RuntimeError("Overriding the event loop is not possible in the browser")

        elif not pyodide.ffi.can_run_sync():
            warnings.warn(
                RuntimeWarning(
                    "Browser most likely not supported, please use a Chromium-based browser like Chrome or Microsoft "
                    "Edge. Reason: WebAssembly stack switching is not supported in this JavaScript runtime. "
                    "Note: You can always use the AsyncCogniteClient, but it requires the use of 'await', e.g.: "
                    "`dps = await client.time_series.data.retrieve(...)`"
                )
            )
        self.run_coro = pyodide.ffi.run_sync

    def start(self) -> None:
        pass


# We need this in order to support a synchronous Cognite client.
_INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON: EventLoopThreadExecutor


def _get_event_loop_executor() -> EventLoopThreadExecutor:
    global _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON
    try:
        return _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON
    except NameError:
        # First time we need to initialize:
        from cognite.client import global_config

        ex_cls = EventLoopThreadExecutor
        if _RUNNING_IN_BROWSER:
            ex_cls = cast(type[EventLoopThreadExecutor], _PyodideEventLoopExecutor)

        executor = _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON = ex_cls(global_config.event_loop)
        executor.start()
        return executor


async def execute_async_tasks_with_fail_fast(tasks: list[AsyncSDKTask]) -> TasksSummary:
    # If no future raises an exception then this is equivalent to asyncio.ALL_COMPLETED:
    done, pending = await asyncio.wait(
        [task.schedule() for task in tasks],
        return_when=asyncio.FIRST_EXCEPTION,
    )
    if all(task.exception() is None for task in done):
        return TasksSummary([task.result() for task in done], successful_tasks=tasks)

    # Something failed, and because of fail-fast, we (attempt to) cancel all pending tasks:
    if pending:  # while we are waiting on 3.11 and asyncio.TaskGroup...
        for unfinished in pending:
            unfinished.cancel()

        # Wait for all cancellations to be processed:
        await asyncio.gather(*pending, return_exceptions=True)

    result, successful, unsuccessful, skipped, exceptions = [], [], [], [], []
    for task in tasks:
        if task.cancelled():
            skipped.append(task)
        elif err := task.exception():
            exceptions.append(err)
            unsuccessful.append(task)
        else:
            result.append(task.result())
            successful.append(task)

    return TasksSummary(
        result,
        successful_tasks=successful,
        unsuccessful_tasks=unsuccessful,
        skipped_tasks=skipped,
        exceptions=exceptions,
    )


async def execute_async_tasks(tasks: list[AsyncSDKTask], fail_fast: bool = False) -> TasksSummary:
    if not tasks:
        return TasksSummary([], successful_tasks=[], unsuccessful_tasks=[], exceptions=[])

    elif fail_fast:
        return await execute_async_tasks_with_fail_fast(tasks)

    await asyncio.wait([task.schedule() for task in tasks], return_when=asyncio.ALL_COMPLETED)

    results, successful, unsuccessful, exceptions = [], [], [], []
    for task in tasks:
        if err := task.exception():
            exceptions.append(err)
            unsuccessful.append(task)
        else:
            results.append(task.result())
            successful.append(task)

    return TasksSummary(
        results,
        successful_tasks=successful,
        unsuccessful_tasks=unsuccessful,
        exceptions=exceptions,
    )
