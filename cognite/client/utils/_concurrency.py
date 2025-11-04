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
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils._auxiliary import no_op


@cache
def get_global_semaphore() -> asyncio.BoundedSemaphore:
    from cognite.client import global_config

    return asyncio.BoundedSemaphore(global_config.max_workers)


@cache
def get_global_data_modeling_semaphore() -> asyncio.BoundedSemaphore:
    return asyncio.BoundedSemaphore(2)


class AsyncSDKTask:
    """
    This class stores info about a task that should be run asynchronously (like what function to call,
    and with what arguments). This is quite useful when we later need to map e.g. which input arguments
    (typically, which identifiers) resulted in some failed API request.
    """

    def __init__(self, fn: Callable[..., Coroutine], /, *args: Any, **kwargs: Any) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self._async_task: asyncio.Task | None = None

    def schedule(self) -> asyncio.Task:
        """Schedule the task for execution. Can only be called once."""
        if self._async_task is not None:
            raise RuntimeError("Task has already been scheduled")

        self._async_task = asyncio.create_task(self.fn(*self.args, **self.kwargs))
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

        self.not_found_error: CogniteNotFoundError | None = None
        self.duplicated_error: CogniteDuplicatedError | None = None
        self.unknown_error: Exception | None = None
        self.missing, self.duplicated, self.cluster, self.project = self._inspect_exceptions(exceptions)

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

    def _inspect_exceptions(self, exceptions: list[Exception]) -> tuple[list, list, str | None, str | None]:
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
            successful=successful,
            failed=failed,
            unknown=unknown,
            skipped=skipped,
        ) from cause


_T = TypeVar("_T")


class EventLoopThreadExecutor(threading.Thread):
    def __init__(self, loop: asyncio.AbstractEventLoop | None = None, daemon: bool = True) -> None:
        super().__init__(name=type(self).__name__, daemon=daemon)
        self._event_loop = loop or asyncio.new_event_loop()

    def run(self) -> None:
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.run_forever()

    def stop(self) -> None:
        self._event_loop.call_soon_threadsafe(self._event_loop.stop)
        self.join()

    def run_coro(self, coro: Coroutine[Any, Any, _T], timeout: float | None = None) -> _T:
        return asyncio.run_coroutine_threadsafe(coro, self._event_loop).result(timeout)


# We need this in order to support a synchronous Cognite client.
_INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON: EventLoopThreadExecutor


class ConcurrencySettings:
    @classmethod
    def _get_event_loop_executor(cls) -> EventLoopThreadExecutor:
        global _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON
        try:
            return _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON
        except NameError:
            # First time we need to initialize:
            from cognite.client import global_config

            executor = _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON = EventLoopThreadExecutor(
                global_config.event_loop
            )
            executor.start()
            return executor
