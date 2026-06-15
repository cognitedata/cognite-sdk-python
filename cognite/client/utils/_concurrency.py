from __future__ import annotations

import asyncio
import functools
import threading
import warnings
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Callable, Coroutine
from enum import Enum
from typing import (
    Any,
    Literal,
    NoReturn,
    TypeVar,
    cast,
)

from typing_extensions import assert_never

from cognite.client._constants import _RUNNING_IN_BROWSER
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils._auxiliary import no_op


class ConcurrencyConfig(ABC):
    """
    Abstract base class for concurrency settings.

    Args:
        concurrency_settings (ConcurrencySettings): Reference to the parent settings object, used to check if settings are frozen.
        api_name (str): Which API these settings apply to (e.g. "data_modeling", "datapoints", etc.).
        read (int): Maximum number of concurrent generic read requests.
        write (int): Maximum number of concurrent generic write requests.
        delete (int): Maximum number of concurrent generic delete requests.
    """

    def __init__(
        self,
        concurrency_settings: ConcurrencySettings,
        api_name: str,
        read: int,
        write: int,
        delete: int,
    ) -> None:
        self._check_frozen: Callable[[str], None] = functools.partial(
            concurrency_settings._check_frozen, api_name=api_name
        )
        self.api_name = api_name
        self._read = read
        self._write = write
        self._delete = delete
        self._semaphore_cache: dict[tuple[str, str, asyncio.AbstractEventLoop], asyncio.BoundedSemaphore] = {}

    @property
    def read(self) -> int:
        return self._read

    @read.setter
    def read(self, value: int) -> None:
        self._check_frozen("read")
        self._read = value

    @property
    def write(self) -> int:
        return self._write

    @write.setter
    def write(self, value: int) -> None:
        self._check_frozen("write")
        self._write = value

    @property
    def delete(self) -> int:
        return self._delete

    @delete.setter
    def delete(self, value: int) -> None:
        self._check_frozen("delete")
        self._delete = value

    @abstractmethod
    def _semaphore_factory(self, operation: Any, project: str) -> asyncio.BoundedSemaphore: ...

    @abstractmethod
    def __repr__(self) -> str: ...


class CRUDConcurrency(ConcurrencyConfig):
    """
    Basic concurrency settings, only differentiating on CRUD operation types.

    Args:
        concurrency_settings (ConcurrencySettings): Reference to the parent settings object, used to check if settings are frozen.
        api_name (str): Which API these settings apply to (e.g. "data_modeling", "datapoints", etc.).
        read (int): Maximum number of concurrent read requests (list, retrieve, search, etc.).
        write (int): Maximum number of concurrent write requests (create, update, upsert, etc.).
        delete (int): Maximum number of concurrent delete requests.
    """

    def _semaphore_factory(
        self, operation: Literal["read", "write", "delete"], project: str
    ) -> asyncio.BoundedSemaphore:
        # We include 'project' in the cache key, since concurrency limits should apply per-project.
        # We include the event loop because semaphores are bound to the loop they're first used on,
        # so the sync client (background loop) and async client (e.g. Jupyter's loop) need separate instances.
        key = (operation, project, asyncio.get_running_loop())
        if key in self._semaphore_cache:
            return self._semaphore_cache[key]

        from cognite.client import global_config

        global_config.concurrency_settings._freeze()  # Disallow any further changes

        match operation:
            case "read":
                sem = asyncio.BoundedSemaphore(self.read)
            case "write":
                sem = asyncio.BoundedSemaphore(self.write)
            case "delete":
                sem = asyncio.BoundedSemaphore(self.delete)
            case _:
                assert_never(operation)
        self._semaphore_cache[key] = sem
        return sem

    def __repr__(self) -> str:
        return f"Concurrency[{self.api_name}](read={self._read}, write={self._write}, delete={self._delete})"


class DataModelingConcurrencyConfig(ConcurrencyConfig):
    """
    Concurrency settings specific to the Data Modeling API, to differentiate e.g. schema operations from regular consumption of the API.
    Schema operations involve any call to views, data models and containers.

    Args:
        concurrency_settings (ConcurrencySettings): Reference to the parent settings object, used to check if settings are frozen.
        read (int): Maximum number of concurrent non-schema read requests. Mostly covers instance operations: query, retrieve, list,
            sync and inspect, but also graphQL instance queries.
        write (int): Maximum number of concurrent non-schema write requests, currently only instances -> apply.
        delete (int): Maximum number of concurrent non-schema delete requests, currently only instances -> delete.
        search (int): Maximum number of concurrent search and aggregation requests for instances.
        read_schema (int): Maximum number of concurrent schema read requests (views, data models, containers and spaces), as well as calls to statistics.
        write_schema (int): Maximum number of concurrent schema write requests (views, data models, containers and spaces).
    """

    def __init__(
        self,
        concurrency_settings: ConcurrencySettings,
        read: int,
        write: int,
        delete: int,
        search: int,
        read_schema: int,
        write_schema: int,
    ) -> None:
        super().__init__(concurrency_settings, "data_modeling", read, write, delete)
        self._search = search
        self._read_schema = read_schema
        self._write_schema = write_schema

    @property
    def search(self) -> int:
        return self._search

    @search.setter
    def search(self, value: int) -> None:
        self._check_frozen("search")
        self._search = value

    @property
    def read_schema(self) -> int:
        return self._read_schema

    @read_schema.setter
    def read_schema(self, value: int) -> None:
        self._check_frozen("read_schema")
        self._read_schema = value

    @property
    def write_schema(self) -> int:
        return self._write_schema

    @write_schema.setter
    def write_schema(self, value: int) -> None:
        self._check_frozen("write_schema")
        self._write_schema = value

    def _semaphore_factory(
        self, operation: Literal["read", "write", "delete", "search", "read_schema", "write_schema"], project: str
    ) -> asyncio.BoundedSemaphore:
        key = (operation, project, asyncio.get_running_loop())
        if key in self._semaphore_cache:
            return self._semaphore_cache[key]

        from cognite.client import global_config

        global_config.concurrency_settings._freeze()  # Disallow any further changes
        match operation:
            case "read":
                sem = asyncio.BoundedSemaphore(self.read)
            case "write":
                sem = asyncio.BoundedSemaphore(self.write)
            case "delete":
                sem = asyncio.BoundedSemaphore(self.delete)
            case "search":
                sem = asyncio.BoundedSemaphore(self.search)
            case "read_schema":
                sem = asyncio.BoundedSemaphore(self.read_schema)
            case "write_schema":
                sem = asyncio.BoundedSemaphore(self.write_schema)
            case _:
                assert_never(operation)
        self._semaphore_cache[key] = sem
        return sem

    def __repr__(self) -> str:
        return (
            f"Concurrency[{self.api_name}]("
            f"read={self._read}, write={self._write}, delete={self._delete}, "
            f"search={self._search}, read_schema={self._read_schema}, write_schema={self._write_schema})"
        )


class HierarchicalBoundedSemaphore:
    """Acquires multiple semaphores in order, releases in reverse.

    Used to model the Records API's hierarchical rate limits where an endpoint
    (e.g. retrieve) must pass both its dedicated budget and the shared query budget.

    If acquisition is interrupted (e.g. by cancellation), all already-acquired
    semaphores are released before the exception propagates. Similarly, if a
    release raises, the remaining semaphores are still released.
    """

    def __init__(self, *semaphores: asyncio.BoundedSemaphore) -> None:
        self._semaphores = semaphores

    async def __aenter__(self) -> None:
        acquired: list[asyncio.BoundedSemaphore] = []
        try:
            for sem in self._semaphores:
                await sem.__aenter__()
                acquired.append(sem)
        except BaseException:
            for sem in reversed(acquired):
                await sem.__aexit__(None, None, None)
            raise

    async def __aexit__(self, *exc: Any) -> None:
        first_err: BaseException | None = None
        for sem in reversed(self._semaphores):
            try:
                await sem.__aexit__(*exc)
            except BaseException as e:
                if first_err is None:
                    first_err = e
        if first_err is not None:
            raise first_err


class RecordsConcurrencyOperation(Enum):
    WRITE = "write"
    QUERY_MUTABLE = "query_mutable"
    QUERY_IMMUTABLE = "query_immutable"
    RETRIEVE_MUTABLE = "retrieve_mutable"
    RETRIEVE_IMMUTABLE = "retrieve_immutable"
    AGGREGATE_MUTABLE = "aggregate_mutable"
    AGGREGATE_IMMUTABLE = "aggregate_immutable"


class RecordsGlobalConcurrencyConfig(ConcurrencyConfig):
    """
    Global concurrency settings for the Records API.

    The Records API has separate rate-limit budgets for reads and writes, and read budgets
    differ between mutable and immutable streams. Read budgets are hierarchical: the
    retrieve and aggregate endpoints each have a dedicated budget that is checked *before*
    the shared query budget (both must pass).

    - **write**: Shared across ingest, upsert, and delete (same limit for both stream types).
    - **query_mutable / query_immutable**: Shared read budget consumed by all query endpoints
      (list/filter, sync, retrieve, aggregate).
    - **retrieve_mutable / retrieve_immutable**: Dedicated budget for retrieve, checked
      *in addition to* the shared query budget.
    - **aggregate_mutable / aggregate_immutable**: Dedicated budget for aggregate, checked
      *in addition to* the shared query budget.

    Args:
        concurrency_settings (ConcurrencySettings): Reference to the parent settings object.
        write (int): Maximum concurrent write requests (ingest, upsert, delete).
        query_mutable (int): Maximum concurrent query requests against mutable streams.
        query_immutable (int): Maximum concurrent query requests against immutable streams.
        retrieve_mutable (int): Dedicated retrieve concurrency for mutable streams.
        retrieve_immutable (int): Dedicated retrieve concurrency for immutable streams.
        aggregate_mutable (int): Dedicated aggregate concurrency for mutable streams.
        aggregate_immutable (int): Dedicated aggregate concurrency for immutable streams.
    """

    def __init__(
        self,
        concurrency_settings: ConcurrencySettings,
        write: int,
        query_mutable: int,
        query_immutable: int,
        retrieve_mutable: int,
        retrieve_immutable: int,
        aggregate_mutable: int,
        aggregate_immutable: int,
    ) -> None:
        super().__init__(concurrency_settings, "records", read=0, write=write, delete=0)
        self._query_mutable = query_mutable
        self._query_immutable = query_immutable
        self._retrieve_mutable = retrieve_mutable
        self._retrieve_immutable = retrieve_immutable
        self._aggregate_mutable = aggregate_mutable
        self._aggregate_immutable = aggregate_immutable
        self._validate_budgets()

    def _validate_budgets(self, **overrides: int) -> None:
        def resolve(name: str) -> int:
            return overrides.get(name, getattr(self, f"_{name}"))

        for dedicated_name, shared_name in [
            ("retrieve_mutable", "query_mutable"),
            ("retrieve_immutable", "query_immutable"),
            ("aggregate_mutable", "query_mutable"),
            ("aggregate_immutable", "query_immutable"),
        ]:
            dedicated = resolve(dedicated_name)
            shared = resolve(shared_name)
            if dedicated > shared:
                raise ValueError(
                    f"Dedicated budget must be <= shared query budget "
                    f"({dedicated_name} vs {shared_name}): {dedicated} > {shared}"
                )

    @property
    def query_mutable(self) -> int:
        return self._query_mutable

    @query_mutable.setter
    def query_mutable(self, value: int) -> None:
        self._check_frozen(RecordsConcurrencyOperation.QUERY_MUTABLE.value)
        self._validate_budgets(query_mutable=value)
        self._query_mutable = value

    @property
    def query_immutable(self) -> int:
        return self._query_immutable

    @query_immutable.setter
    def query_immutable(self, value: int) -> None:
        self._check_frozen(RecordsConcurrencyOperation.QUERY_IMMUTABLE.value)
        self._validate_budgets(query_immutable=value)
        self._query_immutable = value

    @property
    def retrieve_mutable(self) -> int:
        return self._retrieve_mutable

    @retrieve_mutable.setter
    def retrieve_mutable(self, value: int) -> None:
        self._check_frozen(RecordsConcurrencyOperation.RETRIEVE_MUTABLE.value)
        self._validate_budgets(retrieve_mutable=value)
        self._retrieve_mutable = value

    @property
    def retrieve_immutable(self) -> int:
        return self._retrieve_immutable

    @retrieve_immutable.setter
    def retrieve_immutable(self, value: int) -> None:
        self._check_frozen(RecordsConcurrencyOperation.RETRIEVE_IMMUTABLE.value)
        self._validate_budgets(retrieve_immutable=value)
        self._retrieve_immutable = value

    @property
    def aggregate_mutable(self) -> int:
        return self._aggregate_mutable

    @aggregate_mutable.setter
    def aggregate_mutable(self, value: int) -> None:
        self._check_frozen(RecordsConcurrencyOperation.AGGREGATE_MUTABLE.value)
        self._validate_budgets(aggregate_mutable=value)
        self._aggregate_mutable = value

    @property
    def aggregate_immutable(self) -> int:
        return self._aggregate_immutable

    @aggregate_immutable.setter
    def aggregate_immutable(self, value: int) -> None:
        self._check_frozen(RecordsConcurrencyOperation.AGGREGATE_IMMUTABLE.value)
        self._validate_budgets(aggregate_immutable=value)
        self._aggregate_immutable = value

    def _semaphore_factory(self, operation: RecordsConcurrencyOperation, project: str) -> asyncio.BoundedSemaphore:
        key = (operation.value, project, asyncio.get_running_loop())
        if key in self._semaphore_cache:
            return self._semaphore_cache[key]

        from cognite.client import global_config

        global_config.concurrency_settings._freeze()
        match operation:
            case RecordsConcurrencyOperation.WRITE:
                sem = asyncio.BoundedSemaphore(self._write)
            case RecordsConcurrencyOperation.QUERY_MUTABLE:
                sem = asyncio.BoundedSemaphore(self._query_mutable)
            case RecordsConcurrencyOperation.QUERY_IMMUTABLE:
                sem = asyncio.BoundedSemaphore(self._query_immutable)
            case RecordsConcurrencyOperation.RETRIEVE_MUTABLE:
                sem = asyncio.BoundedSemaphore(self._retrieve_mutable)
            case RecordsConcurrencyOperation.RETRIEVE_IMMUTABLE:
                sem = asyncio.BoundedSemaphore(self._retrieve_immutable)
            case RecordsConcurrencyOperation.AGGREGATE_MUTABLE:
                sem = asyncio.BoundedSemaphore(self._aggregate_mutable)
            case RecordsConcurrencyOperation.AGGREGATE_IMMUTABLE:
                sem = asyncio.BoundedSemaphore(self._aggregate_immutable)
            case _:
                assert_never(operation)
        self._semaphore_cache[key] = sem
        return sem

    def __repr__(self) -> str:
        return (
            f"Concurrency[records]("
            f"write={self._write}, "
            f"query_mutable={self._query_mutable}, query_immutable={self._query_immutable}, "
            f"retrieve_mutable={self._retrieve_mutable}, retrieve_immutable={self._retrieve_immutable}, "
            f"aggregate_mutable={self._aggregate_mutable}, aggregate_immutable={self._aggregate_immutable})"
        )


class FileConcurrencyConfig(ConcurrencyConfig):
    """
    Concurrency settings for the Files API.

    ``read``, ``write``, ``upload``, ``download``, and ``delete`` are **per-project** limits,
    consistent with all other API concurrency settings.

    ``open_files`` is the exception: it is a **global** (process-wide) limit on how many file
    handles may be open simultaneously during uploads, because OS file-descriptor limits are
    not scoped to a CDF project.

    Args:
        concurrency_settings (ConcurrencySettings): Reference to the parent settings object.
        read (int): Maximum concurrent metadata read requests (retrieve, list, search, etc.).
        write (int): Maximum concurrent metadata write requests (create, update).
        upload (int): Maximum concurrent file content upload requests.
        download (int): Maximum concurrent file content download requests.
        delete (int): Maximum concurrent delete requests.
        open_files (int): Global cap on simultaneously open file handles during upload (not per-project).
    """

    def __init__(
        self,
        concurrency_settings: ConcurrencySettings,
        read: int,
        write: int,
        upload: int,
        download: int,
        delete: int,
        open_files: int,
    ) -> None:
        super().__init__(concurrency_settings, "files", read, write, delete)
        self._upload = upload
        self._download = download
        self._open_files = open_files
        # open_files key is only the event loop — project is intentionally excluded. It is a bit unfortunate
        # that we can't remove the loop from the key as well, but semaphores are bound to the loop they are
        # first used on, one of the httpx.Clients would break if we did that. We intentionally use a limit << OS fd limit.
        self._open_files_cache: dict[asyncio.AbstractEventLoop, asyncio.BoundedSemaphore] = {}

    @property
    def upload(self) -> int:
        return self._upload

    @upload.setter
    def upload(self, value: int) -> None:
        self._check_frozen("upload")
        self._upload = value

    @property
    def download(self) -> int:
        return self._download

    @download.setter
    def download(self, value: int) -> None:
        self._check_frozen("download")
        self._download = value

    @property
    def open_files(self) -> int:
        return self._open_files

    @open_files.setter
    def open_files(self, value: int) -> None:
        self._check_frozen("open_files")
        self._open_files = value

    def _semaphore_factory(
        self, operation: Literal["read", "write", "upload", "download", "delete", "open_files"], project: str
    ) -> asyncio.BoundedSemaphore:
        # This one is special - global due to OS file descriptor limits::
        if operation == "open_files":
            return self._get_open_files_semaphore()

        key = (operation, project, asyncio.get_running_loop())
        if key in self._semaphore_cache:
            return self._semaphore_cache[key]

        from cognite.client import global_config

        global_config.concurrency_settings._freeze()
        match operation:
            case "read":
                sem = asyncio.BoundedSemaphore(self._read)
            case "write":
                sem = asyncio.BoundedSemaphore(self._write)
            case "upload":
                sem = asyncio.BoundedSemaphore(self._upload)
            case "download":
                sem = asyncio.BoundedSemaphore(self._download)
            case "delete":
                sem = asyncio.BoundedSemaphore(self._delete)
            case _:
                assert_never(operation)
        self._semaphore_cache[key] = sem
        return sem

    def _get_open_files_semaphore(self) -> asyncio.BoundedSemaphore:
        key = asyncio.get_running_loop()
        if key in self._open_files_cache:
            return self._open_files_cache[key]

        from cognite.client import global_config

        global_config.concurrency_settings._freeze()
        sem = asyncio.BoundedSemaphore(self._open_files)
        self._open_files_cache[key] = sem
        return sem

    def __repr__(self) -> str:
        return (
            f"Concurrency[files]("
            f"read={self._read}, write={self._write}, upload={self._upload}, download={self._download}, "
            f"delete={self._delete}, open_files={self._open_files})"
        )


class ConcurrencySettings:
    """
    Utility class for managing concurrency settings, controlled by semaphores.
    The total concurrency budget, i.e. the maximum number of concurrent requests in flight,
    is the sum of all categories (e.g. general) and operation types (e.g. read or write).

    See: https://cognite-sdk-python.readthedocs-hosted.com/en/latest/settings.html#concurrency-settings

    Note:
        Most settings apply on a per-project level, thus if you have multiple clients
        pointing to different CDF projects, each client will have its budget to consume from.
        The exception is ``files.open_files``, which is a global process-wide limit because
        OS file-descriptor limits are not scoped to a CDF project.

    Warning:
        Once any semaphore is initialized (i.e., after the first API request is made),
        all settings become frozen and cannot be changed. Attempting to modify frozen
        settings will raise a RuntimeError.
    """

    def __init__(self) -> None:
        self.__frozen = False
        # NOTE: DO NOT make changes here without also updating the 'Concurrency Settings' section
        # in 'docs/source/settings.rst'. That guide is the only way users can easily familiarize
        # themselves with the various concurrency settings that are available.

        # 'general' is a bit special - it is the default budget FOR ALL API endpoints that do not have
        # their own specific settings like e.g. many classical APIs (assets, events, files, etc.)
        self._general = CRUDConcurrency(self, "general", read=5, write=4, delete=2)

        # Specific APIs with their own concurrency settings, to allow more fine-grained control.
        # We use this when we want to allow higher levels of concurrency for high-throughput APIs (e.g. datapoints),
        # or the opposite; protect lower-throughput APIs (e.g. certain parts of data modeling):
        self._raw = CRUDConcurrency(self, "raw", read=5, write=2, delete=2)
        self._datapoints = CRUDConcurrency(self, "datapoints", read=5, write=5, delete=5)
        self._data_modeling = DataModelingConcurrencyConfig(
            self,
            read=1,
            write=1,
            delete=1,
            search=2,
            read_schema=2,
            write_schema=1,
        )
        self._files = FileConcurrencyConfig(self, read=4, write=2, upload=5, download=5, delete=2, open_files=15)
        self._records = RecordsGlobalConcurrencyConfig(
            self,
            write=20,
            query_mutable=30,
            query_immutable=10,
            retrieve_mutable=20,
            retrieve_immutable=10,
            aggregate_mutable=10,
            aggregate_immutable=5,
        )

    @functools.cached_property
    def _all_concurrency_configs(self) -> list[ConcurrencyConfig]:
        """Helper method primarily used in testing to handle the 'annoying' state of concurrency settings"""
        configs = [name for name, val in vars(type(self)).items() if isinstance(val, property)]
        configs.remove("is_frozen")
        return [getattr(self, name) for name in configs]

    def _check_frozen(self, name: str, api_name: str) -> None:
        if self.__frozen:
            raise RuntimeError(
                f"Cannot modify '{api_name}.{name}' after concurrency settings have been used to create semaphores. "
                "Concurrency settings must be configured before sending any API requests. "
                "See: https://cognite-sdk-python.readthedocs-hosted.com/en/latest/settings.html#concurrency-settings"
            )

    def _freeze(self) -> None:
        """Called internally when settings are consumed to create semaphores."""
        self.__frozen = True

    @property
    def is_frozen(self) -> bool:
        """Returns True if settings have been frozen (at least one semaphore has been created and used)."""
        return self.__frozen

    @property
    def general(self) -> CRUDConcurrency:
        return self._general

    @property
    def datapoints(self) -> CRUDConcurrency:
        return self._datapoints

    @property
    def data_modeling(self) -> DataModelingConcurrencyConfig:
        return self._data_modeling

    @property
    def raw(self) -> CRUDConcurrency:
        return self._raw

    @property
    def files(self) -> FileConcurrencyConfig:
        return self._files

    @property
    def records(self) -> RecordsGlobalConcurrencyConfig:
        return self._records

    def __repr__(self) -> str:
        frozen_str = " (frozen)" if self.__frozen else ""
        return (
            f"ConcurrencySettings(\n"
            f"  general={self._general},\n"
            f"  raw={self._raw},\n"
            f"  datapoints={self._datapoints},\n"
            f"  data_modeling={self._data_modeling},\n"
            f"  files={self._files},\n"
            f"  records={self._records},\n"
            f"){frozen_str}"
        )


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
    def __init__(self, daemon: bool = True) -> None:
        super().__init__(name=type(self).__name__, daemon=daemon)
        self._event_loop = asyncio.new_event_loop()

    def run(self) -> None:
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.run_forever()

    def stop(self) -> None:
        self._event_loop.call_soon_threadsafe(self._event_loop.stop)
        self.join()

    def run_coro(self, coro: Coroutine[Any, Any, _T], timeout: float | None = None) -> _T:
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
_INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON: EventLoopThreadExecutor | None = None
_EXECUTOR_INIT_LOCK = threading.Lock()


def _get_event_loop_executor() -> EventLoopThreadExecutor:
    global _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON

    # Fast path: singleton already initialized — no lock needed.
    if _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON is not None:
        return _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON

    # Slow path: serialize initialization. Without this, multiple threads racing on the first
    # call (e.g. a ThreadPoolExecutor of sync clients) can each construct their own executor
    # and end up using different background loops, breaking the per-loop semaphore cache key.
    with _EXECUTOR_INIT_LOCK:
        if _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON is None:
            ex_cls = EventLoopThreadExecutor
            if _RUNNING_IN_BROWSER:
                ex_cls = cast(type[EventLoopThreadExecutor], _PyodideEventLoopExecutor)
            _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON = ex_cls()
            _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON.start()
        return _INTERNAL_EVENT_LOOP_THREAD_EXECUTOR_SINGLETON


async def execute_async_tasks_with_fail_fast(tasks: list[AsyncSDKTask]) -> TasksSummary:
    # If no future raises an exception then this is equivalent to asyncio.ALL_COMPLETED:
    done, pending = await asyncio.wait(
        [task.schedule() for task in tasks],
        return_when=asyncio.FIRST_EXCEPTION,
    )
    if all(task.exception() is None for task in done):
        return TasksSummary([task.result() for task in tasks], successful_tasks=tasks)

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
