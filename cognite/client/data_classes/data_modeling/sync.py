from __future__ import annotations

import hashlib
import logging
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, NoReturn

from cognite.client._version import __version__ as sdk_version
from cognite.client.data_classes.data_modeling.instances import EdgeList, NodeList
from cognite.client.data_classes.data_modeling.query import QueryResult, QuerySync
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils import _json_extended as json

if TYPE_CHECKING:
    import asyncio

    from cognite.client._api.data_modeling.instances import InstancesAPI

logger = logging.getLogger(__name__)


@dataclass
class SubscriptionContext:
    """Manages an active subscription to an instance sync query."""

    last_successful_sync: datetime | None = None
    last_successful_callback: datetime | None = None
    _task: asyncio.Task | None = None

    def cancel(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def is_alive(self) -> bool:
        return self.is_running()


class SyncSessionWithCache(AbstractAsyncContextManager["SyncSessionWithCache"]):
    """Data modeling /sync session with persistent backup to a CDF Data Modeling file.

    Obtain via :meth:`InstancesAPI.sync_with_file_cache`. Use as an async context manager.

    On entry the session downloads the backup file from CDF and restores the instance data and
    previous cursor positions if the query hash matches, allowing you to immediately continue
    syncing instances from where your last session left off (no need for a full backfill).
    """

    def __init__(
        self,
        api: InstancesAPI,
        query: QuerySync,
        *,
        file_external_id: str,
        security_category: int,
        backup_every: timedelta | None,
        backup_on_exit: bool,
    ) -> None:
        self._api = api
        self._query = query
        self._file_external_id = file_external_id
        self._security_category = security_category
        self._backup_every = backup_every
        self._backup_on_exit = backup_on_exit

        # Populated on enter:
        self._query_hash: str
        self._last_backup_time: datetime

        # Runtime state
        self._cursors: dict[str, str] = {}
        self._instances: dict[str, list[dict[str, Any]]] = {}
        self._has_changes_since_backup: bool = False
        self._entered: bool = False
        # When the user is running with sync_mode="two_phase" (default), the backfill phase may be done
        # (we don't know), so we'll fetch two consecutive batches with "less than limit" before we consider
        # it done (just to be sure):
        self._backfill_done: bool = False

    async def __aenter__(self) -> SyncSessionWithCache:
        self._query_hash = self._compute_query_hash(self._query)
        await self._load_from_cdf()
        if self._cursors:
            self._query.cursors = self._cursors
        self._last_backup_time = datetime.now(timezone.utc)
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._entered and self._has_changes_since_backup and self._backup_on_exit:
            await self._backup_to_cdf()

    def __enter__(self) -> NoReturn:
        raise NotImplementedError(
            f"{type(self).__name__} must be used as an async context manager (for now). Hint: `async with session:`"
        )

    def __exit__(self, *_: object) -> NoReturn:
        raise NotImplementedError(
            f"{type(self).__name__} must be used as an async context manager (for now). Hint: `async with session:`"
        )

    async def sync(self) -> None:
        """Perform one incremental sync, updating the internal snapshot state."""
        await self._sync_one()

    async def sync_until_live(self) -> None:
        """Call :meth:`sync` repeatedly until we have caught up with all live changes."""
        n_consecutive_small_batches = 0  # small as in "less than limit"
        required = 1 if self._backfill_done else 2
        while True:
            if self._batch_is_full(await self._sync_one()):
                n_consecutive_small_batches = 0
                continue

            n_consecutive_small_batches += 1
            if n_consecutive_small_batches >= required:
                self._backfill_done = True
                break

    def get_nodes(self, key: str) -> NodeList:
        """Return the current in-memory snapshot of nodes for the given result-expression key.

        Args:
            key (str): A key from ``query.select`` that maps to a node result-set expression.

        Returns:
            NodeList: The current in-memory snapshot for that key.

        Raises:
            KeyError: If *key* is not found.
            ValueError: If the data stored under *key* contains edges, not nodes.
        """
        items = self._instances[key]
        if items and items[0].get("instanceType") == "edge":
            raise ValueError(f"Key {key!r} contains edges, not nodes. Use get_edges() instead.")
        return NodeList._load(items)

    def get_edges(self, key: str) -> EdgeList:
        """Return the current in-memory snapshot of edges for the given result-expression key.

        Args:
            key (str): A key from ``query.select`` that maps to an edge result-set expression.

        Returns:
            EdgeList: The current in-memory snapshot for that key.

        Raises:
            KeyError: If *key* is not found.
            ValueError: If the data stored under *key* contains nodes, not edges.
        """
        items = self._instances[key]
        if items and items[0].get("instanceType") != "edge":
            raise ValueError(f"Key {key!r} contains nodes, not edges. Use get_nodes() instead.")
        return EdgeList._load(items)

    async def invalidate(self) -> None:
        """Clear all in-memory state and immediately overwrite the CDF backup file."""
        self._cursors = {}
        self._query.cursors = {}
        self._instances = {}
        self._has_changes_since_backup = True
        await self._backup_to_cdf()

    def _batch_is_full(self, result: QueryResult) -> bool:
        # We have enforcement on limit being given (although mypy doesn't know that):
        return any(len(result[k]) >= self._query.with_[k].limit for k in result)  # type: ignore[operator]

    async def _sync_one(self) -> QueryResult:
        if self._cursors:
            self._query.cursors = self._cursors

        sync_result = await self._api.sync(self._query)
        self._merge_result(sync_result)
        self._cursors = sync_result.cursors
        self._has_changes_since_backup = True  # TODO: simple but could be wrong, not that big deal

        if self._backup_every is not None and self._last_backup_time is not None:
            if datetime.now(timezone.utc) - self._last_backup_time >= self._backup_every:
                await self._backup_to_cdf()
                self._last_backup_time = datetime.now(timezone.utc)
                self._has_changes_since_backup = False

        return sync_result

    def _merge_result(self, sync_result: QueryResult) -> None:
        for key, sync_list in sync_result.items():
            cached_by_id: dict[tuple[str, str], dict[str, Any]] = {
                (item["space"], item["externalId"]): item for item in self._instances.get(key, [])
            }
            # Deletes first: an instance may be deleted and re-created in the same batch,
            # so we must not let a delete that appears after a re-create win.
            for item in sync_list:
                if item.deleted_time is not None:
                    cached_by_id.pop((item.space, item.external_id), None)
            for item in sync_list:
                if item.deleted_time is None:
                    cached_by_id[(item.space, item.external_id)] = item.dump(camel_case=True)
            self._instances[key] = list(cached_by_id.values())

    async def _load_from_cdf(self) -> None:
        try:
            cached_bytes = await self._api._cognite_client.files.download_bytes(external_id=self._file_external_id)
        except CogniteNotFoundError:
            logger.info(f"No existing cache file {self._file_external_id!r}, will create on first backup")
            return

        cached_data = json.loads(cached_bytes.decode("utf-8"))
        if cached_data.get("hash") != self._query_hash:
            logger.warning(f"Cache hash mismatch for {self._file_external_id!r}, starting fresh")
            return

        self._instances = cached_data["instances"]
        self._cursors = cached_data["cursors"]
        n_instances = sum(len(v) for v in self._instances.values())
        logger.info(f"Restored cache state from CDF file {self._file_external_id!r} ({n_instances:,} instances)")

    async def _backup_to_cdf(self) -> None:
        try:
            cache_data: dict[str, Any] = {
                "hash": self._query_hash,
                "cursors": self._cursors,
                "instances": self._instances,
                # We add the SDK version in case we ever need to evolve (break) the format of the cache file,
                # then we can reason based on current- vs. cached SDK version:
                "sdk-version": sdk_version,
            }
            await self._api._cognite_client.files.upload_bytes(
                content=json.dumps(cache_data).encode("utf-8"),
                name=self._file_external_id,
                external_id=self._file_external_id,
                mime_type="application/json",
                security_categories=[self._security_category],
                overwrite=True,
            )
            logger.info(f"Backed up cache to CDF file {self._file_external_id!r}")
        except CogniteAPIError as e:
            logger.warning(f"Failed to back up to CDF Files ({self._file_external_id!r}): {e}. In-memory state intact.")

    @staticmethod
    def _compute_query_hash(query: QuerySync) -> str:
        query_dict = query.dump(camel_case=True)
        query_dict.pop("cursors", None)
        serialized = json.dumps_deterministic(query_dict)
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]
