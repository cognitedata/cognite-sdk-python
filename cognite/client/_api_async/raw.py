from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Database,
    DatabaseList,
    Row,
    RowList,
    Table,
    TableList,
)
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncRawAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/raw"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.databases = AsyncRawDatabasesAPI(self._config, self._api_version, self._cognite_client)
        self.tables = AsyncRawTablesAPI(self._config, self._api_version, self._cognite_client)
        self.rows = AsyncRawRowsAPI(self._config, self._api_version, self._cognite_client)


class AsyncRawDatabasesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/raw/dbs"

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> DatabaseList:
        """`List databases in raw. <https://developer.cognite.com/api#tag/Raw/operation/getDBs>`_"""
        return await self._list(
            list_cls=DatabaseList,
            resource_cls=Database,
            method="GET",
            limit=limit,
        )

    async def create(self, name: str | Database | Sequence[str] | Sequence[Database]) -> Database | DatabaseList:
        """`Create one or more databases in raw. <https://developer.cognite.com/api#tag/Raw/operation/createDBs>`_"""
        items = [{"name": name} if isinstance(name, str) else name.dump() if hasattr(name, 'dump') else name for name in ([name] if not isinstance(name, Sequence) or isinstance(name, str) else name)]
        return await self._create_multiple(
            list_cls=DatabaseList,
            resource_cls=Database,
            items=items,
        )

    async def delete(self, name: str | Sequence[str], recursive: bool = False) -> None:
        """`Delete one or more databases in raw. <https://developer.cognite.com/api#tag/Raw/operation/deleteDBs>`_"""
        names = [name] if isinstance(name, str) else list(name)
        items = [{"name": n} for n in names]
        await self._delete_multiple(
            identifiers=items,
            wrap_ids=False,
            extra_body_fields={"recursive": recursive},
        )


class AsyncRawTablesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/raw/dbs"

    async def list(self, db_name: str, limit: int | None = DEFAULT_LIMIT_READ) -> TableList:
        """`List tables in a database. <https://developer.cognite.com/api#tag/Raw/operation/getTables>`_"""
        return await self._list(
            list_cls=TableList,
            resource_cls=Table,
            method="GET",
            resource_path=f"{self._RESOURCE_PATH}/{db_name}/tables",
            limit=limit,
        )

    async def create(self, db_name: str, name: str | Table | Sequence[str] | Sequence[Table]) -> Table | TableList:
        """`Create one or more tables in a database. <https://developer.cognite.com/api#tag/Raw/operation/createTables>`_"""
        items = [{"name": name} if isinstance(name, str) else name.dump() if hasattr(name, 'dump') else name for name in ([name] if not isinstance(name, Sequence) or isinstance(name, str) else name)]
        return await self._create_multiple(
            list_cls=TableList,
            resource_cls=Table,
            items=items,
            resource_path=f"{self._RESOURCE_PATH}/{db_name}/tables",
        )

    async def delete(self, db_name: str, name: str | Sequence[str]) -> None:
        """`Delete one or more tables in a database. <https://developer.cognite.com/api#tag/Raw/operation/deleteTables>`_"""
        names = [name] if isinstance(name, str) else list(name)
        items = [{"name": n} for n in names]
        await self._delete_multiple(
            identifiers=items,
            wrap_ids=False,
            resource_path=f"{self._RESOURCE_PATH}/{db_name}/tables",
        )


class AsyncRawRowsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/raw/dbs"

    async def list(
        self, 
        db_name: str, 
        table_name: str, 
        limit: int | None = DEFAULT_LIMIT_READ,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        columns: Sequence[str] | None = None,
    ) -> RowList:
        """`List rows in a table. <https://developer.cognite.com/api#tag/Raw/operation/getRows>`_"""
        params = {}
        if min_last_updated_time is not None:
            params["minLastUpdatedTime"] = min_last_updated_time
        if max_last_updated_time is not None:
            params["maxLastUpdatedTime"] = max_last_updated_time
        if columns is not None:
            params["columns"] = ",".join(columns)

        return await self._list(
            list_cls=RowList,
            resource_cls=Row,
            method="GET",
            resource_path=f"{self._RESOURCE_PATH}/{db_name}/tables/{table_name}/rows",
            limit=limit,
            other_params=params,
        )

    async def insert(
        self, 
        db_name: str, 
        table_name: str, 
        row: Row | dict | Sequence[Row] | Sequence[dict],
        ensure_parent: bool = False
    ) -> None:
        """`Insert one or more rows into a table. <https://developer.cognite.com/api#tag/Raw/operation/createRows>`_"""
        items = [row] if not isinstance(row, Sequence) else row
        items = [r.dump() if hasattr(r, 'dump') else r for r in items]
        
        await self._post(
            url_path=f"{self._RESOURCE_PATH}/{db_name}/tables/{table_name}/rows",
            json={"items": items, "ensureParent": ensure_parent}
        )

    async def delete(
        self, 
        db_name: str, 
        table_name: str, 
        key: str | Sequence[str]
    ) -> None:
        """`Delete one or more rows from a table. <https://developer.cognite.com/api#tag/Raw/operation/deleteRows>`_"""
        keys = [key] if isinstance(key, str) else list(key)
        items = [{"key": k} for k in keys]
        
        await self._post(
            url_path=f"{self._RESOURCE_PATH}/{db_name}/tables/{table_name}/rows/delete",
            json={"items": items}
        )

    async def retrieve(self, db_name: str, table_name: str, key: str) -> Row | None:
        """`Retrieve a single row from a table. <https://developer.cognite.com/api#tag/Raw/operation/getRow>`_"""
        try:
            res = await self._get(url_path=f"{self._RESOURCE_PATH}/{db_name}/tables/{table_name}/rows/{key}")
            return Row._load(res.json(), cognite_client=self._cognite_client)
        except Exception:
            return None