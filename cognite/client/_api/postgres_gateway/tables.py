from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

import cognite.client.data_classes.postgres_gateway.tables as pg
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.utils._identifier import TablenameSequence
from cognite.client.utils._url import interpolate_and_url_encode
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient, ClientConfig


class TablesAPI(APIClient):
    _RESOURCE_PATH = "/postgresgateway/tables/{}"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 10
        self._DELETE_LIMIT = 10
        self._LIST_LIMIT = 100
        self._RETRIEVE_LIMIT = 10

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> AsyncIterator[pg.Table]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> AsyncIterator[pg.TableList]: ...

    async def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> AsyncIterator[pg.Table | pg.TableList]:
        """Iterate over custom tables

        Fetches custom tables as they are iterated over, so you keep a limited number of custom tables in memory.

        Args:
            chunk_size: Number of custom tables to return in each chunk. Defaults to yielding one custom table at a time.
            limit: Maximum number of custom tables to return. Defaults to return all.

        Yields:
            yields Table one by one if chunk_size is not specified, else TableList objects.
        """
        async for item in self._list_generator(  # type: ignore [call-overload]
            list_cls=pg.TableList,
            resource_cls=pg.Table,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
        ):
            yield item

    @overload
    async def create(self, username: str, items: pg.TableWrite) -> pg.Table: ...

    @overload
    async def create(self, username: str, items: Sequence[pg.TableWrite]) -> pg.TableList: ...

    async def create(self, username: str, items: pg.TableWrite | Sequence[pg.TableWrite]) -> pg.Table | pg.TableList:
        """`Create tables <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/create_tables>`_

        Args:
            username: The name of the username (a.k.a. database) to be managed from the API
            items: The table(s) to create

        Returns:
            Created tables

        Examples:

            Create custom table:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes.postgres_gateway import ViewTableWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> table = ViewTableWrite(tablename="myCustom", options=ViewId(space="mySpace", external_id="myExternalId", version="v1"))
                >>> res = client.postgres_gateway.tables.create("myUserName",table)

        """
        return await self._create_multiple(
            list_cls=pg.TableList,
            resource_cls=pg.Table,  # type: ignore[type-abstract]
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            items=items,  # type: ignore[arg-type]
            input_resource_cls=pg.TableWrite,
        )

    @overload
    async def retrieve(self, username: str, tablename: str, ignore_unknown_ids: Literal[False] = False) -> pg.Table: ...

    @overload
    async def retrieve(self, username: str, tablename: str, ignore_unknown_ids: Literal[True]) -> pg.Table | None: ...

    @overload
    async def retrieve(
        self, username: str, tablename: SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> pg.TableList: ...

    async def retrieve(
        self, username: str, tablename: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> pg.Table | pg.TableList | None:
        """`Retrieve a list of tables by their tables names <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/retrieve_tables>`_

        Retrieve a list of Postgres tables for a user by their table names, optionally ignoring unknown table names

        Args:
            username: The username (a.k.a. database) to be managed from the API
            tablename: The name of the table(s) to be retrieved
            ignore_unknown_ids: Ignore table names not found

        Returns:
            Foreign tables

        Examples:

            Retrieve  custom table:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.postgres_gateway.tables.retrieve("myUserName", 'myCustom')

            Get multiple custom tables by id:

                >>> res = client.postgres_gateway.tables.retrieve("myUserName", ["myCustom", "myCustom2"])

        """
        return await self._retrieve_multiple(
            list_cls=pg.TableList,
            resource_cls=pg.Table,  # type: ignore[type-abstract]
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            other_params={"ignoreUnknownIds": ignore_unknown_ids},
            identifiers=TablenameSequence.load(tablenames=tablename),
        )

    async def delete(
        self, username: str, tablename: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> None:
        """`Delete postgres table(s) <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/delete_tables>`_

        Args:
            username: The name of the username (a.k.a. database) to be managed from the API
            tablename: The name of the table(s) to be deleted
            ignore_unknown_ids: Ignore table names that are not found

        Examples:

            Delete custom table:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.postgres_gateway.tables.delete("myUserName", ["myCustom", "myCustom2"])


        """
        await self._delete_multiple(
            identifiers=TablenameSequence.load(tablenames=tablename),
            wrap_ids=True,
            returns_items=False,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    async def list(
        self,
        username: str,
        include_built_ins: Literal["yes", "no"] | None = "no",
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> pg.TableList:
        """`List postgres tables <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/list_tables>`_

        List all tables in a given project.

        Args:
            username: The name of the username (a.k.a. database) to be managed from the API
            include_built_ins: Determines if API should return built-in tables or not
            limit: Limits the number of results to be returned.

        Returns:
            Foreign tables

        Examples:

            List tables:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> custom_table_list = client.postgres_gateway.tables.list("myUserName", limit=5)

            Iterate over tables, one-by-one:

                >>> for table in client.postgres_gateway.tables():
                ...     table  # do something with the custom table

            Iterate over chunks of tables to reduce memory load:

                >>> for table_list in client.postgres_gateway.tables(chunk_size=25):
                ...     table_list # do something with the custom tables

        """
        return await self._list(
            list_cls=pg.TableList,
            resource_cls=pg.Table,  # type: ignore[type-abstract]
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            filter={"includeBuiltIns": include_built_ins},
            method="GET",
            limit=limit,
        )
