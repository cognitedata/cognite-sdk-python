from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

import cognite.client.data_classes.postgres_gateway.tables as pg
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.utils._auxiliary import interpolate_and_url_encode
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import TablenameSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class TablesAPI(APIClient):
    _RESOURCE_PATH = "/postgresgateway/tables/{}"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Postgres Gateway: Tables"
        )
        self._CREATE_LIMIT = 10
        self._DELETE_LIMIT = 10
        self._LIST_LIMIT = 100
        self._RETRIEVE_LIMIT = 10

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
    ) -> Iterator[pg.Table]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[pg.TableList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[pg.Table] | Iterator[pg.TableList]:
        """Iterate over custom tables

        Fetches custom tables as they are iterated over, so you keep a limited number of custom tables in memory.

        Args:
            chunk_size (int | None): Number of custom tables to return in each chunk. Defaults to yielding one custom table at a time.
            limit (int | None): Maximum number of custom tables to return. Defaults to return all.

        Returns:
            Iterator[pg.Table] | Iterator[pg.TableList]: yields Table one by one if chunk_size is not specified, else TableList objects.
        """
        self._warning.warn()

        return self._list_generator(
            list_cls=pg.TableList,
            resource_cls=pg.Table,  # type: ignore[type-abstract]
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def __iter__(self) -> Iterator[pg.Table]:
        """Iterate over custom tables

        Fetches custom tables as they are iterated over, so you keep a
        limited number of custom tables in memory.

        Returns:
            Iterator[pg.Table]: yields custom table one by one.
        """
        return self()

    @overload
    def create(self, username: str, items: pg.TableWrite) -> pg.Table: ...

    @overload
    def create(self, username: str, items: Sequence[pg.TableWrite]) -> pg.TableList: ...

    def create(self, username: str, items: pg.TableWrite | Sequence[pg.TableWrite]) -> pg.Table | pg.TableList:
        """`Create tables <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/create_tables>`_

        Args:
            username (str): The name of the username (a.k.a. database) to be managed from the API
            items (pg.TableWrite | Sequence[pg.TableWrite]): The table(s) to create

        Returns:
            pg.Table | pg.TableList: Created tables

        Examples:

            Create custom table:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes.postgres_gateway import ViewTableWrite
                >>> client = CogniteClient()
                >>> table = ViewTableWrite(tablename="myCustom", options=ViewId(space="mySpace", external_id="myExternalId", version="v1"))
                >>> res = client.postgres_gateway.tables.create("myUserName",table)

        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=pg.TableList,
            resource_cls=pg.Table,  # type: ignore[type-abstract]
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            items=items,  # type: ignore[arg-type]
            input_resource_cls=pg.TableWrite,
            headers={"cdf-version": "beta"},
        )

    @overload
    def retrieve(self, username: str, tablename: str, ignore_unknown_ids: Literal[False] = False) -> pg.Table: ...

    @overload
    def retrieve(self, username: str, tablename: str, ignore_unknown_ids: Literal[True]) -> pg.Table | None: ...

    @overload
    def retrieve(
        self, username: str, tablename: SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> pg.TableList: ...

    def retrieve(
        self, username: str, tablename: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> pg.Table | pg.TableList | None:
        """`Retrieve a list of tables by their tables names <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/retrieve_tables>`_

        Retrieve a list of Postgres tables for a user by their table names, optionally ignoring unknown table names

        Args:
            username (str): The username (a.k.a. database) to be managed from the API
            tablename (str | SequenceNotStr[str]): The name of the table(s) to be retrieved
            ignore_unknown_ids (bool): Ignore table names not found

        Returns:
            pg.Table | pg.TableList | None: Foreign tables

        Examples:

            Retrieve  custom table:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.postgres_gateway.tables.retrieve("myUserName", 'myCustom')

            Get multiple custom tables by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.postgres_gateway.tables.retrieve("myUserName", ["myCustom", "myCustom2"])


        """
        self._warning.warn()

        return self._retrieve_multiple(
            list_cls=pg.TableList,
            resource_cls=pg.Table,  # type: ignore[type-abstract]
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            other_params={"ignoreUnknownIds": ignore_unknown_ids},
            identifiers=TablenameSequence.load(tablenames=tablename),
            headers={"cdf-version": "beta"},
        )

    def delete(self, tablename: str | SequenceNotStr[str], username: str, ignore_unknown_ids: bool = False) -> None:
        """`Delete postgres table(s) <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/delete_tables>`_

        Args:
            tablename (str | SequenceNotStr[str]): The name of the table(s) to be deleted
            username (str): The name of the username (a.k.a. database) to be managed from the API
            ignore_unknown_ids (bool): Ignore table names that are not found

        Examples:

            Delete custom table:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.postgres_gateway.tables.delete(["myCustom", "myCustom2"], username="myUserName")


        """
        self._warning.warn()

        self._delete_multiple(
            identifiers=TablenameSequence.load(tablenames=tablename),
            wrap_ids=True,
            returns_items=False,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
            headers={"cdf-version": "beta"},
        )

    def list(
        self,
        username: str,
        include_built_ins: Literal["yes", "no"] | None = "no",
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> pg.TableList:
        """`List postgres tables <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/list_tables>`_

        List all tables in a given project.

        Args:
            username (str): The name of the username (a.k.a. database) to be managed from the API
            include_built_ins (Literal['yes', 'no'] | None): Determines if API should return built-in tables or not
            limit (int | None): Limits the number of results to be returned.

        Returns:
            pg.TableList: Foreign tables

        Examples:

            List tables:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> custom_table_list = client.postgres_gateway.tables.list("myUserName", limit=5)

            Iterate over tables:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for table in client.postgres_gateway.tables:
                ...     table # do something with the custom table

            Iterate over chunks of tables to reduce memory load:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for table_list in client.postgres_gateway.tables(chunk_size=25):
                ...     table_list # do something with the custom tables

        """
        self._warning.warn()

        return self._list(
            list_cls=pg.TableList,
            resource_cls=pg.Table,  # type: ignore[type-abstract]
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            filter={"includeBuiltIns": include_built_ins},
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
