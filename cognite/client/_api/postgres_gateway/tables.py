from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.postgres_gateway.tables import Table, TableList, TableWrite
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
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Tables")
        self._CREATE_LIMIT = 10
        self._DELETE_LIMIT = 10
        self._LIST_LIMIT = 100
        self._RETRIEVE_LIMIT = 10

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
    ) -> Iterator[Table]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[TableList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[Table] | Iterator[TableList]:
        """Iterate over custom tables

        Fetches custom table as they are iterated over, so you keep a limited number of custom tables in memory.

        Args:
            chunk_size (int | None): Number of custom tables to return in each chunk. Defaults to yielding one custom table a time.
            limit (int | None): Maximum number of custom tables to return. Defaults to return all.

        Returns:
            Iterator[Table] | Iterator[:py:class:`cognite.client.data_classes.postgres_gateway.TableList`]: yields CustomTable one by one if chunk_size is not specified, else CustomTableList objects.
        """
        self._warning.warn()

        return self._list_generator(
            list_cls=TableList,
            resource_cls=Table,  # type: ignore[type-abstract]
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def __iter__(self) -> Iterator[Table]:
        """Iterate over custom tables

        Fetches custom tables as they are iterated over, so you keep a
        limited number of custom tables in memory.

        Returns:
            Iterator[Table]: yields custom table one by one.
        """
        return self()

    @overload
    def create(self, items: TableWrite, username: str) -> Table: ...

    @overload
    def create(self, items: Sequence[TableWrite], username: str) -> TableList: ...

    def create(self, items: TableWrite | Sequence[TableWrite], username: str) -> Table | TableList:
        """`Create tables <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/create_tables>`_

        Args:
            items (:py:class:`cognite.client.data_classes.postgres_gateway.TableWrite` | Sequence[:py:class:`cognite.client.data_classes.postgres_gateway.TableWrite`]): The table(s) to create
            username (str): The name of the username (a.k.a. database) to be managed from the API

        Returns:
            Table | :py:class:`cognite.client.data_classes.postgres_gateway.TableList`: Created tables

        Examples:

            Create custom table:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes.postgres_gateway import ViewTableWrite
                >>> client = CogniteClient()
                >>> table = ViewTableWrite(tablename="myCustom", options=ViewId(space="mySpace", external_id="myExternalId", version="v1"))
                >>> res = client.postgres_gateway.tables.create(table, "myUserName")

        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=TableList,
            resource_cls=Table,  # type: ignore[type-abstract]
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            items=items,  # type: ignore[arg-type]
            input_resource_cls=TableWrite,
            headers={"cdf-version": "beta"},
        )

    @overload
    def retrieve(self, tablename: str, username: str, ignore_unknown_ids: Literal[False] = False) -> Table: ...

    @overload
    def retrieve(self, tablename: str, username: str, ignore_unknown_ids: Literal[True]) -> Table | None: ...

    @overload
    def retrieve(
        self, tablename: SequenceNotStr[str], username: str, ignore_unknown_ids: bool = False
    ) -> TableList: ...

    def retrieve(
        self, tablename: str | SequenceNotStr[str], username: str, ignore_unknown_ids: bool = False
    ) -> Table | TableList | None:
        """`Retrieve a list of tables by their tables names <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/retrieve_tables>`_

        Retreive a list of postgres tables for a user by their table names, optionally ignoring unknown table names

        Args:
            tablename (str | SequenceNotStr[str]): The name of the table(s) to be retrieved
            username (str): The name of the username (a.k.a. database) to be managed from the API
            ignore_unknown_ids (bool): Ignore table names not found

        Returns:
            Table | :py:class:`cognite.client.data_classes.postgres_gateway.TableList` | None: Foreign tables

        Examples:

            Retrieve  custom table:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.postgres_gateway.tables.retrieve('myCustom', username="myUserName")

            Get multiple custom tables by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.postgres_gateway.tables.retrieve(["myCustom", "myCustom2"], username="myUserName")


        """
        self._warning.warn()

        return self._retrieve_multiple(
            list_cls=TableList,
            resource_cls=Table,  # type: ignore[type-abstract]
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
        extra_body_fields = {"ignoreUnknownIds": ignore_unknown_ids}

        self._delete_multiple(
            identifiers=TablenameSequence.load(tablenames=tablename),
            wrap_ids=True,
            returns_items=False,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            extra_body_fields=extra_body_fields,
            headers={"cdf-version": "beta"},
        )

    def list(
        self,
        username: str,
        include_built_ins: Literal["yes", "no"] | None = "no",
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TableList:
        """`List postgres tables <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Tables/operation/list_tables>`_

        List all tables in a given project.

        Args:
            username (str): The name of the username (a.k.a. database) to be managed from the API
            include_built_ins (Literal['yes', 'no'] | None): Determines if API should return built-in tables or not
            limit (int | None): Limits the number of results to be returned.

        Returns:
            :py:class:`cognite.client.data_classes.postgres_gateway.TableList`: Foreign tables

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
        filter_ = {"includeBuiltIns": include_built_ins}

        return self._list(
            list_cls=TableList,
            resource_cls=Table,  # type: ignore[type-abstract]
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, username),
            filter=filter_,
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
