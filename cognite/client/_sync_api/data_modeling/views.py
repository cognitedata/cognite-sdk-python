"""
===============================================================================
ccc30965f9b4fb2373f005996eb78293
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DATA_MODELING_DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.ids import ViewId, ViewIdentifier
from cognite.client.data_classes.data_modeling.views import View, ViewApply, ViewList
from cognite.client.utils._async_helpers import SyncIterator, run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncViewsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[View]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[ViewList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[View] | Iterator[ViewList]:
        """
        Iterate over views

        Fetches views as they are iterated over, so you keep a limited number of views in memory.

        Args:
            chunk_size: Number of views to return in each chunk. Defaults to yielding one view at a time.
            limit: Maximum number of views to return. Defaults to returning all items.
            space: The space to query.
            include_inherited_properties: Whether to include properties inherited from views this view implements.
            all_versions: Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
            include_global: Whether to include global views.

        Yields:
            yields View one by one if chunk_size is not specified, else ViewList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.data_modeling.views(
                chunk_size=chunk_size,
                limit=limit,
                space=space,
                include_inherited_properties=include_inherited_properties,
                all_versions=all_versions,
                include_global=include_global,
            )
        )  # type: ignore [misc]

    def retrieve(
        self,
        ids: ViewIdentifier | Sequence[ViewIdentifier],
        include_inherited_properties: bool = True,
        all_versions: bool = True,
    ) -> ViewList:
        """
        `Retrieve a single view by id. <https://developer.cognite.com/api#tag/Views/operation/byExternalIdsViews>`_

        Args:
            ids: The view identifier(s). This can be given as a tuple of strings or a ViewId object. For example, ("my_space", "my_view"), ("my_space", "my_view", "my_version"), or ViewId("my_space", "my_view", "my_version"). Note that version is optional, if not provided, all versions will be returned.
            include_inherited_properties: Whether to include properties inherited from views this view implements.
            all_versions: Whether to return all versions. If false, only the newest version is returned (based on created_time)

        Returns:
            Requested view or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.views.retrieve(('mySpace', 'myView', 'v1'))
        """
        return run_sync(
            self.__async_client.data_modeling.views.retrieve(
                ids=ids, include_inherited_properties=include_inherited_properties, all_versions=all_versions
            )
        )

    def delete(self, ids: ViewIdentifier | Sequence[ViewIdentifier]) -> list[ViewId]:
        """
        `Delete one or more views <https://developer.cognite.com/api#tag/Views/operation/deleteViews>`_

        Args:
            ids: View identifier(s)
        Returns:
            The identifier for the view(s) which has been deleted. Empty list if nothing was deleted.
        Examples:

            Delete views by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.views.delete(('mySpace', 'myView', 'v1'))
        """
        return run_sync(self.__async_client.data_modeling.views.delete(ids=ids))

    def list(
        self,
        limit: int | None = DATA_MODELING_DEFAULT_LIMIT_READ,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> ViewList:
        """
        `List views <https://developer.cognite.com/api#tag/Views/operation/listViews>`_

        Args:
            limit: Maximum number of views to return. Defaults to 10. Set to -1, float("inf") or None to return all items.
            space: The space to query.
            include_inherited_properties: Whether to include properties inherited from views this view implements.
            all_versions: Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
            include_global: Whether to include global views.

        Returns:
            List of requested views

        Examples:

            List 5 views:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> view_list = client.data_modeling.views.list(limit=5)

            Iterate over views, one-by-one:

                >>> for view in client.data_modeling.views():
                ...     view  # do something with the view

            Iterate over chunks of views to reduce memory load:

                >>> for view_list in client.data_modeling.views(chunk_size=10):
                ...     view_list # do something with the views
        """
        return run_sync(
            self.__async_client.data_modeling.views.list(
                limit=limit,
                space=space,
                include_inherited_properties=include_inherited_properties,
                all_versions=all_versions,
                include_global=include_global,
            )
        )

    @overload
    def apply(self, view: Sequence[ViewApply]) -> ViewList: ...

    @overload
    def apply(self, view: ViewApply) -> View: ...

    def apply(self, view: ViewApply | Sequence[ViewApply]) -> View | ViewList:
        """
        `Create or update (upsert) one or more views. <https://developer.cognite.com/api#tag/Views/operation/ApplyViews>`_

        Args:
            view: View(s) to create or update.

        Returns:
            Created view(s)

        Examples:

            Create new views:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewApply, MappedPropertyApply, ContainerId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> views = [
                ...     ViewApply(
                ...         space="mySpace",
                ...         external_id="myView",
                ...         version="v1",
                ...         properties={
                ...             "someAlias": MappedPropertyApply(
                ...                 container=ContainerId("mySpace", "myContainer"),
                ...                 container_property_identifier="someProperty",
                ...             ),
                ...         }
                ...    )
                ... ]
                >>> res = client.data_modeling.views.apply(views)


            Create views with edge relations:

                >>> from cognite.client.data_classes.data_modeling import (
                ...     ContainerId,
                ...     DirectRelationReference,
                ...     MappedPropertyApply,
                ...     MultiEdgeConnectionApply,
                ...     ViewApply,
                ...     ViewId
                ... )
                >>> work_order_for_asset = DirectRelationReference(space="mySpace", external_id="work_order_for_asset")
                >>> work_order_view = ViewApply(
                ...     space="mySpace",
                ...     external_id="WorkOrder",
                ...     version="v1",
                ...     name="WorkOrder",
                ...     properties={
                ...         "title": MappedPropertyApply(
                ...             container=ContainerId(space="mySpace", external_id="WorkOrder"),
                ...             container_property_identifier="title",
                ...         ),
                ...         "asset": MultiEdgeConnectionApply(
                ...             type=work_order_for_asset,
                ...             direction="outwards",
                ...             source=ViewId("mySpace", "Asset", "v1"),
                ...             name="asset",
                ...         ),
                ...     }
                ... )
                >>> asset_view = ViewApply(
                ...     space="mySpace",
                ...     external_id="Asset",
                ...     version="v1",
                ...     name="Asset",
                ...     properties={
                ...         "name": MappedPropertyApply(
                ...             container=ContainerId("mySpace", "Asset"),
                ...             name="name",
                ...             container_property_identifier="name",
                ...         ),
                ...         "work_orders": MultiEdgeConnectionApply(
                ...             type=work_order_for_asset,
                ...             direction="inwards",
                ...             source=ViewId("mySpace", "WorkOrder", "v1"),
                ...             name="work_orders",
                ...         ),
                ...     }
                ... )
                >>> res = client.data_modeling.views.apply([work_order_view, asset_view])
        """
        return run_sync(self.__async_client.data_modeling.views.apply(view=view))
