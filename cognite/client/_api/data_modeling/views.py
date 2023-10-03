from __future__ import annotations

from collections import defaultdict
from typing import Iterator, Sequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DATA_MODELING_DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.ids import (
    ViewId,
    ViewIdentifier,
    _load_identifier,
)
from cognite.client.data_classes.data_modeling.views import View, ViewApply, ViewFilter, ViewList

from ._data_modeling_executor import get_data_modeling_executor


class ViewsAPI(APIClient):
    _RESOURCE_PATH = "/models/views"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[View]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[ViewList]:
        ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> Iterator[View] | Iterator[ViewList]:
        """Iterate over views

        Fetches views as they are iterated over, so you keep a limited number of views in memory.

        Args:
            chunk_size (int | None): Number of views to return in each chunk. Defaults to yielding one view at a time.
            limit (int | None): Maximum number of views to return. Defaults to returning all items.
            space (str | None): (str | None): The space to query.
            include_inherited_properties (bool): Whether to include properties inherited from views this view implements.
            all_versions (bool): Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
            include_global (bool): Whether to include global views.

        Returns:
            Iterator[View] | Iterator[ViewList]: yields View one by one if chunk_size is not specified, else ViewList objects.
        """
        filter_ = ViewFilter(space, include_inherited_properties, all_versions, include_global)
        return self._list_generator(
            list_cls=ViewList,
            resource_cls=View,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            filter=filter_.dump(camel_case=True),
        )

    def __iter__(self) -> Iterator[View]:
        """Iterate over views

        Fetches views as they are iterated over, so you keep a limited number of views in memory.

        Returns:
            Iterator[View]: yields Views one by one.
        """
        return self()

    def _get_latest_views(self, views: ViewList) -> ViewList:
        views_by_space_and_xid = defaultdict(list)
        for view in views:
            views_by_space_and_xid[(view.space, view.external_id)].append(view)
        return ViewList([max(views, key=lambda view: view.created_time) for views in views_by_space_and_xid.values()])

    def retrieve(
        self,
        ids: ViewIdentifier | Sequence[ViewIdentifier],
        include_inherited_properties: bool = True,
        all_versions: bool = True,
    ) -> ViewList:
        """`Retrieve a single view by id. <https://developer.cognite.com/api#tag/Views/operation/byExternalIdsViews>`_

        Args:
            ids (ViewIdentifier | Sequence[ViewIdentifier]): View identifier(s)
            include_inherited_properties (bool): Whether to include properties inherited from views this view implements.
            all_versions (bool): Whether to return all versions. If false, only the newest version is returned,

        Returns:
            ViewList: Requested view or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.views.retrieve(('mySpace', 'myView', 'v1'))

        """
        identifier = _load_identifier(ids, "view")
        views = self._retrieve_multiple(
            list_cls=ViewList,
            resource_cls=View,
            identifiers=identifier,
            params={"includeInheritedProperties": include_inherited_properties},
            executor=get_data_modeling_executor(),
        )
        if all_versions is True:
            return views
        else:
            return self._get_latest_views(views)

    def delete(self, ids: ViewIdentifier | Sequence[ViewIdentifier]) -> list[ViewId]:
        """`Delete one or more views <https://developer.cognite.com/api#tag/Views/operation/deleteViews>`_

        Args:
            ids (ViewIdentifier | Sequence[ViewIdentifier]): View identifier(s)
        Returns:
            list[ViewId]: The identifier for the view(s) which has been deleted. Empty list if nothing was deleted.
        Examples:

            Delete views by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.views.delete(('mySpace', 'myView', 'v1'))
        """
        deleted_views = cast(
            list,
            self._delete_multiple(
                identifiers=_load_identifier(ids, "view"),
                wrap_ids=True,
                returns_items=True,
                executor=get_data_modeling_executor(),
            ),
        )
        return [ViewId(item["space"], item["externalId"], item["version"]) for item in deleted_views]

    def list(
        self,
        limit: int | None = DATA_MODELING_DEFAULT_LIMIT_READ,
        space: str | None = None,
        include_inherited_properties: bool = True,
        all_versions: bool = False,
        include_global: bool = False,
    ) -> ViewList:
        """`List views <https://developer.cognite.com/api#tag/Views/operation/listViews>`_

        Args:
            limit (int | None): Maximum number of views to return. Defaults to 10. Set to -1, float("inf") or None to return all items.
            space (str | None): (str | None): The space to query.
            include_inherited_properties (bool): Whether to include properties inherited from views this view implements.
            all_versions (bool): Whether to return all versions. If false, only the newest version is returned, which is determined based on the 'createdTime' field.
            include_global (bool): Whether to include global views.

        Returns:
            ViewList: List of requested views

        Examples:

            List 5 views::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> view_list = c.data_modeling.views.list(limit=5)

            Iterate over views::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for view in c.data_modeling.views:
                ...     view # do something with the view

            Iterate over chunks of views to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for view_list in c.data_modeling.views(chunk_size=10):
                ...     view_list # do something with the views
        """
        filter_ = ViewFilter(space, include_inherited_properties, all_versions, include_global)

        return self._list(
            list_cls=ViewList, resource_cls=View, method="GET", limit=limit, filter=filter_.dump(camel_case=True)
        )

    @overload
    def apply(self, view: Sequence[ViewApply]) -> ViewList:
        ...

    @overload
    def apply(self, view: ViewApply) -> View:
        ...

    def apply(self, view: ViewApply | Sequence[ViewApply]) -> View | ViewList:
        """`Create or update (upsert) one or more views. <https://developer.cognite.com/api#tag/Views/operation/ApplyViews>`_

        Args:
            view (ViewApply | Sequence[ViewApply]): View(s) to create or update.

        Returns:
            View | ViewList: Created view(s)

        Examples:

            Create new views::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewApply
                >>> c = CogniteClient()
                >>> views = [ViewApply(space="mySpace",external_id="myView",version="v1"),
                ... ViewApply(space="mySpace",external_id="myOtherView",version="v1")]
                >>> res = c.data_modeling.views.apply(views)
        """
        return self._create_multiple(
            list_cls=ViewList,
            resource_cls=View,
            items=view,
            input_resource_cls=ViewApply,
            executor=get_data_modeling_executor(),
        )
