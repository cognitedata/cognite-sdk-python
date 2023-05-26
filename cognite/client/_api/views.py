from __future__ import annotations

from typing import Iterator, Sequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.data_classes.data_modeling import View, ViewList
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.utils._identifier import DataModelingIdentifierSequence


class ViewsAPI(APIClient):
    _RESOURCE_PATH = "/models/views"

    def __call__(
        self,
        chunk_size: int = None,
        limit: int = None,
    ) -> Iterator[View] | Iterator[ViewList]:
        """Iterate over views

        Fetches views as they are iterated over, so you keep a limited number of views in memory.

        Args:
            chunk_size (int, optional): Number of views to return in each chunk. Defaults to yielding one view a time.
            limit (int, optional): Maximum number of views to return. Default to return all items.

        Yields:
            Union[View, ViewList]: yields View one by one if chunk_size is not specified, else ViewList objects.
        """
        return self._list_generator(
            list_cls=ViewList,
            resource_cls=View,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
        )

    def __iter__(self) -> Iterator[View]:
        """Iterate over views

        Fetches views as they are iterated over, so you keep a limited number of views in memory.

        Yields:
            View: yields Views one by one.
        """
        return cast(Iterator[View], self())

    @overload
    def retrieve(self, ids: ViewId) -> View | None:
        ...

    @overload
    def retrieve(self, ids: Sequence[ViewId]) -> ViewList:
        ...

    def retrieve(self, ids: ViewId | Sequence[ViewId]) -> View | ViewList | None:
        """`Retrieve a single view by id. <https://docs.cognite.com/api/v1/#tag/Views/operation/byViewIdsViews>`_

        Args:
            ids (ViewId | Sequence[ViewId]): View dentifier(s)

        Returns:
            Optional[View]: Requested view or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.views.retrieve(view='myView')

        """
        identifier = DataModelingIdentifierSequence.load(ids)
        return self._retrieve_multiple(list_cls=ViewList, resource_cls=View, identifiers=identifier)

    def delete(self, ids: ViewId | Sequence[ViewId]) -> list[tuple[str, str]] | None:
        """`Delete one or more views <https://docs.cognite.com/api/v1/#tag/Views/operation/deleteViewsV3>`_

        Args:
            ids (ViewId | Sequence[ViewId]): View dentifier(s)
        Returns:
            The view(s) which has been deleted. None if nothing was deleted.
        Examples:

            Delete views by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.views.delete(view=["myView", "myOtherView"])
        """
        deleted_views = self._delete_multiple(
            identifiers=DataModelingIdentifierSequence.load(ids),
            wrap_ids=True,
            returns_items=True,
        )
        return [(item["space"], item["externalId"]) for item in deleted_views]  # type: ignore[union-attr]

    def list(
        self,
        limit: int = LIST_LIMIT_DEFAULT,
    ) -> ViewList:
        """`List views <https://docs.cognite.com/api/v1/#tag/Views/operation/listViewsV3>`_

        Args:
            limit (int, optional): Maximum number of views to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ViewList: List of requested views

        Examples:

            List views and filter on max start time::

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
                >>> for view_list in c.data_modeling.views(chunk_size=2500):
                ...     view_list # do something with the views
        """
        return self._list(
            list_cls=ViewList,
            resource_cls=View,
            method="GET",
            limit=limit,
        )

    @overload
    def apply(self, view: Sequence[View]) -> ViewList:
        ...

    @overload
    def apply(self, view: View) -> View:
        ...

    def apply(self, view: View | Sequence[View]) -> View | ViewList:
        """`Create or patch one or more views. <https://docs.cognite.com/api/v1/#tag/Views/operation/ApplyViews>`_

        Args:
            view (view: View | Sequence[View]): View or views of viewsda to create or update.

        Returns:
            View | ViewList: Created view(s)

        Examples:

            Create new viewsda::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import View
                >>> c = CogniteClient()
                >>> views = [View(view="myView", description="My first view", name="My View"),
                ... View(view="myOtherView", description="My second view", name="My Other View")]
                >>> res = c.data_modeling.views.create(views)
        """
        return self._create_multiple(list_cls=ViewList, resource_cls=View, items=view)
