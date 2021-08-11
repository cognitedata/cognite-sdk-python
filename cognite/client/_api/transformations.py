import json as _json
from typing import List, Optional, Union

from cognite.client import utils
from cognite.client._api_client import APIClient
from requests import Response

from cognite.client._api.transformation_schedules import TransformationSchedulesAPI
from cognite.client.data_classes import Transformation, TransformationList
from cognite.client.data_classes.transformations import TransformationFilter, TransformationUpdate


class TransformationsAPI(APIClient):
    _RESOURCE_PATH = "/transformations"
    _LIST_CLASS = TransformationList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.jobs = TransformationJobsAPI(*args, **kwargs)
        self.schedules = TransformationSchedulesAPI(*args, **kwargs)
        # self.notifications = TransformationNotificationsAPI(*args, **kwargs)

    def create(
        self, transformation: Union[Transformation, List[Transformation]]
    ) -> Union[Transformation, TransformationList]:
        """`Create one or more transformations. <https://docs.cognite.com/api/playground/#operation/createTransformations>`_

        Args:
            transformation (Union[Transformation, List[Transformation]]): Transformation or list of transformations to create.

        Returns:
            Created transformation(s)

        Examples:

            Create new transformations:

                >>> from cognite.experimental import CogniteClient
                >>> from cognite.experimental.data_classes import Transformation, TransformationDestination
                >>> c = CogniteClient()
                >>> transformations = [
                >>>     Transformation(
                >>>         name="transformation1", 
                >>>         destination=TransformationDestination.assets()
                >>>     ),
                >>>     Transformation(
                >>>         name="transformation2",
                >>>         destination=TransformationDestination.raw("myDatabase", "myTable"),
                >>>     ),
                >>> ]
                >>> res = c.transformations.create(transformations)
        """
        utils._auxiliary.assert_type(transformation, "transformation", [Transformation, list])
        return self._create_multiple(transformation)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """`Delete one or more transformations. <https://docs.cognite.com/api/playground/#operation/deleteTransformations>`_

        Args:
            id (Union[int, List[int]): Id or list of ids.
            external_id (Union[str, List[str]]): External ID or list of external ids.

        Returns:
            None

        Example:

            Delete transformations by id or external id::

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> c.transformations.delete(id=[1,2,3], external_id="function3")
        """
        self._delete_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def list(self, include_public: bool = True, limit: Optional[int] = 25,) -> TransformationList:
        """`List all transformations. <https://docs.cognite.com/api/playground/#operation/transformations>`_

        Args:
            include_public (bool): Whether public transformations should be included in the results. (default true).
            cursor (str): Cursor for paging through results.
            limit (int): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.

        Returns:
            TransformationList: List of transformations

        Example:

            List transformations::

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> transformations_list = c.transformations.list()
        """
        filter = TransformationFilter(include_public=include_public).dump(camel_case=True)

        return self._list(method="GET", limit=limit, filter=filter,)

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Transformation]:
        """`Retrieve a single transformation by id. <https://docs.cognite.com/api/playground/#operation/getTransformation>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[Transformation]: Requested transformation or None if it does not exist.

        Examples:

            Get transformation by id:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.retrieve(id=1)

            Get transformation by external id:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.retrieve(external_id="1")
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def update(
        self, item: Union[Transformation, TransformationUpdate, List[Union[Transformation, TransformationUpdate]]]
    ) -> Union[Transformation, TransformationList]:
        """`Update one or more transformations <https://docs.cognite.com/api/playground/#operation/updateTransformations>`_

        Args:
            item (Union[Transformation, TransformationUpdate, List[Union[Transformation, TransformationUpdate]]]): Transformation(s) to update

        Returns:
            Union[Transformation, TransformationList]: Updated transformation(s)

        Examples:

            Update a transformation that you have fetched. This will perform a full update of the transformation::

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> transformation = c.transformations.retrieve(id=1)
                >>> transformation.query = "SELECT * FROM _cdf.assets"
                >>> res = c.transformations.update(transformation)

            Perform a partial update on a transformation, updating the query and making it private::

                >>> from cognite.experimental import CogniteClient
                >>> from cognite.experimental.data_classes import TransformationUpdate
                >>> c = CogniteClient()
                >>> my_update = TransformationUpdate(id=1).query.set("SELECT * FROM _cdf.assets").is_public.set(False)
                >>> res = c.transformations.update(my_update)
        """
        return self._update_multiple(items=item)
