from typing import List, Optional

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import TransformationJob, TransformationJobList, TransformationJobMetricList


class TransformationJobsAPI(APIClient):
    _RESOURCE_PATH = "/transformations/jobs"
    _LIST_CLASS = TransformationJobList

    def list(self, limit: Optional[int] = 25, transformation_id: Optional[int] = None) -> TransformationJobList:
        """`List all running transformation jobs. <https://docs.cognite.com/api/playground/#operation/transformationJobs>`_

        Args:
            limit (int): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.
            transformation_id (int): Filters the results by the internal transformation id.

        Returns:
            TransformationJobList: List of transformation jobs

        Example:

            List transformation jobs::

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> transformation_jobs_list = c.transformations.jobs.list()

            List transformation jobs of a single transformation::

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> transformation_jobs_list = c.transformations.jobs.list(transformation_id = 1)
        """
        if transformation_id is not None:
            resource_path = utils._auxiliary.interpolate_and_url_encode(
                "/transformations/{}/jobs", str(transformation_id)
            )
            return self._list(method="GET", limit=limit, resource_path=resource_path)

        return self._list(
            method="GET",
            limit=limit,
        )

    def retrieve(self, id: int) -> Optional[TransformationJob]:
        """`Retrieve a single transformation job by id. <https://docs.cognite.com/api/playground/#operation/getTransformationJob>`_

        Args:
            id (int): Job internal Id

        Returns:
            Optional[TransformationJob]: Requested transformation job or None if it does not exist.

        Examples:

            Get transformation job by id:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.jobs.retrieve(id=1)
        """
        return self._retrieve(id=id)

    def list_metrics(self, id: int) -> TransformationJobMetricList:
        """`List the metrics of a single transformation job. <https://docs.cognite.com/api/playground/#operation/getTransformationJob>`_

        Args:
            id (int): Job internal Id

        Returns:
            TransformationJobMetricList: List of updated metrics of the given job.

        Examples:

            Get metrics by transformation job id:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.jobs.list_metrics(id=1)
        """
        url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/metrics", str(id))

        return self._list(method="GET", limit=None, resource_path=url_path, cls=TransformationJobMetricList)

    def retrieve_multiple(self, ids: List[int], ignore_unknown_ids: bool = False) -> TransformationJobList:
        """`Retrieve multiple transformation jobs by id. <https://docs.cognite.com/api/playground/#operation/getTransformationJob>`_

        Args:
            ids (List[int]): Job internal Ids
            ignore_unknown_ids (bool): Ignore IDs that are not found rather than throw an exception.

        Returns:
            TransformationJobList: Requested transformation jobs.

        Examples:

            Get jobs by id::

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.jobs.retrieve_multiple(ids=[1, 2, 3])
        """
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, ignore_unknown_ids=ignore_unknown_ids, wrap_ids=True)
