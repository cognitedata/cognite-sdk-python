from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    TransformationJob,
    TransformationJobFilter,
    TransformationJobList,
    TransformationJobMetric,
    TransformationJobMetricList,
)
from cognite.client.utils._identifier import IdentifierSequence


class TransformationJobsAPI(APIClient):
    _RESOURCE_PATH = "/transformations/jobs"

    def list(self, limit=25, transformation_id=None, transformation_external_id=None):
        filter = TransformationJobFilter(
            transformation_id=transformation_id, transformation_external_id=transformation_external_id
        ).dump(camel_case=True)
        return self._list(
            list_cls=TransformationJobList, resource_cls=TransformationJob, method="GET", limit=limit, filter=filter
        )

    def retrieve(self, id):
        identifiers = IdentifierSequence.load(ids=id, external_ids=None).as_singleton()
        return self._retrieve_multiple(
            list_cls=TransformationJobList, resource_cls=TransformationJob, identifiers=identifiers
        )

    def list_metrics(self, id):
        url_path = utils._auxiliary.interpolate_and_url_encode((self._RESOURCE_PATH + "/{}/metrics"), str(id))
        return self._list(
            list_cls=TransformationJobMetricList,
            resource_cls=TransformationJobMetric,
            method="GET",
            limit=None,
            resource_path=url_path,
        )

    def retrieve_multiple(self, ids, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=None)
        return self._retrieve_multiple(
            list_cls=TransformationJobList,
            resource_cls=TransformationJob,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )
