from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import TransformationSchemaColumn, TransformationSchemaColumnList
from cognite.client.data_classes.transformations.common import DataModelInstances


class TransformationSchemaAPI(APIClient):
    _RESOURCE_PATH = "/transformations/schema"

    def retrieve(self, destination, conflict_mode=None):
        url_path = utils._auxiliary.interpolate_and_url_encode((self._RESOURCE_PATH + "/{}"), str(destination.type))
        filter = destination.dump(True)
        filter.pop("type")
        if isinstance(destination, DataModelInstances):
            filter["externalId"] = filter.pop("modelExternalId")
            filter.pop("instanceSpaceExternalId")
        if conflict_mode:
            filter["conflictMode"] = conflict_mode
        return self._list(
            list_cls=TransformationSchemaColumnList,
            resource_cls=TransformationSchemaColumn,
            method="GET",
            resource_path=url_path,
            filter=filter,
        )
