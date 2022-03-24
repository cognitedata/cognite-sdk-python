from typing import Optional

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import TransformationDestination, TransformationSchemaColumnList


class TransformationSchemaAPI(APIClient):
    _RESOURCE_PATH = "/transformations/schema"
    _LIST_CLASS = TransformationSchemaColumnList

    def retrieve(
        self, destination: TransformationDestination = None, conflict_mode: Optional[str] = None
    ) -> TransformationSchemaColumnList:
        """`Get expected schema for a transformation destination. <https://docs.cognite.com/api/v1/#operation/getTransformationSchema>`_

        Args:
            destination (TransformationDestination): destination for which the schema is requested.
            conflict_mode (Optional[str]): conflict mode for which the schema is requested.

        Returns:
            TransformationSchemaColumnList: List of column descriptions

        Example:

            Get the schema for a transformation producing assets::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationDestination
                >>> c = CogniteClient()
                >>> columns = c.transformations.schema.retrieve(destination = TransformationDestination.assets())
        """

        url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", str(destination.type))
        other_params = {"conflictMode": conflict_mode} if conflict_mode else None
        return self._list(method="GET", resource_path=url_path, filter=other_params)

    def _alpha_retrieve_data_model_schema(self, modelExternalId: str = None) -> TransformationSchemaColumnList:
        url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", "datamodelinstances")
        return self._list(method="GET", resource_path=url_path, filter={"externalId": modelExternalId})
