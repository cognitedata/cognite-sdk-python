from __future__ import annotations

from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    TransformationDestination,
    TransformationSchemaColumn,
    TransformationSchemaColumnList,
)
from cognite.client.utils._auxiliary import interpolate_and_url_encode


class TransformationSchemaAPI(APIClient):
    _RESOURCE_PATH = "/transformations/schema"

    def retrieve(
        self, destination: TransformationDestination, conflict_mode: str | None = None
    ) -> TransformationSchemaColumnList:
        """`Get expected schema for a transformation destination. <https://developer.cognite.com/api#tag/Schema/operation/getTransformationSchema>`_

        Args:
            destination (TransformationDestination): destination for which the schema is requested.
            conflict_mode (str | None): conflict mode for which the schema is requested.

        Returns:
            TransformationSchemaColumnList: List of column descriptions

        Example:

            Get the schema for a transformation producing assets::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationDestination
                >>> c = CogniteClient()
                >>> columns = c.transformations.schema.retrieve(destination = TransformationDestination.assets())
        """

        url_path = interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", str(destination.type))
        filter = destination.dump(True)
        filter.pop("type")
        other_params = {"conflictMode": conflict_mode} if conflict_mode else None

        return self._list(
            list_cls=TransformationSchemaColumnList,
            resource_cls=TransformationSchemaColumn,
            method="GET",
            resource_path=url_path,
            filter=other_params,
        )
