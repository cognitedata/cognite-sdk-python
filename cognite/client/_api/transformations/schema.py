from __future__ import annotations

from typing import Optional

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    TransformationDestination,
    TransformationSchemaColumn,
    TransformationSchemaColumnList,
)


class TransformationSchemaAPI(APIClient):
    _RESOURCE_PATH = "/transformations/schema"

    def retrieve(
        self,
        destination: TransformationDestination,
        conflict_mode: Optional[str] = None,
        with_instance_space: Optional[bool] = None,
        is_connection_definition: Optional[bool] = None,
        instance_type: Optional[str] = None,
    ) -> TransformationSchemaColumnList:
        """`Get expected schema for a transformation destination. <https://docs.cognite.com/api/v1/#operation/getTransformationSchema>`_

        Args:
            destination (TransformationDestination): destination for which the schema is requested.
            conflict_mode (Optional[str]): conflict mode for which the schema is requested.
            with_instance_space (Optional[bool]): Is instance space set at the transformation config or not
            is_connection_definition (Optional[bool]): If the edge is a connection definition or not
            instance_type (Optional[str]): Instance type to deal with, Enum: "nodes" "edges"

        Returns:
            TransformationSchemaColumnList: List of column descriptions

        Example:

            Get the schema for a transformation producing assets::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationDestination
                >>> c = CogniteClient()
                >>> columns = c.transformations.schema.retrieve(destination = TransformationDestination.assets())

            Get the schema for a transformation producing nodes::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationDestination
                >>> from cognite.client.data_classes.transformations.common import ViewInfo
                >>> c = CogniteClient()
                >>> view = ViewInfo(space="viewSpace", external_id="viewExternalId", version="viewVersion")
                >>> columns = c.transformations.schema.retrieve(destination=TransformationDestination.nodes(view, "InstanceSpace")


            Get the schema for a transformation producing edges::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationDestination
                >>> from cognite.client.data_classes.transformations.common import ViewInfo
                >>> c = CogniteClient()
                >>> view = ViewInfo(space="viewSpace", external_id="viewExternalId", version="viewVersion")
                >>> columns = c.transformations.schema.retrieve(destination=TransformationDestination.edges(view, "InstanceSpace")


            Get the schema for a transformation producing instance type data model::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationDestination
                >>> from cognite.client.data_classes.transformations.common import DataModelInfo
                >>> c = CogniteClient()
                >>> data_model = DataModelInfo(space="dataModelSpace", external_id="dataModelExternalId",version="dataModelVersion",destination_type="type")
                >>> columns = c.transformations.schema.retrieve(destination=TransformationDestination.instances(data_model, "InstanceSpace")


            Get the schema for a transformation producing instance relationship data model::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationDestination
                >>> from cognite.client.data_classes.transformations.common import DataModelInfo
                >>> c = CogniteClient()
                >>> data_model = DataModelInfo(space="dataModelSpace", external_id="dataModelExternalId",version="dataModelVersion",destination_type="type", destination_relationship_from_type="relationshipFromType")
                >>> columns = c.transformations.schema.retrieve(destination=TransformationDestination.instances(data_model, "InstanceSpace")

        """

        if destination.type == "nodes" or destination.type == "edges" or destination.type == "instances":
            url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", "instances")
            filter = destination.dump(True)
            other_params = (
                {
                    "conflictMode": conflict_mode,
                    "withInstanceSpace": with_instance_space,
                    "isConnectionDefinition": is_connection_definition,
                    "instanceType": instance_type,
                }
                if conflict_mode or with_instance_space or is_connection_definition or instance_type
                else None
            )

            return self._list(
                list_cls=TransformationSchemaColumnList,
                resource_cls=TransformationSchemaColumn,
                method="GET",
                resource_path=url_path,
                filter=other_params,
            )

        else:
            url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", str(destination.type))
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
