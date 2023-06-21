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
        self, destination: TransformationDestination, conflict_mode: Optional[str] = None
    ) -> TransformationSchemaColumnList:
        # Implementation for other transformation destinations

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

        other_params = {"conflictMode": conflict_mode} if conflict_mode else None
        return self._retrieve_schema(destination, other_params)

    def retrieve_instances(
        self,
        destination: TransformationDestination,
        conflict_mode: Optional[str] = None,
        with_instance_space: Optional[bool] = None,
        is_connection_definition: Optional[bool] = None,
        instance_type: Optional[str] = None,
    ) -> TransformationSchemaColumnList:
        # Implementation for Instances transformation destination

        other_params = {
            "conflictMode": conflict_mode,
            "withInstanceSpace": with_instance_space,
            "isConnectionDefinition": is_connection_definition,
            "instanceType": instance_type,
        }

        """`Get expected schema for a transformation destination. <https://docs.cognite.com/api/v1/#operation/getFlexibleDataModelInstanceSchema>`_

            Args:
                destination (TransformationDestination): destination for which the schema is requested.
                conflict_mode (Optional[str]): conflict mode for which the schema is requested.
                with_instance_space (Optional[bool]): Is instance space set at the transformation config or not
                is_connection_definition (Optional[bool]): If the edge is a connection definition or not
                instance_type (Optional[str]): Instance type to deal with, Enum: "nodes" "edges"

            Returns:
                TransformationSchemaColumnList: List of column descriptions

            Example:

                Get the schema for a transformation producing nodes::

                    >>> from cognite.client import CogniteClient
                    >>> from cognite.client.data_classes import TransformationDestination
                    >>> from cognite.client.data_classes.transformations.common import ViewInfo
                    >>> c = CogniteClient()
                    >>> view = ViewInfo(space="viewSpace", external_id="viewExternalId", version="viewVersion")
                    >>> columns = c.transformations.schema.retrieve_instances(destination=TransformationDestination.nodes(view, "InstanceSpace")


                Get the schema for a transformation producing edges::

                    >>> from cognite.client import CogniteClient
                    >>> from cognite.client.data_classes import TransformationDestination
                    >>> from cognite.client.data_classes.transformations.common import ViewInfo
                    >>> c = CogniteClient()
                    >>> view = ViewInfo(space="viewSpace", external_id="viewExternalId", version="viewVersion")
                    >>> columns = c.transformations.schema.retrieve_instances(destination=TransformationDestination.edges(view, "InstanceSpace")


                Get the schema for a transformation producing instance type data model::

                    >>> from cognite.client import CogniteClient
                    >>> from cognite.client.data_classes import TransformationDestination
                    >>> from cognite.client.data_classes.transformations.common import DataModelInfo
                    >>> c = CogniteClient()
                    >>> data_model = DataModelInfo(space="dataModelSpace", external_id="dataModelExternalId",version="dataModelVersion",destination_type="type")
                    >>> columns = c.transformations.schema.retrieve_instances(destination=TransformationDestination.instances(data_model, "InstanceSpace")


                Get the schema for a transformation producing instance relationship data model::

                    >>> from cognite.client import CogniteClient
                    >>> from cognite.client.data_classes import TransformationDestination
                    >>> from cognite.client.data_classes.transformations.common import DataModelInfo
                    >>> c = CogniteClient()
                    >>> data_model = DataModelInfo(space="dataModelSpace", external_id="dataModelExternalId",version="dataModelVersion",destination_type="type", destination_relationship_from_type="relationshipFromType")
                    >>> columns = c.transformations.schema.retrieve_instances(destination=TransformationDestination.instances(data_model, "InstanceSpace")

            """

        return self._retrieve_schema(destination, other_params)

    def _retrieve_schema(
        self, destination: TransformationDestination, other_params: Optional[dict]
    ) -> TransformationSchemaColumnList:
        if destination.type in ["nodes", "edges", "instances"]:
            url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", "instances")
        else:
            url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", str(destination.type))

        return self._list(
            list_cls=TransformationSchemaColumnList,
            resource_cls=TransformationSchemaColumn,
            method="GET",
            resource_path=url_path,
            filter=other_params,
        )
