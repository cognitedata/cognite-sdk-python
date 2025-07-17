from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client._cognite_client import CogniteClient


@dataclass
class InstanceStatistics(CogniteObject):
    """Statistics for instances in the data modeling API.

    Attributes:
        edges (int): Number of edges in the project.
        soft_deleted_edges (int): Number of soft-deleted edges in the project.
        nodes (int): Number of nodes in the project.
        soft_deleted_nodes (int): Number of soft-deleted nodes in the project.
        instances (int): Total number of instances in the project.
        instances_limit (int): Maximum number of instances allowed in the project.
        soft_deleted_instances (int): Total number of soft-deleted instances in the project.
        soft_deleted_instances_limit (int): Maximum number of soft-deleted instances allowed in the project.
    """

    edges: int
    soft_deleted_edges: int
    nodes: int
    soft_deleted_nodes: int
    instances: int
    instances_limit: int
    soft_deleted_instances: int
    soft_deleted_instances_limit: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: "CogniteClient | None" = None) -> Self:
        return cls(
            edges=resource["edges"],
            soft_deleted_edges=resource["softDeletedEdges"],
            nodes=resource["nodes"],
            soft_deleted_nodes=resource["softDeletedNodes"],
            instances=resource["instances"],
            instances_limit=resource["instancesLimit"],
            soft_deleted_instances=resource["softDeletedInstances"],
            soft_deleted_instances_limit=resource["softDeletedInstancesLimit"],
        )


@dataclass
class CountLimit(CogniteObject):
    """Usage and limits for a specific resource in the data modeling API.

    Attributes:
        count (int): The current usage count for the resource.
        limit (int): The maximum allowed limit for the resource.

    """

    count: int
    limit: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: "CogniteClient | None" = None) -> Self:
        return cls(count=resource["count"], limit=resource["limit"])


@dataclass
class SpaceStatistics(CogniteResource):
    """Statistics for a space in the data modeling API.

    Attributes:
        space (str): The space name
        containers (int): Number of containers in the space.
        views (int): Number of views in the space.
        data_models (int): Number of data models in the space.
        nodes (int): Number of nodes in the space.
        edges (int): Number of edges in the space.
        soft_deleted_nodes (int): Number of soft-deleted nodes in the space.
        soft_deleted_edges (int): Number of soft-deleted edges in the space.

    """

    space: str
    containers: int
    views: int
    data_models: int
    edges: int
    soft_deleted_edges: int
    nodes: int
    soft_deleted_nodes: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: "CogniteClient | None" = None) -> Self:
        return cls(
            space=resource["space"],
            containers=resource["containers"],
            views=resource["views"],
            data_models=resource["dataModels"],
            edges=resource["edges"],
            soft_deleted_edges=resource["softDeletedEdges"],
            nodes=resource["nodes"],
            soft_deleted_nodes=resource["softDeletedNodes"],
        )


@dataclass
class ProjectStatistics(CogniteResource):
    """Statistics for a project in the data modeling API.

    Attributes:
        spaces (CountLimit): Usage and limits for spaces in the project
        containers (CountLimit): Usage and limits for containers in the project
        views (CountLimit): Usage and limits for views including all versions in the project
        data_models (CountLimit): Usage and limits for data models including all versions in the project
        container_properties (CountLimit): Usage and limits for sum of container properties in the project
        instances (InstanceStatistics): Usage and limits for number of instances in the project
        concurrent_read_limit (int): Maximum number of concurrent read operations allowed in the project
        concurrent_write_limit (int): Maximum number of concurrent write operations allowed in the project
        concurrent_delete_limit (int): Maximum number of concurrent delete operations allowed in the project
    """

    spaces: CountLimit
    containers: CountLimit
    views: CountLimit
    data_models: CountLimit
    container_properties: CountLimit
    instances: InstanceStatistics
    concurrent_read_limit: int
    concurrent_write_limit: int
    concurrent_delete_limit: int

    @property
    def project(self) -> str:
        """The project name."""
        return self._cognite_client.config.project

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: "CogniteClient | None" = None) -> Self:
        return cls(
            spaces=CountLimit._load(resource["spaces"]),
            containers=CountLimit._load(resource["containers"]),
            views=CountLimit._load(resource["views"]),
            data_models=CountLimit._load(resource["dataModels"]),
            container_properties=CountLimit._load(resource["containerProperties"]),
            instances=InstanceStatistics._load(resource["instances"]),
            concurrent_read_limit=resource["concurrentReadLimit"],
            concurrent_write_limit=resource["concurrentWriteLimit"],
            concurrent_delete_limit=resource["concurrentDeleteLimit"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the object to a dictionary."""
        dumped = {
            "spaces": self.spaces.dump(camel_case),
            "containers": self.containers.dump(camel_case),
            "views": self.views.dump(camel_case),
            "data_models": self.data_models.dump(camel_case),
            "container_properties": self.container_properties.dump(camel_case),
            "instances": self.instances.dump(camel_case),
            "concurrent_read_limit": self.concurrent_read_limit,
            "concurrent_write_limit": self.concurrent_write_limit,
            "concurrent_delete_limit": self.concurrent_delete_limit,
        }
        if camel_case:
            return convert_all_keys_to_camel_case(dumped)
        return dumped


class SpaceStatisticsList(CogniteResourceList[SpaceStatistics]):
    _RESOURCE = SpaceStatistics
