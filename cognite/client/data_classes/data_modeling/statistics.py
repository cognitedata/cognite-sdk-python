from dataclasses import dataclass
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._text import convert_all_keys_to_camel_case


@dataclass
class InstanceStatistics(CogniteResource):
    """Statistics for instances in the data modeling API.

    Attributes:
        edges: Number of edges in the project.
        soft_deleted_edges: Number of soft-deleted edges in the project.
        nodes: Number of nodes in the project.
        soft_deleted_nodes: Number of soft-deleted nodes in the project.
        instances: Total number of instances in the project.
        instances_limit: Maximum number of instances allowed in the project.
        soft_deleted_instances: Total number of soft-deleted instances in the project.
        soft_deleted_instances_limit: Maximum number of soft-deleted instances allowed in the project.
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
    def _load(cls, resource: dict[str, Any]) -> Self:
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
class CountLimit(CogniteResource):
    """Usage and limits for a specific resource in the data modeling API.

    Attributes:
        count: The current usage count for the resource.
        limit: The maximum allowed limit for the resource.

    """

    count: int
    limit: int

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(count=resource["count"], limit=resource["limit"])


@dataclass
class SpaceStatistics(CogniteResource):
    """Statistics for a space in the data modeling API.

    Attributes:
        space: The space name
        containers: Number of containers in the space.
        views: Number of views in the space.
        data_models: Number of data models in the space.
        nodes: Number of nodes in the space.
        edges: Number of edges in the space.
        soft_deleted_nodes: Number of soft-deleted nodes in the space.
        soft_deleted_edges: Number of soft-deleted edges in the space.

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
    def _load(cls, resource: dict[str, Any]) -> Self:
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
        spaces: Usage and limits for spaces in the project
        containers: Usage and limits for containers in the project
        views: Usage and limits for views including all versions in the project
        data_models: Usage and limits for data models including all versions in the project
        container_properties: Usage and limits for sum of container properties in the project
        instances: Usage and limits for number of instances in the project
        concurrent_read_limit: Maximum number of concurrent read operations allowed in the project
        concurrent_write_limit: Maximum number of concurrent write operations allowed in the project
        concurrent_delete_limit: Maximum number of concurrent delete operations allowed in the project
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
        try:
            return self._project
        except AttributeError:
            raise AttributeError("'project' not set on this ProjectStatistics, did you instantiate it yourself?")

    @project.setter
    def project(self, value: str) -> None:
        self._project = value

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
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

    @classmethod
    def _load_with_project(cls, resource: dict[str, Any], project: str) -> Self:
        instance = cls._load(resource)
        instance.project = project
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
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
