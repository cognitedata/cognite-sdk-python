from __future__ import annotations

from collections import UserList
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.utils._importing import local_import
from cognite.client.utils._pandas_helpers import notebook_display_with_fallback

if TYPE_CHECKING:
    import pandas as pd


@dataclass
class InstanceStatsPerSpace:
    space: str
    nodes: int
    edges: int
    soft_deleted_nodes: int
    soft_deleted_edges: int

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            space=data["space"],
            nodes=data["nodes"],
            edges=data["edges"],
            soft_deleted_nodes=data["softDeletedNodes"],
            soft_deleted_edges=data["softDeletedEdges"],
        )

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)

    def to_pandas(self) -> pd.DataFrame:
        pd = local_import("pandas")
        space = (dumped := asdict(self)).pop("space")
        return pd.Series(dumped).to_frame(name=space)


class InstanceStatsList(UserList):
    def __init__(self, items: list[InstanceStatsPerSpace]):
        super().__init__(items)

    @classmethod
    def _load(cls, data: Iterable[dict[str, Any]]) -> Self:
        return cls([InstanceStatsPerSpace._load(item) for item in data])

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)

    def to_pandas(self) -> pd.DataFrame:
        pd = local_import("pandas")
        df = pd.DataFrame([asdict(item) for item in self]).set_index("space")
        order_by_total = (df["nodes"] + df["edges"]).sort_values(ascending=False).index
        return df.loc[order_by_total]


@dataclass
class CountLimit:
    count: int
    limit: int

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(count=data["count"], limit=data["limit"])


@dataclass
class InstanceStatsAndLimits:
    nodes: int
    edges: int
    instances: int
    instances_limit: int
    soft_deleted_nodes: int
    soft_deleted_edges: int
    soft_deleted_instances: int
    soft_deleted_instances_limit: int

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            nodes=data["nodes"],
            edges=data["edges"],
            instances=data["instances"],
            instances_limit=data["instancesLimit"],
            soft_deleted_nodes=data["softDeletedNodes"],
            soft_deleted_edges=data["softDeletedEdges"],
            soft_deleted_instances=data["softDeletedInstances"],
            soft_deleted_instances_limit=data["softDeletedInstancesLimit"],
        )

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)

    def to_pandas(self) -> pd.DataFrame:
        pd = local_import("pandas")
        return pd.Series(asdict(self)).to_frame()


@dataclass
class ProjectStatsAndLimits:
    project: str
    spaces: CountLimit
    containers: CountLimit
    views: CountLimit
    data_models: CountLimit
    container_properties: CountLimit
    instances: InstanceStatsAndLimits
    concurrent_read_limit: int
    concurrent_write_limit: int
    concurrent_delete_limit: int

    @classmethod
    def _load(cls, data: dict[str, Any], project: str) -> Self:
        return cls(
            project=project,
            spaces=CountLimit._load(data["spaces"]),
            containers=CountLimit._load(data["containers"]),
            views=CountLimit._load(data["views"]),
            data_models=CountLimit._load(data["dataModels"]),
            container_properties=CountLimit._load(data["containerProperties"]),
            instances=InstanceStatsAndLimits._load(data["instances"]),
            concurrent_read_limit=data["concurrentReadLimit"],
            concurrent_write_limit=data["concurrentWriteLimit"],
            concurrent_delete_limit=data["concurrentDeleteLimit"],
        )

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)

    def to_pandas(self) -> pd.DataFrame:
        pd = local_import("pandas")
        project = (dumped := asdict(self)).pop("project")
        return pd.Series(dumped).to_frame(name=project)
