from __future__ import annotations

from datetime import datetime

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.typed_instances import (
    PropertyOptions,
    TypedEdge,
    TypedEdgeWrite,
    TypedNode,
    TypedNodeWrite,
)


class SourceableProperties:
    source_id = PropertyOptions("sourceId")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Sourceable", "v1")


class SourceableWrite(SourceableProperties, TypedNodeWrite):
    def __init__(
        self,
        space: str,
        external_id: str,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, type)
        self.source = source
        self.source_id = source_id
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user


class Sourceable(
    SourceableProperties,
    TypedNode[SourceableWrite],
):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, type, deleted_time)
        self.source = source
        self.source_id = source_id
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user

    def as_write(self) -> SourceableWrite:
        return SourceableWrite(
            self.space,
            self.external_id,
            self.source_id,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.version,
            self.type,
        )


class DescribableProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Describable", "v1")


class DescribableWrite(DescribableProperties, TypedNodeWrite):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases


class Describable(
    DescribableProperties,
    TypedNode[DescribableWrite],
):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, type, deleted_time)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases

    def as_write(self) -> DescribableWrite:
        return DescribableWrite(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version,
            self.type,
        )


class SchedulableProperties:
    start_time = PropertyOptions("startTime")
    end_time = PropertyOptions("endTime")
    scheduled_start_time = PropertyOptions("scheduledStartTime")
    scheduled_end_time = PropertyOptions("scheduledEndTime")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Schedulable", "v1")


class SchedulableWrite(SchedulableProperties, TypedNodeWrite):
    def __init__(
        self,
        space: str,
        external_id: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, type)
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time


class Schedulable(SchedulableProperties, TypedNode[SchedulableWrite]):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, type, deleted_time)
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time

    def as_write(self) -> SchedulableWrite:
        return SchedulableWrite(
            self.space,
            self.external_id,
            self.start_time,
            self.end_time,
            self.scheduled_start_time,
            self.scheduled_end_time,
            self.version,
            self.type,
        )


class Connection3DProperties:
    revision_id = PropertyOptions("revisionId")
    revision_node_id = PropertyOptions("revisionNodeId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Connection3D", "v1")


class Connection3DWrite(Connection3DProperties, TypedEdgeWrite):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        revision_id: int,
        revision_node_id: int,
        existing_version: int | None = None,
    ) -> None:
        super().__init__(space, external_id, type, start_node, end_node, existing_version)
        self.revision_id = revision_id
        self.revision_node_id = revision_node_id


class Connection3D(Connection3DProperties, TypedEdge[Connection3DWrite]):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        version: int,
        last_updated_time: int,
        created_time: int,
        revision_id: int,
        revision_node_id: int,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space, external_id, type, start_node, end_node, version, last_updated_time, created_time, deleted_time
        )
        self.revision_id = revision_id
        self.revision_node_id = revision_node_id

    def as_write(self) -> Connection3DWrite:
        return Connection3DWrite(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            self.revision_id,
            self.revision_node_id,
            self.version,
        )
