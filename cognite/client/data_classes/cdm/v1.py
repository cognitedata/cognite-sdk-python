from __future__ import annotations

from datetime import datetime

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.typed_instances import (
    PropertyOptions,
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
