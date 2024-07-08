from __future__ import annotations

from datetime import datetime

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.typed_instances import (
    PropertyOptions,
    TypedEdge,
    TypedEdgeApply,
    TypedNode,
    TypedNodeApply,
)
from cognite.client.utils._experimental import FeaturePreviewWarning

FeaturePreviewWarning("alpha", "alpha", "Core Data Model").warn()


class SourceableProperties:
    source_id = PropertyOptions("sourceId")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Sourceable", "v1")


class SourceableApply(SourceableProperties, TypedNodeApply):
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
        super().__init__(space, external_id, existing_version, None, type)
        self.source = source
        self.source_id = source_id
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user


class Sourceable(
    SourceableProperties,
    TypedNode,
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
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.source = source
        self.source_id = source_id
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user

    def as_write(self) -> SourceableApply:
        return SourceableApply(
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


class DescribableApply(DescribableProperties, TypedNodeApply):
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
        super().__init__(space, external_id, existing_version, None, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases


class Describable(
    DescribableProperties,
    TypedNode,
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
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases

    def as_write(self) -> DescribableApply:
        return DescribableApply(
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


class SchedulableApply(SchedulableProperties, TypedNodeApply):
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
        super().__init__(space, external_id, existing_version, None, type)
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time


class Schedulable(SchedulableProperties, TypedNode):
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
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time

    def as_write(self) -> SchedulableApply:
        return SchedulableApply(
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


class Connection3DApply(Connection3DProperties, TypedEdgeApply):
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


class Connection3D(Connection3DProperties, TypedEdge):
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
            space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time, None
        )
        self.revision_id = revision_id
        self.revision_node_id = revision_node_id

    def as_write(self) -> Connection3DApply:
        return Connection3DApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            self.revision_id,
            self.revision_node_id,
            self.version,
        )


class Model3DProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Model3D", "v1")


class Model3DApply(Model3DProperties, SourceableApply, DescribableApply):
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
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        SourceableApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        DescribableApply.__init__(self, space, external_id, name, description, tags, aliases, existing_version, type)


class Model3D(Model3DProperties, Sourceable, Describable):
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
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        Sourceable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        Describable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name,
            description,
            tags,
            aliases,
            type,
            deleted_time,
        )

    def as_write(self) -> Model3DApply:
        return Model3DApply(
            self.space,
            self.external_id,
            self.source_id,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version,
            self.type,
        )


class Object3DProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Object3D", "v1")


class Object3DApply(Object3DProperties, SourceableApply, DescribableApply):
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
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        SourceableApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        DescribableApply.__init__(self, space, external_id, name, description, tags, aliases, existing_version, type)


class Object3D(Object3DProperties, Sourceable, Describable):
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
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        Sourceable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        Describable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name,
            description,
            tags,
            aliases,
            type,
            deleted_time,
        )

    def as_write(self) -> Object3DApply:
        return Object3DApply(
            self.space,
            self.external_id,
            self.source_id,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version,
            self.type,
        )


class AssetTypeProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "AssetType", "v1")


class AssetTypeApply(AssetTypeProperties, DescribableApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.code = code


class AssetType(AssetTypeProperties, Describable):
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
        code: str | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name,
            description,
            tags,
            aliases,
            type,
            deleted_time,
        )
        self.code = code

    def as_write(self) -> AssetTypeApply:
        return AssetTypeApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.code,
            self.version,
            self.type,
        )


class AssetProperties:
    last_path_materialized_time = PropertyOptions("lastPathMaterializedTime")
    asset_type = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Asset", "v1")


class AssetApply(AssetProperties, Object3DApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        parent: DirectRelationReference | tuple[str, str] | None = None,
        path: DirectRelationReference | tuple[str, str] | None = None,
        last_path_materialization_time: datetime | None = None,
        equipment: DirectRelationReference | tuple[str, str] | None = None,
        asset_type: DirectRelationReference | tuple[str, str] | None = None,
        root: DirectRelationReference | tuple[str, str] | None = None,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            name,
            description,
            tags,
            aliases,
            existing_version,
            type,
        )

        self.parent = DirectRelationReference.load(parent) if parent else None
        self.path = DirectRelationReference.load(path) if path else None
        self.last_path_materialization_time = last_path_materialization_time
        self.equipment = DirectRelationReference.load(equipment) if equipment else None
        self.asset_type = DirectRelationReference.load(asset_type) if asset_type else None
        self.root = DirectRelationReference.load(root) if root else None


class Asset(AssetProperties, Object3D):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        parent: DirectRelationReference | tuple[str, str] | None = None,
        path: DirectRelationReference | tuple[str, str] | None = None,
        last_path_materialization_time: datetime | None = None,
        equipment: DirectRelationReference | tuple[str, str] | None = None,
        asset_type: DirectRelationReference | tuple[str, str] | None = None,
        root: DirectRelationReference | tuple[str, str] | None = None,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            name,
            description,
            tags,
            aliases,
            type,
            deleted_time,
        )
        self.parent = DirectRelationReference.load(parent) if parent else None
        self.path = DirectRelationReference.load(path) if path else None
        self.last_path_materialization_time = last_path_materialization_time
        self.equipment = DirectRelationReference.load(equipment) if equipment else None
        self.asset_type = DirectRelationReference.load(asset_type) if asset_type else None
        self.root = DirectRelationReference.load(root) if root else None

    def as_write(self) -> AssetApply:
        return AssetApply(
            self.space,
            self.external_id,
            self.parent,
            self.path,
            self.last_path_materialization_time,
            self.equipment,
            self.asset_type,
            self.root,
            self.source_id,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version,
            self.type,
        )


class EquipmentProperties:
    serial_number = PropertyOptions("serialNumber")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Equipment", "v1")


class EquipmentApply(EquipmentProperties, Object3DApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        serial_number: str | None = None,
        manufacturer: str | None = None,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            name,
            description,
            tags,
            aliases,
            existing_version,
            type,
        )
        self.serial_number = serial_number
        self.manufacturer = manufacturer


class Equipment(EquipmentProperties, Object3D):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        serial_number: str | None = None,
        manufacturer: str | None = None,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            name,
            description,
            tags,
            aliases,
            type,
            deleted_time,
        )
        self.serial_number = serial_number
        self.manufacturer = manufacturer

    def as_write(self) -> EquipmentApply:
        return EquipmentApply(
            self.space,
            self.external_id,
            self.serial_number,
            self.manufacturer,
            self.source_id,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version,
            self.type,
        )


class ActivityProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "Activity", "v1")


class ActivityApply(ActivityProperties, DescribableApply, SourceableApply, SchedulableApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        assets: list[DirectRelationReference] | list[tuple[str, str]] | None = None,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        DescribableApply.__init__(self, space, external_id, name, description, tags, aliases, existing_version, type)
        SourceableApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        SchedulableApply.__init__(
            self,
            space,
            external_id,
            start_time,
            end_time,
            scheduled_start_time,
            scheduled_end_time,
            existing_version,
            type,
        )
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None


class Activity(ActivityProperties, Describable, Sourceable, Schedulable):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        assets: list[DirectRelationReference] | list[tuple[str, str]] | None = None,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        Describable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name,
            description,
            tags,
            aliases,
            type,
            deleted_time,
        )
        Sourceable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        Schedulable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            start_time,
            end_time,
            scheduled_start_time,
            scheduled_end_time,
            type,
            deleted_time,
        )
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None

    def as_write(self) -> ActivityApply:
        return ActivityApply(
            self.space,
            self.external_id,
            self.assets,
            self.source_id,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.start_time,
            self.end_time,
            self.scheduled_start_time,
            self.scheduled_end_time,
            self.version,
            self.type,
        )


class TimeSeriesProperties:
    is_step = PropertyOptions("isStep")
    is_string = PropertyOptions("isString")
    source_unit = PropertyOptions("sourceUnit")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "TimeSeriesBase", "v1")


class TimesSeriesBaseApply(TimeSeriesProperties, DescribableApply, SourceableApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        is_step: bool,
        is_string: bool,
        source_unit: str | None = None,
        unit: str | None = None,
        assets: list[DirectRelationReference] | list[tuple[str, str]] | None = None,
        equipment: DirectRelationReference | tuple[str, str] | None = None,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        DescribableApply.__init__(self, space, external_id, name, description, tags, aliases, existing_version, type)
        SourceableApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        self.is_step = is_step
        self.is_string = is_string
        self.source_unit = source_unit
        self.unit = unit
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.equipment = DirectRelationReference.load(equipment) if equipment else None


class TimeSeriesBase(TimeSeriesProperties, Describable, Sourceable):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        is_step: bool,
        is_string: bool,
        source_unit: str | None = None,
        unit: str | None = None,
        assets: list[DirectRelationReference] | list[tuple[str, str]] | None = None,
        equipment: DirectRelationReference | tuple[str, str] | None = None,
        source_id: str | None = None,
        source: str | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        Describable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name,
            description,
            tags,
            aliases,
            type,
            deleted_time,
        )
        Sourceable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        self.is_step = is_step
        self.is_string = is_string
        self.source_unit = source_unit
        self.unit = unit
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.equipment = DirectRelationReference.load(equipment) if equipment else None

    def as_write(self) -> TimesSeriesBaseApply:
        return TimesSeriesBaseApply(
            self.space,
            self.external_id,
            self.is_step,
            self.is_string,
            self.source_unit,
            self.unit,
            self.assets,
            self.equipment,
            self.source_id,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version,
            self.type,
        )
