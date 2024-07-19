from __future__ import annotations

from datetime import datetime
from typing import Literal

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


class CogniteDescribableProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteDescribable", "v1")


class CogniteDescribableNodeApply(CogniteDescribableProperties, TypedNodeApply):
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


class CogniteDescribableNode(CogniteDescribableProperties, TypedNode):
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

    def as_write(self) -> CogniteDescribableNodeApply:
        return CogniteDescribableNodeApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version,
            self.type,
        )


class CogniteSourceableProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteSourceable", "v1")


class CogniteSourceableNodeApply(CogniteSourceableProperties, TypedNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, None, type)
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else None
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user


class CogniteSourceableNode(CogniteSourceableProperties, TypedNode):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.source_id = source_id
        self.source_context = source_context
        self.source = source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user

    def as_write(self) -> CogniteSourceableNodeApply:
        return CogniteSourceableNodeApply(
            self.space,
            self.external_id,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.version,
            self.type,
        )


class CogniteSchedulableProperties:
    start_time = PropertyOptions("startTime")
    end_time = PropertyOptions("endTime")
    scheduled_start_time = PropertyOptions("scheduledStartTime")
    scheduled_end_time = PropertyOptions("scheduledEndTime")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteSchedulable", "v1")


class CogniteSchedulableApply(CogniteSchedulableProperties, TypedNodeApply):
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


class CogniteSchedulable(CogniteSchedulableProperties, TypedNode):
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

    def as_write(self) -> CogniteSchedulableApply:
        return CogniteSchedulableApply(
            self.space,
            self.external_id,
            self.start_time,
            self.end_time,
            self.scheduled_start_time,
            self.scheduled_end_time,
            self.version,
            self.type,
        )


class CogniteVisualizableProperties:
    object_3_d = PropertyOptions("object3D")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteVisualizable", "v1")


class CogniteVisualizableApply(CogniteVisualizableProperties, TypedNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        object_3_d: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, None, type)
        self.object_3_d = object_3_d


class CogniteVisualizable(CogniteVisualizableProperties, TypedNode):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        object_3_d: DirectRelationReference | tuple[str, str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.object_3_d = object_3_d

    def as_write(self) -> CogniteVisualizableApply:
        return CogniteVisualizableApply(
            self.space,
            self.external_id,
            self.object_3_d,
            self.version,
            self.type,
        )


class CogniteRevision3DProperties:
    type_ = PropertyOptions("type")
    model_3_d = PropertyOptions("model3D")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteRevision3D", "v1")


class CogniteRevision3DApply(CogniteRevision3DProperties, TypedNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, None, type)
        self.status = status
        self.published = published
        self.type_ = type_
        self.model_3_d = model_3_d


class CogniteRevision3D(CogniteRevision3DProperties, TypedNode):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.status = status
        self.published = published
        self.type_ = type_
        self.model_3_d = model_3_d

    def as_write(self) -> CogniteRevision3DApply:
        return CogniteRevision3DApply(
            self.space,
            self.external_id,
            self.status,
            self.published,
            self.type_,
            self.model_3_d,
            self.version,
            self.type,
        )


class CogniteCubeMapProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteCubeMap", "v1")


class CogniteCubeMapApply(CogniteCubeMapProperties, TypedNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        front: DirectRelationReference | tuple[str, str] | None = None,
        back: DirectRelationReference | tuple[str, str] | None = None,
        left: DirectRelationReference | tuple[str, str] | None = None,
        right: DirectRelationReference | tuple[str, str] | None = None,
        top: DirectRelationReference | tuple[str, str] | None = None,
        bottom: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, None, type)
        self.front = front
        self.back = back
        self.left = left
        self.right = right
        self.top = top
        self.bottom = bottom


class CogniteCubeMap(CogniteCubeMapProperties, TypedNode):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        front: DirectRelationReference | tuple[str, str] | None = None,
        back: DirectRelationReference | tuple[str, str] | None = None,
        left: DirectRelationReference | tuple[str, str] | None = None,
        right: DirectRelationReference | tuple[str, str] | None = None,
        top: DirectRelationReference | tuple[str, str] | None = None,
        bottom: DirectRelationReference | tuple[str, str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.front = front
        self.back = back
        self.left = left
        self.right = right
        self.top = top
        self.bottom = bottom

    def as_write(self) -> CogniteCubeMapApply:
        return CogniteCubeMapApply(
            self.space,
            self.external_id,
            self.front,
            self.back,
            self.left,
            self.right,
            self.top,
            self.bottom,
            self.version,
            self.type,
        )


class CogniteTransformation3DProperties:
    translation_x = PropertyOptions("translationX")
    translation_y = PropertyOptions("translationY")
    translation_z = PropertyOptions("translationZ")
    euler_rotation_x = PropertyOptions("eulerRotationX")
    euler_rotation_y = PropertyOptions("eulerRotationY")
    euler_rotation_z = PropertyOptions("eulerRotationZ")
    scale_x = PropertyOptions("scaleX")
    scale_y = PropertyOptions("scaleY")
    scale_z = PropertyOptions("scaleZ")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteTransformation3D", "v1")


class CogniteTransformation3DNodeApply(CogniteTransformation3DProperties, TypedNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        translation_x: float | None = None,
        translation_y: float | None = None,
        translation_z: float | None = None,
        euler_rotation_x: float | None = None,
        euler_rotation_y: float | None = None,
        euler_rotation_z: float | None = None,
        scale_x: float | None = None,
        scale_y: float | None = None,
        scale_z: float | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, existing_version, None, type)
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z


class CogniteTransformation3DNode(CogniteTransformation3DProperties, TypedNode):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        translation_x: float | None = None,
        translation_y: float | None = None,
        translation_z: float | None = None,
        euler_rotation_x: float | None = None,
        euler_rotation_y: float | None = None,
        euler_rotation_z: float | None = None,
        scale_x: float | None = None,
        scale_y: float | None = None,
        scale_z: float | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z

    def as_write(self) -> CogniteTransformation3DNodeApply:
        return CogniteTransformation3DNodeApply(
            self.space,
            self.external_id,
            self.translation_x,
            self.translation_y,
            self.translation_z,
            self.euler_rotation_x,
            self.euler_rotation_y,
            self.euler_rotation_z,
            self.scale_x,
            self.scale_y,
            self.scale_z,
            self.version,
            self.type,
        )


class CogniteAssetClassProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteAssetClass", "v1")


class CogniteAssetClassApply(CogniteAssetClassProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        standard: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.code = code
        self.standard = standard


class CogniteAssetClass(CogniteAssetClassProperties, CogniteDescribableNode):
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
        standard: str | None = None,
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
        self.standard = standard

    def as_write(self) -> CogniteAssetClassApply:
        return CogniteAssetClassApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.code,
            self.standard,
            self.version,
            self.type,
        )


class CogniteAssetTypeProperties:
    asset_class = PropertyOptions("assetClass")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteAssetType", "v1")


class CogniteAssetTypeApply(CogniteAssetTypeProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        asset_class: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.code = code
        self.asset_class = asset_class


class CogniteAssetType(CogniteAssetTypeProperties, CogniteDescribableNode):
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
        asset_class: DirectRelationReference | tuple[str, str] | None = None,
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
        self.asset_class = asset_class

    def as_write(self) -> CogniteAssetTypeApply:
        return CogniteAssetTypeApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.code,
            self.asset_class,
            self.version,
            self.type,
        )


class CogniteCADNodeProperties:
    object_3_d = PropertyOptions("object3D")
    model_3_d = PropertyOptions("model3D")
    cad_node_reference = PropertyOptions("cadNodeReference")
    tree_indexes = PropertyOptions("treeIndexes")
    sub_tree_sizes = PropertyOptions("subTreeSizes")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteCADNode", "v1")


class CogniteCADNodeApply(CogniteCADNodeProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        object_3_d: DirectRelationReference | tuple[str, str] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        cad_node_reference: str | None = None,
        revisions: list[DirectRelationReference | tuple[str, str]] | None = None,
        tree_indexes: list[int] | None = None,
        sub_tree_sizes: list[int] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.object_3_d = object_3_d
        self.model_3_d = model_3_d
        self.cad_node_reference = cad_node_reference
        self.revisions = revisions
        self.tree_indexes = tree_indexes
        self.sub_tree_sizes = sub_tree_sizes


class CogniteCADNode(CogniteCADNodeProperties, CogniteDescribableNode):
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
        object_3_d: DirectRelationReference | tuple[str, str] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        cad_node_reference: str | None = None,
        revisions: list[DirectRelationReference | tuple[str, str]] | None = None,
        tree_indexes: list[int] | None = None,
        sub_tree_sizes: list[int] | None = None,
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
        self.object_3_d = object_3_d
        self.model_3_d = model_3_d
        self.cad_node_reference = cad_node_reference
        self.revisions = revisions
        self.tree_indexes = tree_indexes
        self.sub_tree_sizes = sub_tree_sizes

    def as_write(self) -> CogniteCADNodeApply:
        return CogniteCADNodeApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.object_3_d,
            self.model_3_d,
            self.cad_node_reference,
            self.revisions,
            self.tree_indexes,
            self.sub_tree_sizes,
            self.version,
            self.type,
        )


class CogniteEquipmentTypeProperties:
    equipment_class = PropertyOptions("equipmentClass")
    standard_reference = PropertyOptions("standardReference")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteEquipmentType", "v1")


class CogniteEquipmentTypeApply(CogniteEquipmentTypeProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        equipment_class: str | None = None,
        standard: str | None = None,
        standard_reference: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.code = code
        self.equipment_class = equipment_class
        self.standard = standard
        self.standard_reference = standard_reference


class CogniteEquipmentType(CogniteEquipmentTypeProperties, CogniteDescribableNode):
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
        equipment_class: str | None = None,
        standard: str | None = None,
        standard_reference: str | None = None,
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
        self.equipment_class = equipment_class
        self.standard = standard
        self.standard_reference = standard_reference

    def as_write(self) -> CogniteEquipmentTypeApply:
        return CogniteEquipmentTypeApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.code,
            self.equipment_class,
            self.standard,
            self.standard_reference,
            self.version,
            self.type,
        )


class CogniteFileCategoryProperties:
    standard_reference = PropertyOptions("standardReference")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteFileCategory", "v1")


class CogniteFileCategoryApply(CogniteFileCategoryProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        code: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        standard: str | None = None,
        standard_reference: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.code = code
        self.standard = standard
        self.standard_reference = standard_reference


class CogniteFileCategory(CogniteFileCategoryProperties, CogniteDescribableNode):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        code: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        standard: str | None = None,
        standard_reference: str | None = None,
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
        self.standard = standard
        self.standard_reference = standard_reference

    def as_write(self) -> CogniteFileCategoryApply:
        return CogniteFileCategoryApply(
            self.space,
            self.external_id,
            self.code,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.standard,
            self.standard_reference,
            self.version,
            self.type,
        )


class CogniteImage360StationProperties:
    group_type = PropertyOptions("groupType")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360Station", "v1")


class CogniteImage360StationApply(CogniteImage360StationProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        group_type: Literal["Station360"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.group_type = group_type


class CogniteImage360Station(CogniteImage360StationProperties, CogniteDescribableNode):
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
        group_type: Literal["Station360"] | None = None,
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
        self.group_type = group_type

    def as_write(self) -> CogniteImage360StationApply:
        return CogniteImage360StationApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.group_type,
            self.version,
            self.type,
        )


class CogniteModel3DProperties:
    type_ = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteModel3D", "v1")


class CogniteModel3DApply(CogniteModel3DProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.type_ = type_


class CogniteModel3D(CogniteModel3DProperties, CogniteDescribableNode):
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
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
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
        self.type_ = type_

    def as_write(self) -> CogniteModel3DApply:
        return CogniteModel3DApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.type_,
            self.version,
            self.type,
        )


class CogniteObject3DProperties:
    x_min = PropertyOptions("xMin")
    x_max = PropertyOptions("xMax")
    y_min = PropertyOptions("yMin")
    y_max = PropertyOptions("yMax")
    z_min = PropertyOptions("zMin")
    z_max = PropertyOptions("zMax")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteObject3D", "v1")


class CogniteObject3DApply(CogniteObject3DProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        x_min: float | None = None,
        x_max: float | None = None,
        y_min: float | None = None,
        y_max: float | None = None,
        z_min: float | None = None,
        z_max: float | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max
        self.z_min = z_min
        self.z_max = z_max


class CogniteObject3D(CogniteObject3DProperties, CogniteDescribableNode):
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
        x_min: float | None = None,
        x_max: float | None = None,
        y_min: float | None = None,
        y_max: float | None = None,
        z_min: float | None = None,
        z_max: float | None = None,
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
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max
        self.z_min = z_min
        self.z_max = z_max

    def as_write(self) -> CogniteObject3DApply:
        return CogniteObject3DApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.x_min,
            self.x_max,
            self.y_min,
            self.y_max,
            self.z_min,
            self.z_max,
            self.version,
            self.type,
        )


class CognitePointCloudVolumeProperties:
    object_3_d = PropertyOptions("object3D")
    model_3_d = PropertyOptions("model3D")
    volume_references = PropertyOptions("volumeReferences")
    volume_type = PropertyOptions("volumeType")
    format_version = PropertyOptions("formatVersion")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CognitePointCloudVolume", "v1")


class CognitePointCloudVolumeApply(CognitePointCloudVolumeProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        object_3_d: DirectRelationReference | tuple[str, str] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        volume_references: list[str] | None = None,
        revisions: list[DirectRelationReference | tuple[str, str]] | None = None,
        volume_type: Literal["Box", "Cylinder"] | None = None,
        volume: list[float] | None = None,
        format_version: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.object_3_d = object_3_d
        self.model_3_d = model_3_d
        self.volume_references = volume_references
        self.revisions = revisions
        self.volume_type = volume_type
        self.volume = volume
        self.format_version = format_version


class CognitePointCloudVolume(CognitePointCloudVolumeProperties, CogniteDescribableNode):
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
        object_3_d: DirectRelationReference | tuple[str, str] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        volume_references: list[str] | None = None,
        revisions: list[DirectRelationReference | tuple[str, str]] | None = None,
        volume_type: Literal["Box", "Cylinder"] | None = None,
        volume: list[float] | None = None,
        format_version: str | None = None,
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
        self.object_3_d = object_3_d
        self.model_3_d = model_3_d
        self.volume_references = volume_references
        self.revisions = revisions
        self.volume_type = volume_type
        self.volume = volume
        self.format_version = format_version

    def as_write(self) -> CognitePointCloudVolumeApply:
        return CognitePointCloudVolumeApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.object_3_d,
            self.model_3_d,
            self.volume_references,
            self.revisions,
            self.volume_type,
            self.volume,
            self.format_version,
            self.version,
            self.type,
        )


class CogniteSourceSystemProperties:
    version_ = PropertyOptions("version")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteSourceSystem", "v1")


class CogniteSourceSystemApply(CogniteSourceSystemProperties, CogniteDescribableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        version_: str | None = None,
        manufacturer: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, existing_version, type)
        self.version_ = version_
        self.manufacturer = manufacturer


class CogniteSourceSystem(CogniteSourceSystemProperties, CogniteDescribableNode):
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
        version_: str | None = None,
        manufacturer: str | None = None,
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
        self.version_ = version_
        self.manufacturer = manufacturer

    def as_write(self) -> CogniteSourceSystemApply:
        return CogniteSourceSystemApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version_,
            self.manufacturer,
            self.version,
            self.type,
        )


class CogniteEquipmentProperties:
    serial_number = PropertyOptions("serialNumber")
    equipment_type = PropertyOptions("equipmentType")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteEquipment", "v1")


class CogniteEquipmentApply(CogniteEquipmentProperties, CogniteDescribableNodeApply, CogniteSourceableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        serial_number: str | None = None,
        manufacturer: str | None = None,
        equipment_type: DirectRelationReference | tuple[str, str] | None = None,
        files: list[DirectRelationReference | tuple[str, str]] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        CogniteDescribableNodeApply.__init__(
            self, space, external_id, name, description, tags, aliases, existing_version, type
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        self.serial_number = serial_number
        self.manufacturer = manufacturer
        self.equipment_type = equipment_type
        self.files = files


class CogniteEquipment(CogniteEquipmentProperties, CogniteDescribableNode, CogniteSourceableNode):
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
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        serial_number: str | None = None,
        manufacturer: str | None = None,
        equipment_type: DirectRelationReference | tuple[str, str] | None = None,
        files: list[DirectRelationReference | tuple[str, str]] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
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
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        self.serial_number = serial_number
        self.manufacturer = manufacturer
        self.equipment_type = equipment_type
        self.files = files

    def as_write(self) -> CogniteEquipmentApply:
        return CogniteEquipmentApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.serial_number,
            self.manufacturer,
            self.equipment_type,
            self.files,
            self.version,
            self.type,
        )


class CogniteFileProperties:
    mime_type = PropertyOptions("mimeType")
    is_uploaded = PropertyOptions("isUploaded")
    uploaded_time = PropertyOptions("uploadedTime")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteFile", "v1")


class CogniteFileApply(CogniteFileProperties, CogniteDescribableNodeApply, CogniteSourceableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        assets: list[DirectRelationReference | tuple[str, str]] | None = None,
        mime_type: str | None = None,
        directory: str | None = None,
        is_uploaded: bool | None = None,
        uploaded_time: datetime | None = None,
        category: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        CogniteDescribableNodeApply.__init__(
            self, space, external_id, name, description, tags, aliases, existing_version, type
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        self.assets = assets
        self.mime_type = mime_type
        self.directory = directory
        self.is_uploaded = is_uploaded
        self.uploaded_time = uploaded_time
        self.category = category


class CogniteFile(CogniteFileProperties, CogniteDescribableNode, CogniteSourceableNode):
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
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        assets: list[DirectRelationReference | tuple[str, str]] | None = None,
        mime_type: str | None = None,
        directory: str | None = None,
        is_uploaded: bool | None = None,
        uploaded_time: datetime | None = None,
        category: DirectRelationReference | tuple[str, str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
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
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        self.assets = assets
        self.mime_type = mime_type
        self.directory = directory
        self.is_uploaded = is_uploaded
        self.uploaded_time = uploaded_time
        self.category = category

    def as_write(self) -> CogniteFileApply:
        return CogniteFileApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.assets,
            self.mime_type,
            self.directory,
            self.is_uploaded,
            self.uploaded_time,
            self.category,
            self.version,
            self.type,
        )


class CogniteTimeSeriesProperties:
    type_ = PropertyOptions("type")
    is_step = PropertyOptions("isStep")
    source_unit = PropertyOptions("sourceUnit")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteTimeSeries", "v1")


class CogniteTimeSeriesApply(CogniteTimeSeriesProperties, CogniteDescribableNodeApply, CogniteSourceableNodeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        type_: Literal["numeric", "string"],
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        is_step: bool | None = None,
        source_unit: str | None = None,
        unit: DirectRelationReference | tuple[str, str] | None = None,
        assets: list[DirectRelationReference | tuple[str, str]] | None = None,
        equipment: list[DirectRelationReference | tuple[str, str]] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        CogniteDescribableNodeApply.__init__(
            self, space, external_id, name, description, tags, aliases, existing_version, type
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        self.type_ = type_
        self.is_step = is_step
        self.source_unit = source_unit
        self.unit = unit
        self.assets = assets
        self.equipment = equipment


class CogniteTimeSeries(CogniteTimeSeriesProperties, CogniteDescribableNode, CogniteSourceableNode):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        type_: Literal["numeric", "string"],
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        is_step: bool | None = None,
        source_unit: str | None = None,
        unit: DirectRelationReference | tuple[str, str] | None = None,
        assets: list[DirectRelationReference | tuple[str, str]] | None = None,
        equipment: list[DirectRelationReference | tuple[str, str]] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
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
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        self.type_ = type_
        self.is_step = is_step
        self.source_unit = source_unit
        self.unit = unit
        self.assets = assets
        self.equipment = equipment

    def as_write(self) -> CogniteTimeSeriesApply:
        return CogniteTimeSeriesApply(
            self.space,
            self.external_id,
            self.type_,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.is_step,
            self.source_unit,
            self.unit,
            self.assets,
            self.equipment,
            self.version,
            self.type,
        )


class CogniteActivityProperties:
    time_series = PropertyOptions("timeSeries")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteActivity", "v1")


class CogniteActivityApply(
    CogniteActivityProperties, CogniteDescribableNodeApply, CogniteSourceableNodeApply, CogniteSchedulableApply
):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        assets: list[DirectRelationReference | tuple[str, str]] | None = None,
        equipment: list[DirectRelationReference | tuple[str, str]] | None = None,
        time_series: list[DirectRelationReference | tuple[str, str]] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        CogniteDescribableNodeApply.__init__(
            self, space, external_id, name, description, tags, aliases, existing_version, type
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        CogniteSchedulableApply.__init__(
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
        self.assets = assets
        self.equipment = equipment
        self.time_series = time_series


class CogniteActivity(CogniteActivityProperties, CogniteDescribableNode, CogniteSourceableNode, CogniteSchedulable):
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
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        assets: list[DirectRelationReference | tuple[str, str]] | None = None,
        equipment: list[DirectRelationReference | tuple[str, str]] | None = None,
        time_series: list[DirectRelationReference | tuple[str, str]] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
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
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        CogniteSchedulable.__init__(
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
        self.assets = assets
        self.equipment = equipment
        self.time_series = time_series

    def as_write(self) -> CogniteActivityApply:
        return CogniteActivityApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.start_time,
            self.end_time,
            self.scheduled_start_time,
            self.scheduled_end_time,
            self.assets,
            self.equipment,
            self.time_series,
            self.version,
            self.type,
        )


class CogniteAssetProperties:
    last_path_materialization_time = PropertyOptions("lastPathMaterializationTime")
    asset_class = PropertyOptions("assetClass")
    type_ = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteAsset", "v1")


class CogniteAssetApply(
    CogniteAssetProperties, CogniteVisualizableApply, CogniteDescribableNodeApply, CogniteSourceableNodeApply
):
    def __init__(
        self,
        space: str,
        external_id: str,
        object_3_d: DirectRelationReference | tuple[str, str] | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        parent: DirectRelationReference | tuple[str, str] | None = None,
        root: DirectRelationReference | tuple[str, str] | None = None,
        path: list[DirectRelationReference | tuple[str, str]] | None = None,
        last_path_materialization_time: datetime | None = None,
        equipment: DirectRelationReference | tuple[str, str] | None = None,
        asset_class: DirectRelationReference | tuple[str, str] | None = None,
        type_: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        CogniteVisualizableApply.__init__(self, space, external_id, object_3_d, existing_version, type)
        CogniteDescribableNodeApply.__init__(
            self, space, external_id, name, description, tags, aliases, existing_version, type
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
            type,
        )
        self.parent = parent
        self.root = root
        self.path = path
        self.last_path_materialization_time = last_path_materialization_time
        self.equipment = equipment
        self.asset_class = asset_class
        self.type_ = type_


class CogniteAsset(CogniteAssetProperties, CogniteVisualizable, CogniteDescribableNode, CogniteSourceableNode):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        object_3_d: DirectRelationReference | tuple[str, str] | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        parent: DirectRelationReference | tuple[str, str] | None = None,
        root: DirectRelationReference | tuple[str, str] | None = None,
        path: list[DirectRelationReference | tuple[str, str]] | None = None,
        last_path_materialization_time: datetime | None = None,
        equipment: DirectRelationReference | tuple[str, str] | None = None,
        asset_class: DirectRelationReference | tuple[str, str] | None = None,
        type_: DirectRelationReference | tuple[str, str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteVisualizable.__init__(
            self, space, external_id, version, last_updated_time, created_time, object_3_d, type, deleted_time
        )
        CogniteDescribableNode.__init__(
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
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            type,
            deleted_time,
        )
        self.parent = parent
        self.root = root
        self.path = path
        self.last_path_materialization_time = last_path_materialization_time
        self.equipment = equipment
        self.asset_class = asset_class
        self.type_ = type_

    def as_write(self) -> CogniteAssetApply:
        return CogniteAssetApply(
            self.space,
            self.external_id,
            self.object_3_d,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.parent,
            self.root,
            self.path,
            self.last_path_materialization_time,
            self.equipment,
            self.asset_class,
            self.type_,
            self.version,
            self.type,
        )


class CogniteCADRevisionProperties:
    revision_id = PropertyOptions("revisionId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteCADRevision", "v1")


class CogniteCADRevisionApply(CogniteCADRevisionProperties, CogniteRevision3DApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        revision_id: int | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, status, published, type_, model_3_d, existing_version, type)
        self.revision_id = revision_id


class CogniteCADRevision(CogniteCADRevisionProperties, CogniteRevision3D):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        revision_id: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            status,
            published,
            type_,
            model_3_d,
            type,
            deleted_time,
        )
        self.revision_id = revision_id

    def as_write(self) -> CogniteCADRevisionApply:
        return CogniteCADRevisionApply(
            self.space,
            self.external_id,
            self.status,
            self.published,
            self.type_,
            self.model_3_d,
            self.revision_id,
            self.version,
            self.type,
        )


class CogniteImage360CollectionProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360Collection", "v1")


class CogniteImage360CollectionApply(
    CogniteImage360CollectionProperties, CogniteDescribableNodeApply, CogniteRevision3DApply
):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        CogniteDescribableNodeApply.__init__(
            self, space, external_id, name, description, tags, aliases, existing_version, type
        )
        CogniteRevision3DApply.__init__(
            self, space, external_id, status, published, type_, model_3_d, existing_version, type
        )


class CogniteImage360Collection(CogniteImage360CollectionProperties, CogniteDescribableNode, CogniteRevision3D):
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
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
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
        CogniteRevision3D.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            status,
            published,
            type_,
            model_3_d,
            type,
            deleted_time,
        )

    def as_write(self) -> CogniteImage360CollectionApply:
        return CogniteImage360CollectionApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.status,
            self.published,
            self.type_,
            self.model_3_d,
            self.version,
            self.type,
        )


class CognitePointCloudRevisionProperties:
    revision_id = PropertyOptions("revisionId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CognitePointCloudRevision", "v1")


class CognitePointCloudRevisionApply(CognitePointCloudRevisionProperties, CogniteRevision3DApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        revision_id: int | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, status, published, type_, model_3_d, existing_version, type)
        self.revision_id = revision_id


class CognitePointCloudRevision(CognitePointCloudRevisionProperties, CogniteRevision3D):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3_d: DirectRelationReference | tuple[str, str] | None = None,
        revision_id: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            status,
            published,
            type_,
            model_3_d,
            type,
            deleted_time,
        )
        self.revision_id = revision_id

    def as_write(self) -> CognitePointCloudRevisionApply:
        return CognitePointCloudRevisionApply(
            self.space,
            self.external_id,
            self.status,
            self.published,
            self.type_,
            self.model_3_d,
            self.revision_id,
            self.version,
            self.type,
        )


class CogniteImage360Properties:
    collection_360 = PropertyOptions("collection360")
    station_360 = PropertyOptions("station360")
    taken_at = PropertyOptions("takenAt")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360", "v1")


class CogniteImage360Apply(CogniteImage360Properties, CogniteTransformation3DNodeApply, CogniteCubeMapApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        translation_x: float | None = None,
        translation_y: float | None = None,
        translation_z: float | None = None,
        euler_rotation_x: float | None = None,
        euler_rotation_y: float | None = None,
        euler_rotation_z: float | None = None,
        scale_x: float | None = None,
        scale_y: float | None = None,
        scale_z: float | None = None,
        front: DirectRelationReference | tuple[str, str] | None = None,
        back: DirectRelationReference | tuple[str, str] | None = None,
        left: DirectRelationReference | tuple[str, str] | None = None,
        right: DirectRelationReference | tuple[str, str] | None = None,
        top: DirectRelationReference | tuple[str, str] | None = None,
        bottom: DirectRelationReference | tuple[str, str] | None = None,
        collection_360: DirectRelationReference | tuple[str, str] | None = None,
        station_360: DirectRelationReference | tuple[str, str] | None = None,
        taken_at: datetime | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        CogniteTransformation3DNodeApply.__init__(
            self,
            space,
            external_id,
            translation_x,
            translation_y,
            translation_z,
            euler_rotation_x,
            euler_rotation_y,
            euler_rotation_z,
            scale_x,
            scale_y,
            scale_z,
            existing_version,
            type,
        )
        CogniteCubeMapApply.__init__(
            self, space, external_id, front, back, left, right, top, bottom, existing_version, type
        )
        self.collection_360 = collection_360
        self.station_360 = station_360
        self.taken_at = taken_at


class CogniteImage360(CogniteImage360Properties, CogniteTransformation3DNode, CogniteCubeMap):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        translation_x: float | None = None,
        translation_y: float | None = None,
        translation_z: float | None = None,
        euler_rotation_x: float | None = None,
        euler_rotation_y: float | None = None,
        euler_rotation_z: float | None = None,
        scale_x: float | None = None,
        scale_y: float | None = None,
        scale_z: float | None = None,
        front: DirectRelationReference | tuple[str, str] | None = None,
        back: DirectRelationReference | tuple[str, str] | None = None,
        left: DirectRelationReference | tuple[str, str] | None = None,
        right: DirectRelationReference | tuple[str, str] | None = None,
        top: DirectRelationReference | tuple[str, str] | None = None,
        bottom: DirectRelationReference | tuple[str, str] | None = None,
        collection_360: DirectRelationReference | tuple[str, str] | None = None,
        station_360: DirectRelationReference | tuple[str, str] | None = None,
        taken_at: datetime | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteTransformation3DNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            translation_x,
            translation_y,
            translation_z,
            euler_rotation_x,
            euler_rotation_y,
            euler_rotation_z,
            scale_x,
            scale_y,
            scale_z,
            type,
            deleted_time,
        )
        CogniteCubeMap.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            front,
            back,
            left,
            right,
            top,
            bottom,
            type,
            deleted_time,
        )
        self.collection_360 = collection_360
        self.station_360 = station_360
        self.taken_at = taken_at

    def as_write(self) -> CogniteImage360Apply:
        return CogniteImage360Apply(
            self.space,
            self.external_id,
            self.translation_x,
            self.translation_y,
            self.translation_z,
            self.euler_rotation_x,
            self.euler_rotation_y,
            self.euler_rotation_z,
            self.scale_x,
            self.scale_y,
            self.scale_z,
            self.front,
            self.back,
            self.left,
            self.right,
            self.top,
            self.bottom,
            self.collection_360,
            self.station_360,
            self.taken_at,
            self.version,
            self.type,
        )


class CogniteCADModelProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteCADModel", "v1")


class CogniteCADModelApply(CogniteCADModelProperties, CogniteModel3DApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, type_, existing_version, type)


class CogniteCADModel(CogniteCADModelProperties, CogniteModel3D):
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
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
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
            type_,
            type,
            deleted_time,
        )

    def as_write(self) -> CogniteCADModelApply:
        return CogniteCADModelApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.type_,
            self.version,
            self.type,
        )


class CogniteImage360ModelProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360Model", "v1")


class CogniteImage360ModelApply(CogniteImage360ModelProperties, CogniteModel3DApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, type_, existing_version, type)


class CogniteImage360Model(CogniteImage360ModelProperties, CogniteModel3D):
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
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
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
            type_,
            type,
            deleted_time,
        )

    def as_write(self) -> CogniteImage360ModelApply:
        return CogniteImage360ModelApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.type_,
            self.version,
            self.type,
        )


class CognitePointCloudModelProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CognitePointCloudModel", "v1")


class CognitePointCloudModelApply(CognitePointCloudModelProperties, CogniteModel3DApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space, external_id, name, description, tags, aliases, type_, existing_version, type)


class CognitePointCloudModel(CognitePointCloudModelProperties, CogniteModel3D):
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
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
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
            type_,
            type,
            deleted_time,
        )

    def as_write(self) -> CognitePointCloudModelApply:
        return CognitePointCloudModelApply(
            self.space,
            self.external_id,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.type_,
            self.version,
            self.type,
        )


class CogniteSourceableEdgeApply(CogniteSourceableProperties, TypedEdgeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        existing_version: int | None = None,
    ) -> None:
        super().__init__(space, external_id, type, start_node, end_node, existing_version)
        self.source_id = source_id
        self.source_context = source_context
        self.source = source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user


class CogniteSourceableEdge(CogniteSourceableProperties, TypedEdge):
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
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time, None
        )
        self.source_id = source_id
        self.source_context = source_context
        self.source = source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user

    def as_write(self) -> CogniteSourceableEdgeApply:
        return CogniteSourceableEdgeApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.version,
        )


class CogniteDescribableEdgeApply(CogniteDescribableProperties, TypedEdgeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
    ) -> None:
        super().__init__(space, external_id, type, start_node, end_node, existing_version)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases


class CogniteDescribableEdge(CogniteDescribableProperties, TypedEdge):
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
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time, None
        )
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases

    def as_write(self) -> CogniteDescribableEdgeApply:
        return CogniteDescribableEdgeApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.version,
        )


class CogniteTransformation3DEdgeApply(CogniteTransformation3DProperties, TypedEdgeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        translation_x: float | None = None,
        translation_y: float | None = None,
        translation_z: float | None = None,
        euler_rotation_x: float | None = None,
        euler_rotation_y: float | None = None,
        euler_rotation_z: float | None = None,
        scale_x: float | None = None,
        scale_y: float | None = None,
        scale_z: float | None = None,
        existing_version: int | None = None,
    ) -> None:
        super().__init__(space, external_id, type, start_node, end_node, existing_version)
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z


class CogniteTransformation3DEdge(CogniteTransformation3DProperties, TypedEdge):
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
        translation_x: float | None = None,
        translation_y: float | None = None,
        translation_z: float | None = None,
        euler_rotation_x: float | None = None,
        euler_rotation_y: float | None = None,
        euler_rotation_z: float | None = None,
        scale_x: float | None = None,
        scale_y: float | None = None,
        scale_z: float | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time, None
        )
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z

    def as_write(self) -> CogniteTransformation3DEdgeApply:
        return CogniteTransformation3DEdgeApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            self.translation_x,
            self.translation_y,
            self.translation_z,
            self.euler_rotation_x,
            self.euler_rotation_y,
            self.euler_rotation_z,
            self.scale_x,
            self.scale_y,
            self.scale_z,
            self.version,
        )


class CogniteAnnotationProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteAnnotation", "v1")


class CogniteAnnotationApply(CogniteAnnotationProperties, CogniteDescribableEdgeApply, CogniteSourceableEdgeApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        confidence: float | None = None,
        status: Literal["Approved", "Rejected", "Suggested"] | None = None,
        existing_version: int | None = None,
    ) -> None:
        CogniteDescribableEdgeApply.__init__(
            self, space, external_id, type, start_node, end_node, name, description, tags, aliases, existing_version
        )
        CogniteSourceableEdgeApply.__init__(
            self,
            space,
            external_id,
            type,
            start_node,
            end_node,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            existing_version,
        )
        self.confidence = confidence
        self.status = status


class CogniteAnnotation(CogniteAnnotationProperties, CogniteDescribableEdge, CogniteSourceableEdge):
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
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        confidence: float | None = None,
        status: Literal["Approved", "Rejected", "Suggested"] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableEdge.__init__(
            self,
            space,
            external_id,
            type,
            start_node,
            end_node,
            version,
            last_updated_time,
            created_time,
            name,
            description,
            tags,
            aliases,
            deleted_time,
        )
        CogniteSourceableEdge.__init__(
            self,
            space,
            external_id,
            type,
            start_node,
            end_node,
            version,
            last_updated_time,
            created_time,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            deleted_time,
        )
        self.confidence = confidence
        self.status = status

    def as_write(self) -> CogniteAnnotationApply:
        return CogniteAnnotationApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.confidence,
            self.status,
            self.version,
        )


class CogniteDiagramAnnotationProperties:
    start_node_page_number = PropertyOptions("startNodePageNumber")
    end_node_page_number = PropertyOptions("endNodePageNumber")
    start_node_x_min = PropertyOptions("startNodeXMin")
    start_node_x_max = PropertyOptions("startNodeXMax")
    start_node_y_min = PropertyOptions("startNodeYMin")
    start_node_y_max = PropertyOptions("startNodeYMax")
    start_node_text = PropertyOptions("startNodeText")
    end_node_x_min = PropertyOptions("endNodeXMin")
    end_node_x_max = PropertyOptions("endNodeXMax")
    end_node_y_min = PropertyOptions("endNodeYMin")
    end_node_y_max = PropertyOptions("endNodeYMax")
    end_node_text = PropertyOptions("endNodeText")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteDiagramAnnotation", "v1")


class CogniteDiagramAnnotationApply(CogniteDiagramAnnotationProperties, CogniteAnnotationApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        confidence: float | None = None,
        status: Literal["Approved", "Rejected", "Suggested"] | None = None,
        start_node_page_number: int | None = None,
        end_node_page_number: int | None = None,
        start_node_x_min: float | None = None,
        start_node_x_max: float | None = None,
        start_node_y_min: float | None = None,
        start_node_y_max: float | None = None,
        start_node_text: str | None = None,
        end_node_x_min: float | None = None,
        end_node_x_max: float | None = None,
        end_node_y_min: float | None = None,
        end_node_y_max: float | None = None,
        end_node_text: str | None = None,
        existing_version: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            type,
            start_node,
            end_node,
            name,
            description,
            tags,
            aliases,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            confidence,
            status,
            existing_version,
        )
        self.start_node_page_number = start_node_page_number
        self.end_node_page_number = end_node_page_number
        self.start_node_x_min = start_node_x_min
        self.start_node_x_max = start_node_x_max
        self.start_node_y_min = start_node_y_min
        self.start_node_y_max = start_node_y_max
        self.start_node_text = start_node_text
        self.end_node_x_min = end_node_x_min
        self.end_node_x_max = end_node_x_max
        self.end_node_y_min = end_node_y_min
        self.end_node_y_max = end_node_y_max
        self.end_node_text = end_node_text


class CogniteDiagramAnnotation(CogniteDiagramAnnotationProperties, CogniteAnnotation):
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
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        confidence: float | None = None,
        status: Literal["Approved", "Rejected", "Suggested"] | None = None,
        start_node_page_number: int | None = None,
        end_node_page_number: int | None = None,
        start_node_x_min: float | None = None,
        start_node_x_max: float | None = None,
        start_node_y_min: float | None = None,
        start_node_y_max: float | None = None,
        start_node_text: str | None = None,
        end_node_x_min: float | None = None,
        end_node_x_max: float | None = None,
        end_node_y_min: float | None = None,
        end_node_y_max: float | None = None,
        end_node_text: str | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            type,
            start_node,
            end_node,
            version,
            last_updated_time,
            created_time,
            name,
            description,
            tags,
            aliases,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            confidence,
            status,
            deleted_time,
        )
        self.start_node_page_number = start_node_page_number
        self.end_node_page_number = end_node_page_number
        self.start_node_x_min = start_node_x_min
        self.start_node_x_max = start_node_x_max
        self.start_node_y_min = start_node_y_min
        self.start_node_y_max = start_node_y_max
        self.start_node_text = start_node_text
        self.end_node_x_min = end_node_x_min
        self.end_node_x_max = end_node_x_max
        self.end_node_y_min = end_node_y_min
        self.end_node_y_max = end_node_y_max
        self.end_node_text = end_node_text

    def as_write(self) -> CogniteDiagramAnnotationApply:
        return CogniteDiagramAnnotationApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.confidence,
            self.status,
            self.start_node_page_number,
            self.end_node_page_number,
            self.start_node_x_min,
            self.start_node_x_max,
            self.start_node_y_min,
            self.start_node_y_max,
            self.start_node_text,
            self.end_node_x_min,
            self.end_node_x_max,
            self.end_node_y_min,
            self.end_node_y_max,
            self.end_node_text,
            self.version,
        )


class CogniteImage360AnnotationProperties:
    format_version = PropertyOptions("formatVersion")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360Annotation", "v1")


class CogniteImage360AnnotationApply(CogniteImage360AnnotationProperties, CogniteAnnotationApply):
    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        confidence: float | None = None,
        status: Literal["Approved", "Rejected", "Suggested"] | None = None,
        polygon: list[float] | None = None,
        format_version: str | None = None,
        existing_version: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            type,
            start_node,
            end_node,
            name,
            description,
            tags,
            aliases,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            confidence,
            status,
            existing_version,
        )
        self.polygon = polygon
        self.format_version = format_version


class CogniteImage360Annotation(CogniteImage360AnnotationProperties, CogniteAnnotation):
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
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        confidence: float | None = None,
        status: Literal["Approved", "Rejected", "Suggested"] | None = None,
        polygon: list[float] | None = None,
        format_version: str | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            type,
            start_node,
            end_node,
            version,
            last_updated_time,
            created_time,
            name,
            description,
            tags,
            aliases,
            source_id,
            source_context,
            source,
            source_created_time,
            source_updated_time,
            source_created_user,
            source_updated_user,
            confidence,
            status,
            deleted_time,
        )
        self.polygon = polygon
        self.format_version = format_version

    def as_write(self) -> CogniteImage360AnnotationApply:
        return CogniteImage360AnnotationApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            self.name,
            self.description,
            self.tags,
            self.aliases,
            self.source_id,
            self.source_context,
            self.source,
            self.source_created_time,
            self.source_updated_time,
            self.source_created_user,
            self.source_updated_user,
            self.confidence,
            self.status,
            self.polygon,
            self.format_version,
            self.version,
        )
