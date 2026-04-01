from __future__ import annotations

from datetime import datetime
from typing import Literal

from cognite.client._constants import OMITTED, Omitted
from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    PropertyOptions,
    TypedEdge,
    TypedEdgeApply,
    TypedNode,
    TypedNodeApply,
)


class _Cognite360ImageProperties:
    translation_x = PropertyOptions("translationX")
    translation_y = PropertyOptions("translationY")
    translation_z = PropertyOptions("translationZ")
    euler_rotation_x = PropertyOptions("eulerRotationX")
    euler_rotation_y = PropertyOptions("eulerRotationY")
    euler_rotation_z = PropertyOptions("eulerRotationZ")
    scale_x = PropertyOptions("scaleX")
    scale_y = PropertyOptions("scaleY")
    scale_z = PropertyOptions("scaleZ")
    collection_360 = PropertyOptions("collection360")
    station_360 = PropertyOptions("station360")
    taken_at = PropertyOptions("takenAt")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "Cognite360Image", "v1")


class Cognite360ImageApply(_Cognite360ImageProperties, TypedNodeApply):
    """This represents the writing format of Cognite 360 image.

    It is used when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image.
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        front: Direct relation to a file holding the front projection of the cube map
        back: Direct relation to a file holding the back projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        top: Direct relation to a file holding the top projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        collection_360: Direct relation to Cognite360ImageCollection
        station_360: Direct relation to Cognite3DGroup instance that groups different Cognite360Image instances to the same station
        taken_at: The timestamp when the 6 photos were taken
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        translation_x: float | None | Omitted = OMITTED,
        translation_y: float | None | Omitted = OMITTED,
        translation_z: float | None | Omitted = OMITTED,
        euler_rotation_x: float | None | Omitted = OMITTED,
        euler_rotation_y: float | None | Omitted = OMITTED,
        euler_rotation_z: float | None | Omitted = OMITTED,
        scale_x: float | None | Omitted = OMITTED,
        scale_y: float | None | Omitted = OMITTED,
        scale_z: float | None | Omitted = OMITTED,
        front: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        back: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        left: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        right: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        top: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        bottom: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        collection_360: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        station_360: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        taken_at: datetime | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z
        self.front = DirectRelationReference.load(front) if front else front
        self.back = DirectRelationReference.load(back) if back else back
        self.left = DirectRelationReference.load(left) if left else left
        self.right = DirectRelationReference.load(right) if right else right
        self.top = DirectRelationReference.load(top) if top else top
        self.bottom = DirectRelationReference.load(bottom) if bottom else bottom
        self.collection_360 = DirectRelationReference.load(collection_360) if collection_360 else collection_360
        self.station_360 = DirectRelationReference.load(station_360) if station_360 else station_360
        self.taken_at = taken_at


class Cognite360Image(_Cognite360ImageProperties, TypedNode):
    """This represents the reading format of Cognite 360 image.

    It is used when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        front: Direct relation to a file holding the front projection of the cube map
        back: Direct relation to a file holding the back projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        top: Direct relation to a file holding the top projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        collection_360: Direct relation to Cognite360ImageCollection
        station_360: Direct relation to Cognite3DGroup instance that groups different Cognite360Image instances to the same station
        taken_at: The timestamp when the 6 photos were taken
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        translation_x: float | None = None,
        translation_y: float | None = None,
        translation_z: float | None = None,
        euler_rotation_x: float | None = None,
        euler_rotation_y: float | None = None,
        euler_rotation_z: float | None = None,
        scale_x: float | None = None,
        scale_y: float | None = None,
        scale_z: float | None = None,
        front: DirectRelationReference | None = None,
        back: DirectRelationReference | None = None,
        left: DirectRelationReference | None = None,
        right: DirectRelationReference | None = None,
        top: DirectRelationReference | None = None,
        bottom: DirectRelationReference | None = None,
        collection_360: DirectRelationReference | None = None,
        station_360: DirectRelationReference | None = None,
        taken_at: datetime | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z
        self.front = DirectRelationReference.load(front) if front else front
        self.back = DirectRelationReference.load(back) if back else back
        self.left = DirectRelationReference.load(left) if left else left
        self.right = DirectRelationReference.load(right) if right else right
        self.top = DirectRelationReference.load(top) if top else top
        self.bottom = DirectRelationReference.load(bottom) if bottom else bottom
        self.collection_360 = DirectRelationReference.load(collection_360) if collection_360 else collection_360
        self.station_360 = DirectRelationReference.load(station_360) if station_360 else station_360
        self.taken_at = taken_at

    def as_write(self) -> Cognite360ImageApply:
        return Cognite360ImageApply(
            self.space,
            self.external_id,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            front=self.front,
            back=self.back,
            left=self.left,
            right=self.right,
            top=self.top,
            bottom=self.bottom,
            collection_360=self.collection_360,
            station_360=self.station_360,
            taken_at=self.taken_at,
            existing_version=self.version,
            type=self.type,
        )


class _Cognite360ImageCollectionProperties:
    revision_type = PropertyOptions("type")
    model_3d = PropertyOptions("model3D")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "Cognite360ImageCollection", "v1")


class Cognite360ImageCollectionApply(_Cognite360ImageCollectionProperties, TypedNodeApply):
    """This represents the writing format of Cognite 360 image collection.

    It is used when data is written to CDF.

    Represents a logical collection of Cognite360Image instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image collection.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        status: The status field.
        published: The published field.
        revision_type: The revision type field.
        model_3d: The model 3d field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None | Omitted = OMITTED,
        published: bool | None | Omitted = OMITTED,
        revision_type: Literal["CAD", "Image360", "PointCloud"] | None | Omitted = OMITTED,
        model_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.status = status
        self.published = published
        self.revision_type = revision_type
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d


class Cognite360ImageCollection(_Cognite360ImageCollectionProperties, TypedNode):
    """This represents the reading format of Cognite 360 image collection.

    It is used when data is read from CDF.

    Represents a logical collection of Cognite360Image instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image collection.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        status: The status field.
        published: The published field.
        revision_type: The revision type field.
        model_3d: The model 3d field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: bool | None = None,
        revision_type: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.status = status
        self.published = published
        self.revision_type = revision_type
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d

    def as_write(self) -> Cognite360ImageCollectionApply:
        return Cognite360ImageCollectionApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            status=self.status,
            published=self.published,
            revision_type=self.revision_type,
            model_3d=self.model_3d,
            existing_version=self.version,
            type=self.type,
        )


class _Cognite360ImageModelProperties:
    model_type = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "Cognite360ImageModel", "v1")


class Cognite360ImageModelApply(_Cognite360ImageModelProperties, TypedNodeApply):
    """This represents the writing format of Cognite 360 image model.

    It is used when data is written to CDF.

    Navigational aid for traversing Cognite360ImageModel instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image model.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        model_type: CAD, PointCloud or Image360
        thumbnail: Thumbnail of the 3D model
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        model_type: Literal["CAD", "Image360", "PointCloud"] | None | Omitted = OMITTED,
        thumbnail: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.model_type = model_type
        self.thumbnail = DirectRelationReference.load(thumbnail) if thumbnail else thumbnail


class Cognite360ImageModel(_Cognite360ImageModelProperties, TypedNode):
    """This represents the reading format of Cognite 360 image model.

    It is used when data is read from CDF.

    Navigational aid for traversing Cognite360ImageModel instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image model.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        model_type: CAD, PointCloud or Image360
        thumbnail: Thumbnail of the 3D model
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        model_type: Literal["CAD", "Image360", "PointCloud"] | None = None,
        thumbnail: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.model_type = model_type
        self.thumbnail = DirectRelationReference.load(thumbnail) if thumbnail else thumbnail

    def as_write(self) -> Cognite360ImageModelApply:
        return Cognite360ImageModelApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            model_type=self.model_type,
            thumbnail=self.thumbnail,
            existing_version=self.version,
            type=self.type,
        )


class _Cognite360ImageStationProperties:
    group_type = PropertyOptions("groupType")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "Cognite360ImageStation", "v1")


class Cognite360ImageStationApply(_Cognite360ImageStationProperties, TypedNodeApply):
    """This represents the writing format of Cognite 360 image station.

    It is used when data is written to CDF.

    A way to group images across collections. Used for creating visual scan history

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image station.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        group_type: Type of group
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        group_type: Literal["Station360"] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.group_type = group_type


class Cognite360ImageStation(_Cognite360ImageStationProperties, TypedNode):
    """This represents the reading format of Cognite 360 image station.

    It is used when data is read from CDF.

    A way to group images across collections. Used for creating visual scan history

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image station.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        group_type: Type of group
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        group_type: Literal["Station360"] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.group_type = group_type

    def as_write(self) -> Cognite360ImageStationApply:
        return Cognite360ImageStationApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            group_type=self.group_type,
            existing_version=self.version,
            type=self.type,
        )


class _Cognite3DModelProperties:
    model_type = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "Cognite3DModel", "v1")


class Cognite3DModelApply(_Cognite3DModelProperties, TypedNodeApply):
    """This represents the writing format of Cognite 3D model.

    It is used when data is written to CDF.

    Groups revisions of 3D data of various kinds together (CAD, PointCloud, Image360)

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D model.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        model_type: CAD, PointCloud or Image360
        thumbnail: Thumbnail of the 3D model
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        model_type: Literal["CAD", "Image360", "PointCloud"] | None | Omitted = OMITTED,
        thumbnail: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.model_type = model_type
        self.thumbnail = DirectRelationReference.load(thumbnail) if thumbnail else thumbnail


class Cognite3DModel(_Cognite3DModelProperties, TypedNode):
    """This represents the reading format of Cognite 3D model.

    It is used when data is read from CDF.

    Groups revisions of 3D data of various kinds together (CAD, PointCloud, Image360)

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D model.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        model_type: CAD, PointCloud or Image360
        thumbnail: Thumbnail of the 3D model
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        model_type: Literal["CAD", "Image360", "PointCloud"] | None = None,
        thumbnail: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.model_type = model_type
        self.thumbnail = DirectRelationReference.load(thumbnail) if thumbnail else thumbnail

    def as_write(self) -> Cognite3DModelApply:
        return Cognite3DModelApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            model_type=self.model_type,
            thumbnail=self.thumbnail,
            existing_version=self.version,
            type=self.type,
        )


class _Cognite3DObjectProperties:
    x_min = PropertyOptions("xMin")
    x_max = PropertyOptions("xMax")
    y_min = PropertyOptions("yMin")
    y_max = PropertyOptions("yMax")
    z_min = PropertyOptions("zMin")
    z_max = PropertyOptions("zMax")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "Cognite3DObject", "v1")


class Cognite3DObjectApply(_Cognite3DObjectProperties, TypedNodeApply):
    """This represents the writing format of Cognite 3D object.

    It is used when data is written to CDF.

    This is the virtual position representation of an object in the physical world, connecting an asset to one or more 3D resources

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D object.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        x_min: Lowest X value in bounding box
        x_max: Highest X value in bounding box
        y_min: Lowest Y value in bounding box
        y_max: Highest Y value in bounding box
        z_min: Lowest Z value in bounding box
        z_max: Highest Z value in bounding box
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        x_min: float | None | Omitted = OMITTED,
        x_max: float | None | Omitted = OMITTED,
        y_min: float | None | Omitted = OMITTED,
        y_max: float | None | Omitted = OMITTED,
        z_min: float | None | Omitted = OMITTED,
        z_max: float | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max
        self.z_min = z_min
        self.z_max = z_max


class Cognite3DObject(_Cognite3DObjectProperties, TypedNode):
    """This represents the reading format of Cognite 3D object.

    It is used when data is read from CDF.

    This is the virtual position representation of an object in the physical world, connecting an asset to one or more 3D resources

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D object.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        x_min: Lowest X value in bounding box
        x_max: Highest X value in bounding box
        y_min: Lowest Y value in bounding box
        y_max: Highest Y value in bounding box
        z_min: Lowest Z value in bounding box
        z_max: Highest Z value in bounding box
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
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
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max
        self.z_min = z_min
        self.z_max = z_max

    def as_write(self) -> Cognite3DObjectApply:
        return Cognite3DObjectApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            x_min=self.x_min,
            x_max=self.x_max,
            y_min=self.y_min,
            y_max=self.y_max,
            z_min=self.z_min,
            z_max=self.z_max,
            existing_version=self.version,
            type=self.type,
        )


class _Cognite3DRevisionProperties:
    revision_type = PropertyOptions("type")
    model_3d = PropertyOptions("model3D")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "Cognite3DRevision", "v1")


class Cognite3DRevisionApply(_Cognite3DRevisionProperties, TypedNodeApply):
    """This represents the writing format of Cognite 3D revision.

    It is used when data is written to CDF.

    Shared revision information for various 3D data types. Normally not used directly, but through CognitePointCloudRevision, Image360Collection or CogniteCADRevision

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D revision.
        status: The status field.
        published: The published field.
        revision_type: The revision type field.
        model_3d: The model 3d field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None | Omitted = OMITTED,
        published: bool | None | Omitted = OMITTED,
        revision_type: Literal["CAD", "Image360", "PointCloud"] | None | Omitted = OMITTED,
        model_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.status = status
        self.published = published
        self.revision_type = revision_type
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d


class Cognite3DRevision(_Cognite3DRevisionProperties, TypedNode):
    """This represents the reading format of Cognite 3D revision.

    It is used when data is read from CDF.

    Shared revision information for various 3D data types. Normally not used directly, but through CognitePointCloudRevision, Image360Collection or CogniteCADRevision

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D revision.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status: The status field.
        published: The published field.
        revision_type: The revision type field.
        model_3d: The model 3d field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: bool | None = None,
        revision_type: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.status = status
        self.published = published
        self.revision_type = revision_type
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d

    def as_write(self) -> Cognite3DRevisionApply:
        return Cognite3DRevisionApply(
            self.space,
            self.external_id,
            status=self.status,
            published=self.published,
            revision_type=self.revision_type,
            model_3d=self.model_3d,
            existing_version=self.version,
            type=self.type,
        )


class _Cognite3DTransformationProperties:
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
        return ViewId("cdf_cdm", "Cognite3DTransformation", "v1")


class Cognite3DTransformationNodeApply(_Cognite3DTransformationProperties, TypedNodeApply):
    """This represents the writing format of Cognite 3D transformation node.

    It is used when data is written to CDF.

    The Cognite3DTransformation object defines a comprehensive 3D transformation, enabling precise adjustments to an object's position, orientation, and size in the 3D coordinate system. It allows for the translation of objects along the three spatial axes, rotation around these axes using Euler angles, and scaling along each axis to modify the object's dimensions. The object's transformation is defined in "CDF space", a coordinate system where the positive Z axis is the up direction

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation node.
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        translation_x: float | None | Omitted = OMITTED,
        translation_y: float | None | Omitted = OMITTED,
        translation_z: float | None | Omitted = OMITTED,
        euler_rotation_x: float | None | Omitted = OMITTED,
        euler_rotation_y: float | None | Omitted = OMITTED,
        euler_rotation_z: float | None | Omitted = OMITTED,
        scale_x: float | None | Omitted = OMITTED,
        scale_y: float | None | Omitted = OMITTED,
        scale_z: float | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z


class Cognite3DTransformationNode(_Cognite3DTransformationProperties, TypedNode):
    """This represents the reading format of Cognite 3D transformation node.

    It is used when data is read from CDF.

    The Cognite3DTransformation object defines a comprehensive 3D transformation, enabling precise adjustments to an object's position, orientation, and size in the 3D coordinate system. It allows for the translation of objects along the three spatial axes, rotation around these axes using Euler angles, and scaling along each axis to modify the object's dimensions. The object's transformation is defined in "CDF space", a coordinate system where the positive Z axis is the up direction

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation node.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        translation_x: float | None = None,
        translation_y: float | None = None,
        translation_z: float | None = None,
        euler_rotation_x: float | None = None,
        euler_rotation_y: float | None = None,
        euler_rotation_z: float | None = None,
        scale_x: float | None = None,
        scale_y: float | None = None,
        scale_z: float | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z

    def as_write(self) -> Cognite3DTransformationNodeApply:
        return Cognite3DTransformationNodeApply(
            self.space,
            self.external_id,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteActivityProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
    start_time = PropertyOptions("startTime")
    end_time = PropertyOptions("endTime")
    scheduled_start_time = PropertyOptions("scheduledStartTime")
    scheduled_end_time = PropertyOptions("scheduledEndTime")
    time_series = PropertyOptions("timeSeries")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteActivity", "v1")


class CogniteActivityApply(_CogniteActivityProperties, TypedNodeApply):
    """This represents the writing format of Cognite activity.

    It is used when data is written to CDF.

    Represents activities. Activities typically happen over a period and have a start and end time.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite activity.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        start_time: The actual start time of an activity (or similar that extends this)
        end_time: The actual end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        assets: A list of assets the activity is related to.
        equipment: A list of equipment the activity is related to.
        time_series: A list of time series the activity is related to.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        start_time: datetime | None | Omitted = OMITTED,
        end_time: datetime | None | Omitted = OMITTED,
        scheduled_start_time: datetime | None | Omitted = OMITTED,
        scheduled_end_time: datetime | None | Omitted = OMITTED,
        assets: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        equipment: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        time_series: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time
        self.assets = [DirectRelationReference.load(a) for a in assets] if assets else assets
        self.equipment = [DirectRelationReference.load(e) for e in equipment] if equipment else equipment
        self.time_series = [DirectRelationReference.load(t) for t in time_series] if time_series else time_series


class CogniteActivity(_CogniteActivityProperties, TypedNode):
    """This represents the reading format of Cognite activity.

    It is used when data is read from CDF.

    Represents activities. Activities typically happen over a period and have a start and end time.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite activity.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        start_time: The actual start time of an activity (or similar that extends this)
        end_time: The actual end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        assets: A list of assets the activity is related to.
        equipment: A list of equipment the activity is related to.
        time_series: A list of time series the activity is related to.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        assets: list[DirectRelationReference] | None = None,
        equipment: list[DirectRelationReference] | None = None,
        time_series: list[DirectRelationReference] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time
        self.assets = [DirectRelationReference.load(a) for a in assets] if assets else assets
        self.equipment = [DirectRelationReference.load(e) for e in equipment] if equipment else equipment
        self.time_series = [DirectRelationReference.load(t) for t in time_series] if time_series else time_series

    def as_write(self) -> CogniteActivityApply:
        return CogniteActivityApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            start_time=self.start_time,
            end_time=self.end_time,
            scheduled_start_time=self.scheduled_start_time,
            scheduled_end_time=self.scheduled_end_time,
            assets=self.assets,  # type: ignore[arg-type]
            equipment=self.equipment,  # type: ignore[arg-type]
            time_series=self.time_series,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteAssetProperties:
    object_3d = PropertyOptions("object3D")
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
    asset_class = PropertyOptions("assetClass")
    asset_type = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteAsset", "v1")


class CogniteAssetApply(_CogniteAssetProperties, TypedNodeApply):
    """This represents the writing format of Cognite asset.

    It is used when data is written to CDF.

    Assets represent systems that support industrial functions or processes. Assets are often called 'functional location'.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset.
        object_3d: Direct relation to an Object3D instance representing the 3D resource
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        parent: The parent of the asset.
        asset_class: Specifies the class of the asset. It's a direct relation to CogniteAssetClass.
        asset_type: Specifies the type of the asset. It's a direct relation to CogniteAssetType.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        object_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        parent: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        asset_class: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        asset_type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else object_3d
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.parent = DirectRelationReference.load(parent) if parent else parent
        self.asset_class = DirectRelationReference.load(asset_class) if asset_class else asset_class
        self.asset_type = DirectRelationReference.load(asset_type) if asset_type else asset_type


class CogniteAsset(_CogniteAssetProperties, TypedNode):
    """This represents the reading format of Cognite asset.

    It is used when data is read from CDF.

    Assets represent systems that support industrial functions or processes. Assets are often called 'functional location'.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        object_3d: Direct relation to an Object3D instance representing the 3D resource
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        parent: The parent of the asset.
        root: An automatically updated reference to the top-level asset of the hierarchy.
        path: An automatically updated ordered list of this asset's ancestors, starting with the root asset. Enables subtree filtering to find all assets under a parent.
        path_last_updated_time: The last time the path was updated for this asset.
        asset_class: Specifies the class of the asset. It's a direct relation to CogniteAssetClass.
        asset_type: Specifies the type of the asset. It's a direct relation to CogniteAssetType.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    path_last_updated_time = PropertyOptions("pathLastUpdatedTime")

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        object_3d: DirectRelationReference | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        parent: DirectRelationReference | None = None,
        root: DirectRelationReference | None = None,
        path: list[DirectRelationReference] | None = None,
        path_last_updated_time: datetime | None = None,
        asset_class: DirectRelationReference | None = None,
        asset_type: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else object_3d
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.parent = DirectRelationReference.load(parent) if parent else parent
        self.root = DirectRelationReference.load(root) if root else root
        self.path = [DirectRelationReference.load(p) for p in path] if path else path
        self.path_last_updated_time = path_last_updated_time
        self.asset_class = DirectRelationReference.load(asset_class) if asset_class else asset_class
        self.asset_type = DirectRelationReference.load(asset_type) if asset_type else asset_type

    def as_write(self) -> CogniteAssetApply:
        return CogniteAssetApply(
            self.space,
            self.external_id,
            object_3d=self.object_3d,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            parent=self.parent,
            asset_class=self.asset_class,
            asset_type=self.asset_type,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteAssetClassProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteAssetClass", "v1")


class CogniteAssetClassApply(_CogniteAssetClassProperties, TypedNodeApply):
    """This represents the writing format of Cognite asset clas.

    It is used when data is written to CDF.

    Represents the class of an asset.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset clas.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        code: A unique identifier for the class of asset.
        standard: A text string to specify which standard the class is from.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        code: str | None | Omitted = OMITTED,
        standard: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.code = code
        self.standard = standard


class CogniteAssetClass(_CogniteAssetClassProperties, TypedNode):
    """This represents the reading format of Cognite asset clas.

    It is used when data is read from CDF.

    Represents the class of an asset.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset clas.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        code: A unique identifier for the class of asset.
        standard: A text string to specify which standard the class is from.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        standard: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.code = code
        self.standard = standard

    def as_write(self) -> CogniteAssetClassApply:
        return CogniteAssetClassApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            code=self.code,
            standard=self.standard,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteAssetTypeProperties:
    asset_class = PropertyOptions("assetClass")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteAssetType", "v1")


class CogniteAssetTypeApply(_CogniteAssetTypeProperties, TypedNodeApply):
    """This represents the writing format of Cognite asset type.

    It is used when data is written to CDF.

    Represents the type of an asset.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset type.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        code: A unique identifier for the type of asset.
        standard: A text string to specify which standard the type is from.
        asset_class: Specifies the class the type belongs to. It's a direct relation to CogniteAssetClass.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        code: str | None | Omitted = OMITTED,
        standard: str | None | Omitted = OMITTED,
        asset_class: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.code = code
        self.standard = standard
        self.asset_class = DirectRelationReference.load(asset_class) if asset_class else asset_class


class CogniteAssetType(_CogniteAssetTypeProperties, TypedNode):
    """This represents the reading format of Cognite asset type.

    It is used when data is read from CDF.

    Represents the type of an asset.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset type.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        code: A unique identifier for the type of asset.
        standard: A text string to specify which standard the type is from.
        asset_class: Specifies the class the type belongs to. It's a direct relation to CogniteAssetClass.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        standard: str | None = None,
        asset_class: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.code = code
        self.standard = standard
        self.asset_class = DirectRelationReference.load(asset_class) if asset_class else asset_class

    def as_write(self) -> CogniteAssetTypeApply:
        return CogniteAssetTypeApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            code=self.code,
            standard=self.standard,
            asset_class=self.asset_class,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteCADModelProperties:
    model_type = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteCADModel", "v1")


class CogniteCADModelApply(_CogniteCADModelProperties, TypedNodeApply):
    """This represents the writing format of Cognite cad model.

    It is used when data is written to CDF.

    Navigational aid for traversing CogniteCADModel instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad model.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        model_type: CAD, PointCloud or Image360
        thumbnail: Thumbnail of the 3D model
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        model_type: Literal["CAD", "Image360", "PointCloud"] | None | Omitted = OMITTED,
        thumbnail: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.model_type = model_type
        self.thumbnail = DirectRelationReference.load(thumbnail) if thumbnail else thumbnail


class CogniteCADModel(_CogniteCADModelProperties, TypedNode):
    """This represents the reading format of Cognite cad model.

    It is used when data is read from CDF.

    Navigational aid for traversing CogniteCADModel instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad model.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        model_type: CAD, PointCloud or Image360
        thumbnail: Thumbnail of the 3D model
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        model_type: Literal["CAD", "Image360", "PointCloud"] | None = None,
        thumbnail: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.model_type = model_type
        self.thumbnail = DirectRelationReference.load(thumbnail) if thumbnail else thumbnail

    def as_write(self) -> CogniteCADModelApply:
        return CogniteCADModelApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            model_type=self.model_type,
            thumbnail=self.thumbnail,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteCADNodeProperties:
    object_3d = PropertyOptions("object3D")
    model_3d = PropertyOptions("model3D")
    cad_node_reference = PropertyOptions("cadNodeReference")
    tree_indexes = PropertyOptions("treeIndexes")
    sub_tree_sizes = PropertyOptions("subTreeSizes")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteCADNode", "v1")


class CogniteCADNodeApply(_CogniteCADNodeProperties, TypedNodeApply):
    """This represents the writing format of Cognite cad node.

    It is used when data is written to CDF.

    Represents nodes from the 3D model that have been contextualized

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad node.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        object_3d: Direct relation to object3D grouping for this node
        model_3d: Direct relation to Cognite3DModel
        cad_node_reference: Reference to a node within a CAD model from the 3D API
        revisions: List of direct relations to instances of Cognite3DRevision which this CogniteCADNode exists in.
        tree_indexes: List of tree indexes in the same order as revisions. Used by Reveal and similar applications to map from CogniteCADNode to tree index
        sub_tree_sizes: List of subtree sizes in the same order as revisions. Used by Reveal and similar applications to know how many nodes exists below this node in the hierarchy
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        object_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        model_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        cad_node_reference: str | None | Omitted = OMITTED,
        revisions: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        tree_indexes: list[int] | None | Omitted = OMITTED,
        sub_tree_sizes: list[int] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else object_3d
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d
        self.cad_node_reference = cad_node_reference
        self.revisions = [DirectRelationReference.load(r) for r in revisions] if revisions else revisions
        self.tree_indexes = tree_indexes
        self.sub_tree_sizes = sub_tree_sizes


class CogniteCADNode(_CogniteCADNodeProperties, TypedNode):
    """This represents the reading format of Cognite cad node.

    It is used when data is read from CDF.

    Represents nodes from the 3D model that have been contextualized

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad node.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        object_3d: Direct relation to object3D grouping for this node
        model_3d: Direct relation to Cognite3DModel
        cad_node_reference: Reference to a node within a CAD model from the 3D API
        revisions: List of direct relations to instances of Cognite3DRevision which this CogniteCADNode exists in.
        tree_indexes: List of tree indexes in the same order as revisions. Used by Reveal and similar applications to map from CogniteCADNode to tree index
        sub_tree_sizes: List of subtree sizes in the same order as revisions. Used by Reveal and similar applications to know how many nodes exists below this node in the hierarchy
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        object_3d: DirectRelationReference | None = None,
        model_3d: DirectRelationReference | None = None,
        cad_node_reference: str | None = None,
        revisions: list[DirectRelationReference] | None = None,
        tree_indexes: list[int] | None = None,
        sub_tree_sizes: list[int] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else object_3d
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d
        self.cad_node_reference = cad_node_reference
        self.revisions = [DirectRelationReference.load(r) for r in revisions] if revisions else revisions
        self.tree_indexes = tree_indexes
        self.sub_tree_sizes = sub_tree_sizes

    def as_write(self) -> CogniteCADNodeApply:
        return CogniteCADNodeApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            object_3d=self.object_3d,
            model_3d=self.model_3d,
            cad_node_reference=self.cad_node_reference,
            revisions=self.revisions,  # type: ignore[arg-type]
            tree_indexes=self.tree_indexes,
            sub_tree_sizes=self.sub_tree_sizes,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteCADRevisionProperties:
    revision_type = PropertyOptions("type")
    model_3d = PropertyOptions("model3D")
    revision_id = PropertyOptions("revisionId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteCADRevision", "v1")


class CogniteCADRevisionApply(_CogniteCADRevisionProperties, TypedNodeApply):
    """This represents the writing format of Cognite cad revision.

    It is used when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad revision.
        status: The status field.
        published: The published field.
        revision_type: The revision type field.
        model_3d: .
        revision_id: The 3D API revision identifier for this CAD model
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None | Omitted = OMITTED,
        published: bool | None | Omitted = OMITTED,
        revision_type: Literal["CAD", "Image360", "PointCloud"] | None | Omitted = OMITTED,
        model_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        revision_id: int | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.status = status
        self.published = published
        self.revision_type = revision_type
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d
        self.revision_id = revision_id


class CogniteCADRevision(_CogniteCADRevisionProperties, TypedNode):
    """This represents the reading format of Cognite cad revision.

    It is used when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad revision.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status: The status field.
        published: The published field.
        revision_type: The revision type field.
        model_3d: .
        revision_id: The 3D API revision identifier for this CAD model
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: bool | None = None,
        revision_type: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | None = None,
        revision_id: int | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.status = status
        self.published = published
        self.revision_type = revision_type
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d
        self.revision_id = revision_id

    def as_write(self) -> CogniteCADRevisionApply:
        return CogniteCADRevisionApply(
            self.space,
            self.external_id,
            status=self.status,
            published=self.published,
            revision_type=self.revision_type,
            model_3d=self.model_3d,
            revision_id=self.revision_id,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteCubeMapProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteCubeMap", "v1")


class CogniteCubeMapApply(_CogniteCubeMapProperties, TypedNodeApply):
    """This represents the writing format of Cognite cube map.

    It is used when data is written to CDF.

    The cube map holds references to 6 images in used to visually represent the surrounding environment

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cube map.
        front: Direct relation to a file holding the front projection of the cube map
        back: Direct relation to a file holding the back projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        top: Direct relation to a file holding the top projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        front: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        back: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        left: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        right: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        top: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        bottom: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.front = DirectRelationReference.load(front) if front else front
        self.back = DirectRelationReference.load(back) if back else back
        self.left = DirectRelationReference.load(left) if left else left
        self.right = DirectRelationReference.load(right) if right else right
        self.top = DirectRelationReference.load(top) if top else top
        self.bottom = DirectRelationReference.load(bottom) if bottom else bottom


class CogniteCubeMap(_CogniteCubeMapProperties, TypedNode):
    """This represents the reading format of Cognite cube map.

    It is used when data is read from CDF.

    The cube map holds references to 6 images in used to visually represent the surrounding environment

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cube map.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        front: Direct relation to a file holding the front projection of the cube map
        back: Direct relation to a file holding the back projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        top: Direct relation to a file holding the top projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        front: DirectRelationReference | None = None,
        back: DirectRelationReference | None = None,
        left: DirectRelationReference | None = None,
        right: DirectRelationReference | None = None,
        top: DirectRelationReference | None = None,
        bottom: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.front = DirectRelationReference.load(front) if front else front
        self.back = DirectRelationReference.load(back) if back else back
        self.left = DirectRelationReference.load(left) if left else left
        self.right = DirectRelationReference.load(right) if right else right
        self.top = DirectRelationReference.load(top) if top else top
        self.bottom = DirectRelationReference.load(bottom) if bottom else bottom

    def as_write(self) -> CogniteCubeMapApply:
        return CogniteCubeMapApply(
            self.space,
            self.external_id,
            front=self.front,
            back=self.back,
            left=self.left,
            right=self.right,
            top=self.top,
            bottom=self.bottom,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteDescribableProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteDescribable", "v1")


class CogniteDescribableNodeApply(_CogniteDescribableProperties, TypedNodeApply):
    """This represents the writing format of Cognite describable node.

    It is used when data is written to CDF.

    The describable core concept is used as a standard way of holding the bare minimum of information about the instance

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable node.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases


class CogniteDescribableNode(_CogniteDescribableProperties, TypedNode):
    """This represents the reading format of Cognite describable node.

    It is used when data is read from CDF.

    The describable core concept is used as a standard way of holding the bare minimum of information about the instance

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable node.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases

    def as_write(self) -> CogniteDescribableNodeApply:
        return CogniteDescribableNodeApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteEquipmentProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
    serial_number = PropertyOptions("serialNumber")
    equipment_type = PropertyOptions("equipmentType")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteEquipment", "v1")


class CogniteEquipmentApply(_CogniteEquipmentProperties, TypedNodeApply):
    """This represents the writing format of Cognite equipment.

    It is used when data is written to CDF.

    Equipment represents physical supplies or devices.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite equipment.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        asset: The asset the equipment is related to.
        serial_number: The serial number of the equipment.
        manufacturer: The manufacturer of the equipment.
        equipment_type: Specifies the type of the equipment. It's a direct relation to CogniteEquipmentType.
        files: A list of files the equipment relates to.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        asset: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        serial_number: str | None | Omitted = OMITTED,
        manufacturer: str | None | Omitted = OMITTED,
        equipment_type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        files: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.asset = DirectRelationReference.load(asset) if asset else asset
        self.serial_number = serial_number
        self.manufacturer = manufacturer
        self.equipment_type = DirectRelationReference.load(equipment_type) if equipment_type else equipment_type
        self.files = [DirectRelationReference.load(f) for f in files] if files else files


class CogniteEquipment(_CogniteEquipmentProperties, TypedNode):
    """This represents the reading format of Cognite equipment.

    It is used when data is read from CDF.

    Equipment represents physical supplies or devices.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite equipment.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        asset: The asset the equipment is related to.
        serial_number: The serial number of the equipment.
        manufacturer: The manufacturer of the equipment.
        equipment_type: Specifies the type of the equipment. It's a direct relation to CogniteEquipmentType.
        files: A list of files the equipment relates to.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        asset: DirectRelationReference | None = None,
        serial_number: str | None = None,
        manufacturer: str | None = None,
        equipment_type: DirectRelationReference | None = None,
        files: list[DirectRelationReference] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.asset = DirectRelationReference.load(asset) if asset else asset
        self.serial_number = serial_number
        self.manufacturer = manufacturer
        self.equipment_type = DirectRelationReference.load(equipment_type) if equipment_type else equipment_type
        self.files = [DirectRelationReference.load(f) for f in files] if files else files

    def as_write(self) -> CogniteEquipmentApply:
        return CogniteEquipmentApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            asset=self.asset,
            serial_number=self.serial_number,
            manufacturer=self.manufacturer,
            equipment_type=self.equipment_type,
            files=self.files,  # type: ignore[arg-type]
            existing_version=self.version,
            type=self.type,
        )


class _CogniteEquipmentTypeProperties:
    equipment_class = PropertyOptions("equipmentClass")
    standard_reference = PropertyOptions("standardReference")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteEquipmentType", "v1")


class CogniteEquipmentTypeApply(_CogniteEquipmentTypeProperties, TypedNodeApply):
    """This represents the writing format of Cognite equipment type.

    It is used when data is written to CDF.

    Represents the type of equipment.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite equipment type.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        code: A unique identifier for the type of equipment.
        equipment_class: Represents the class of equipment.
        standard: An identifier for the standard this equipment type is sourced from, for example, ISO14224.
        standard_reference: A reference to the source of the equipment standard.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        code: str | None | Omitted = OMITTED,
        equipment_class: str | None | Omitted = OMITTED,
        standard: str | None | Omitted = OMITTED,
        standard_reference: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.code = code
        self.equipment_class = equipment_class
        self.standard = standard
        self.standard_reference = standard_reference


class CogniteEquipmentType(_CogniteEquipmentTypeProperties, TypedNode):
    """This represents the reading format of Cognite equipment type.

    It is used when data is read from CDF.

    Represents the type of equipment.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite equipment type.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        code: A unique identifier for the type of equipment.
        equipment_class: Represents the class of equipment.
        standard: An identifier for the standard this equipment type is sourced from, for example, ISO14224.
        standard_reference: A reference to the source of the equipment standard.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        equipment_class: str | None = None,
        standard: str | None = None,
        standard_reference: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.code = code
        self.equipment_class = equipment_class
        self.standard = standard
        self.standard_reference = standard_reference

    def as_write(self) -> CogniteEquipmentTypeApply:
        return CogniteEquipmentTypeApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            code=self.code,
            equipment_class=self.equipment_class,
            standard=self.standard,
            standard_reference=self.standard_reference,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteFileProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
    mime_type = PropertyOptions("mimeType")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteFile", "v1")


class CogniteFileApply(_CogniteFileProperties, TypedNodeApply):
    """This represents the writing format of Cognite file.

    It is used when data is written to CDF.

    Represents files.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        assets: A list of assets this file is related to.
        mime_type: The MIME type of the file.
        directory: Contains the path elements from the source (if the source system has a file system hierarchy or similar.)
        category: Specifies the detected category the file belongs to. It's a direct relation to an instance of CogniteFileCategory.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        assets: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        mime_type: str | None | Omitted = OMITTED,
        directory: str | None | Omitted = OMITTED,
        category: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.assets = [DirectRelationReference.load(a) for a in assets] if assets else assets
        self.mime_type = mime_type
        self.directory = directory
        self.category = DirectRelationReference.load(category) if category else category


class CogniteFile(_CogniteFileProperties, TypedNode):
    """This represents the reading format of Cognite file.

    It is used when data is read from CDF.

    Represents files.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        assets: A list of assets this file is related to.
        mime_type: The MIME type of the file.
        directory: Contains the path elements from the source (if the source system has a file system hierarchy or similar.)
        is_uploaded: Specifies if the file content has been uploaded to Cognite Data Fusion or not.
        uploaded_time: The time the file upload completed.
        category: Specifies the detected category the file belongs to. It's a direct relation to an instance of CogniteFileCategory.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    is_uploaded = PropertyOptions("isUploaded")
    uploaded_time = PropertyOptions("uploadedTime")

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        assets: list[DirectRelationReference] | None = None,
        mime_type: str | None = None,
        directory: str | None = None,
        is_uploaded: bool | None = None,
        uploaded_time: datetime | None = None,
        category: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.assets = [DirectRelationReference.load(a) for a in assets] if assets else assets
        self.mime_type = mime_type
        self.directory = directory
        self.is_uploaded = is_uploaded
        self.uploaded_time = uploaded_time
        self.category = DirectRelationReference.load(category) if category else category

    def as_write(self) -> CogniteFileApply:
        return CogniteFileApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            assets=self.assets,  # type: ignore[arg-type]
            mime_type=self.mime_type,
            directory=self.directory,
            category=self.category,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteFileCategoryProperties:
    standard_reference = PropertyOptions("standardReference")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteFileCategory", "v1")


class CogniteFileCategoryApply(_CogniteFileCategoryProperties, TypedNodeApply):
    """This represents the writing format of Cognite file category.

    It is used when data is written to CDF.

    Represents the categories of files as determined by contextualization or categorization.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file category.
        code: An identifier for the category, for example, 'AA' for Accounting (from Norsok.)
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        standard: The name of the standard the category originates from, for example, 'Norsok'.
        standard_reference: A reference to the source of the category standard.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        code: str,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        standard: str | None | Omitted = OMITTED,
        standard_reference: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.code = code
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.standard = standard
        self.standard_reference = standard_reference


class CogniteFileCategory(_CogniteFileCategoryProperties, TypedNode):
    """This represents the reading format of Cognite file category.

    It is used when data is read from CDF.

    Represents the categories of files as determined by contextualization or categorization.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file category.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        code: An identifier for the category, for example, 'AA' for Accounting (from Norsok.)
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        standard: The name of the standard the category originates from, for example, 'Norsok'.
        standard_reference: A reference to the source of the category standard.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        code: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        standard: str | None = None,
        standard_reference: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.code = code
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.standard = standard
        self.standard_reference = standard_reference

    def as_write(self) -> CogniteFileCategoryApply:
        return CogniteFileCategoryApply(
            self.space,
            self.external_id,
            code=self.code,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            standard=self.standard,
            standard_reference=self.standard_reference,
            existing_version=self.version,
            type=self.type,
        )


class _CognitePointCloudModelProperties:
    model_type = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CognitePointCloudModel", "v1")


class CognitePointCloudModelApply(_CognitePointCloudModelProperties, TypedNodeApply):
    """This represents the writing format of Cognite point cloud model.

    It is used when data is written to CDF.

    Navigational aid for traversing CognitePointCloudModel instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud model.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        model_type: CAD, PointCloud or Image360
        thumbnail: Thumbnail of the 3D model
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        model_type: Literal["CAD", "Image360", "PointCloud"] | None | Omitted = OMITTED,
        thumbnail: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.model_type = model_type
        self.thumbnail = DirectRelationReference.load(thumbnail) if thumbnail else thumbnail


class CognitePointCloudModel(_CognitePointCloudModelProperties, TypedNode):
    """This represents the reading format of Cognite point cloud model.

    It is used when data is read from CDF.

    Navigational aid for traversing CognitePointCloudModel instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud model.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        model_type: CAD, PointCloud or Image360
        thumbnail: Thumbnail of the 3D model
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        model_type: Literal["CAD", "Image360", "PointCloud"] | None = None,
        thumbnail: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.model_type = model_type
        self.thumbnail = DirectRelationReference.load(thumbnail) if thumbnail else thumbnail

    def as_write(self) -> CognitePointCloudModelApply:
        return CognitePointCloudModelApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            model_type=self.model_type,
            thumbnail=self.thumbnail,
            existing_version=self.version,
            type=self.type,
        )


class _CognitePointCloudRevisionProperties:
    revision_type = PropertyOptions("type")
    model_3d = PropertyOptions("model3D")
    revision_id = PropertyOptions("revisionId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CognitePointCloudRevision", "v1")


class CognitePointCloudRevisionApply(_CognitePointCloudRevisionProperties, TypedNodeApply):
    """This represents the writing format of Cognite point cloud revision.

    It is used when data is written to CDF.

    Navigational aid for traversing CognitePointCloudRevision instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud revision.
        status: The status field.
        published: The published field.
        revision_type: The revision type field.
        model_3d: .
        revision_id: The 3D API revision identifier for this PointCloud model
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None | Omitted = OMITTED,
        published: bool | None | Omitted = OMITTED,
        revision_type: Literal["CAD", "Image360", "PointCloud"] | None | Omitted = OMITTED,
        model_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        revision_id: int | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.status = status
        self.published = published
        self.revision_type = revision_type
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d
        self.revision_id = revision_id


class CognitePointCloudRevision(_CognitePointCloudRevisionProperties, TypedNode):
    """This represents the reading format of Cognite point cloud revision.

    It is used when data is read from CDF.

    Navigational aid for traversing CognitePointCloudRevision instances

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud revision.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status: The status field.
        published: The published field.
        revision_type: The revision type field.
        model_3d: .
        revision_id: The 3D API revision identifier for this PointCloud model
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: bool | None = None,
        revision_type: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | None = None,
        revision_id: int | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.status = status
        self.published = published
        self.revision_type = revision_type
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d
        self.revision_id = revision_id

    def as_write(self) -> CognitePointCloudRevisionApply:
        return CognitePointCloudRevisionApply(
            self.space,
            self.external_id,
            status=self.status,
            published=self.published,
            revision_type=self.revision_type,
            model_3d=self.model_3d,
            revision_id=self.revision_id,
            existing_version=self.version,
            type=self.type,
        )


class _CognitePointCloudVolumeProperties:
    object_3d = PropertyOptions("object3D")
    model_3d = PropertyOptions("model3D")
    volume_references = PropertyOptions("volumeReferences")
    volume_type = PropertyOptions("volumeType")
    format_version = PropertyOptions("formatVersion")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CognitePointCloudVolume", "v1")


class CognitePointCloudVolumeApply(_CognitePointCloudVolumeProperties, TypedNodeApply):
    """This represents the writing format of Cognite point cloud volume.

    It is used when data is written to CDF.

    PointCloud volume definition

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud volume.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        object_3d: Direct relation to object3D grouping for this node
        model_3d: Direct relation to Cognite3DModel instance
        volume_references: Unique volume metric hashes used to access the 3D specialized data storage
        revisions: List of direct relations to revision information
        volume_type: Type of volume (Cylinder or Box)
        volume: Relevant coordinates for the volume type, 9 floats in total, that defines the volume
        format_version: Specifies the version the 'volume' field is following. Volume definition is today 9 floats (property volume)
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        object_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        model_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        volume_references: list[str] | None | Omitted = OMITTED,
        revisions: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        volume_type: Literal["Box", "Cylinder"] | None | Omitted = OMITTED,
        volume: list[float] | None | Omitted = OMITTED,
        format_version: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else object_3d
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d
        self.volume_references = volume_references
        self.revisions = [DirectRelationReference.load(r) for r in revisions] if revisions else revisions
        self.volume_type = volume_type
        self.volume = volume
        self.format_version = format_version


class CognitePointCloudVolume(_CognitePointCloudVolumeProperties, TypedNode):
    """This represents the reading format of Cognite point cloud volume.

    It is used when data is read from CDF.

    PointCloud volume definition

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud volume.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        object_3d: Direct relation to object3D grouping for this node
        model_3d: Direct relation to Cognite3DModel instance
        volume_references: Unique volume metric hashes used to access the 3D specialized data storage
        revisions: List of direct relations to revision information
        volume_type: Type of volume (Cylinder or Box)
        volume: Relevant coordinates for the volume type, 9 floats in total, that defines the volume
        format_version: Specifies the version the 'volume' field is following. Volume definition is today 9 floats (property volume)
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        object_3d: DirectRelationReference | None = None,
        model_3d: DirectRelationReference | None = None,
        volume_references: list[str] | None = None,
        revisions: list[DirectRelationReference] | None = None,
        volume_type: Literal["Box", "Cylinder"] | None = None,
        volume: list[float] | None = None,
        format_version: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else object_3d
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else model_3d
        self.volume_references = volume_references
        self.revisions = [DirectRelationReference.load(r) for r in revisions] if revisions else revisions
        self.volume_type = volume_type
        self.volume = volume
        self.format_version = format_version

    def as_write(self) -> CognitePointCloudVolumeApply:
        return CognitePointCloudVolumeApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            object_3d=self.object_3d,
            model_3d=self.model_3d,
            volume_references=self.volume_references,
            revisions=self.revisions,  # type: ignore[arg-type]
            volume_type=self.volume_type,
            volume=self.volume,
            format_version=self.format_version,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteSchedulableProperties:
    start_time = PropertyOptions("startTime")
    end_time = PropertyOptions("endTime")
    scheduled_start_time = PropertyOptions("scheduledStartTime")
    scheduled_end_time = PropertyOptions("scheduledEndTime")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteSchedulable", "v1")


class CogniteSchedulableApply(_CogniteSchedulableProperties, TypedNodeApply):
    """This represents the writing format of Cognite schedulable.

    It is used when data is written to CDF.

    CogniteSchedulable represents the metadata about when an activity (or similar) starts and ends.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite schedulable.
        start_time: The actual start time of an activity (or similar that extends this)
        end_time: The actual end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        start_time: datetime | None | Omitted = OMITTED,
        end_time: datetime | None | Omitted = OMITTED,
        scheduled_start_time: datetime | None | Omitted = OMITTED,
        scheduled_end_time: datetime | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time


class CogniteSchedulable(_CogniteSchedulableProperties, TypedNode):
    """This represents the reading format of Cognite schedulable.

    It is used when data is read from CDF.

    CogniteSchedulable represents the metadata about when an activity (or similar) starts and ends.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite schedulable.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        start_time: The actual start time of an activity (or similar that extends this)
        end_time: The actual end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time

    def as_write(self) -> CogniteSchedulableApply:
        return CogniteSchedulableApply(
            self.space,
            self.external_id,
            start_time=self.start_time,
            end_time=self.end_time,
            scheduled_start_time=self.scheduled_start_time,
            scheduled_end_time=self.scheduled_end_time,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteSourceSystemProperties:
    source_system_version = PropertyOptions("version")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteSourceSystem", "v1")


class CogniteSourceSystemApply(_CogniteSourceSystemProperties, TypedNodeApply):
    """This represents the writing format of Cognite source system.

    It is used when data is written to CDF.

    The CogniteSourceSystem core concept is used to standardize the way source system is stored.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite source system.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_system_version: Version identifier for the source system
        manufacturer: Manufacturer of the source system
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_system_version: str | None | Omitted = OMITTED,
        manufacturer: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_system_version = source_system_version
        self.manufacturer = manufacturer


class CogniteSourceSystem(_CogniteSourceSystemProperties, TypedNode):
    """This represents the reading format of Cognite source system.

    It is used when data is read from CDF.

    The CogniteSourceSystem core concept is used to standardize the way source system is stored.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite source system.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_system_version: Version identifier for the source system
        manufacturer: Manufacturer of the source system
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_system_version: str | None = None,
        manufacturer: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_system_version = source_system_version
        self.manufacturer = manufacturer

    def as_write(self) -> CogniteSourceSystemApply:
        return CogniteSourceSystemApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_system_version=self.source_system_version,
            manufacturer=self.manufacturer,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteSourceableProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteSourceable", "v1")


class CogniteSourceableNodeApply(_CogniteSourceableProperties, TypedNodeApply):
    """This represents the writing format of Cognite sourceable node.

    It is used when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite sourceable node.
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user


class CogniteSourceableNode(_CogniteSourceableProperties, TypedNode):
    """This represents the reading format of Cognite sourceable node.

    It is used when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite sourceable node.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user

    def as_write(self) -> CogniteSourceableNodeApply:
        return CogniteSourceableNodeApply(
            self.space,
            self.external_id,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteTimeSeriesProperties:
    is_step = PropertyOptions("isStep")
    time_series_type = PropertyOptions("type")
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
    source_unit = PropertyOptions("sourceUnit")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteTimeSeries", "v1")


class CogniteTimeSeriesApply(_CogniteTimeSeriesProperties, TypedNodeApply):
    """This represents the writing format of Cognite time series.

    It is used when data is written to CDF.

    Represents a series of data points in time order."

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite time series.
        is_step: Specifies whether the time series is a step time series or not.
        time_series_type: Specifies the data type of the data points.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_unit: The unit specified in the source system.
        unit: The unit of the time series.
        assets: A list of assets the time series is related to.
        equipment: A list of equipment the time series is related to.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        is_step: bool,
        time_series_type: Literal["numeric", "string"],
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        source_unit: str | None | Omitted = OMITTED,
        unit: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        assets: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        equipment: list[DirectRelationReference | tuple[str, str]] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.is_step = is_step
        self.time_series_type = time_series_type
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.source_unit = source_unit
        self.unit = DirectRelationReference.load(unit) if unit else unit
        self.assets = [DirectRelationReference.load(a) for a in assets] if assets else assets
        self.equipment = [DirectRelationReference.load(e) for e in equipment] if equipment else equipment


class CogniteTimeSeries(_CogniteTimeSeriesProperties, TypedNode):
    """This represents the reading format of Cognite time series.

    It is used when data is read from CDF.

    Represents a series of data points in time order."

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite time series.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        is_step: Specifies whether the time series is a step time series or not.
        time_series_type: Specifies the data type of the data points.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_unit: The unit specified in the source system.
        unit: The unit of the time series.
        assets: A list of assets the time series is related to.
        equipment: A list of equipment the time series is related to.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        is_step: bool,
        time_series_type: Literal["numeric", "string"],
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        source_unit: str | None = None,
        unit: DirectRelationReference | None = None,
        assets: list[DirectRelationReference] | None = None,
        equipment: list[DirectRelationReference] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.is_step = is_step
        self.time_series_type = time_series_type
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.source_unit = source_unit
        self.unit = DirectRelationReference.load(unit) if unit else unit
        self.assets = [DirectRelationReference.load(a) for a in assets] if assets else assets
        self.equipment = [DirectRelationReference.load(e) for e in equipment] if equipment else equipment

    def as_write(self) -> CogniteTimeSeriesApply:
        return CogniteTimeSeriesApply(
            self.space,
            self.external_id,
            is_step=self.is_step,
            time_series_type=self.time_series_type,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            source_unit=self.source_unit,
            unit=self.unit,
            assets=self.assets,  # type: ignore[arg-type]
            equipment=self.equipment,  # type: ignore[arg-type]
            existing_version=self.version,
            type=self.type,
        )


class _CogniteUnitProperties:
    source_reference = PropertyOptions("sourceReference")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteUnit", "v1")


class CogniteUnitApply(_CogniteUnitProperties, TypedNodeApply):
    """This represents the writing format of Cognite unit.

    It is used when data is written to CDF.

    Represents a single unit of measurement

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite unit.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        symbol: The symbol for the unit of measurement
        quantity: Specifies the physical quantity the unit measures
        source: Source of the unit definition
        source_reference: Reference to the source of the unit definition
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        symbol: str | None | Omitted = OMITTED,
        quantity: str | None | Omitted = OMITTED,
        source: str | None | Omitted = OMITTED,
        source_reference: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.symbol = symbol
        self.quantity = quantity
        self.source = source
        self.source_reference = source_reference


class CogniteUnit(_CogniteUnitProperties, TypedNode):
    """This represents the reading format of Cognite unit.

    It is used when data is read from CDF.

    Represents a single unit of measurement

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite unit.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        symbol: The symbol for the unit of measurement
        quantity: Specifies the physical quantity the unit measures
        source: Source of the unit definition
        source_reference: Reference to the source of the unit definition
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        symbol: str | None = None,
        quantity: str | None = None,
        source: str | None = None,
        source_reference: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.symbol = symbol
        self.quantity = quantity
        self.source = source
        self.source_reference = source_reference

    def as_write(self) -> CogniteUnitApply:
        return CogniteUnitApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            symbol=self.symbol,
            quantity=self.quantity,
            source=self.source,
            source_reference=self.source_reference,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteVisualizableProperties:
    object_3d = PropertyOptions("object3D")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteVisualizable", "v1")


class CogniteVisualizableApply(_CogniteVisualizableProperties, TypedNodeApply):
    """This represents the writing format of Cognite visualizable.

    It is used when data is written to CDF.

    CogniteVisualizable defines the standard way to reference a related 3D resource

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite visualizable.
        object_3d: Direct relation to an Object3D instance representing the 3D resource
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        object_3d: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else object_3d


class CogniteVisualizable(_CogniteVisualizableProperties, TypedNode):
    """This represents the reading format of Cognite visualizable.

    It is used when data is read from CDF.

    CogniteVisualizable defines the standard way to reference a related 3D resource

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite visualizable.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        object_3d: Direct relation to an Object3D instance representing the 3D resource
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        object_3d: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else object_3d

    def as_write(self) -> CogniteVisualizableApply:
        return CogniteVisualizableApply(
            self.space,
            self.external_id,
            object_3d=self.object_3d,
            existing_version=self.version,
            type=self.type,
        )


class _Cognite360ImageAnnotationProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
    format_version = PropertyOptions("formatVersion")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "Cognite360ImageAnnotation", "v1")


class Cognite360ImageAnnotationApply(_Cognite360ImageAnnotationProperties, TypedEdgeApply):
    """This represents the writing format of Cognite 360 image annotation.

    It is used when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image annotation.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence: The confidence that the annotation is a good match
        status: The status of the annotation
        polygon: List of floats representing the polygon. Format depends on formatVersion
        format_version: Specifies the storage representation for the polygon
        existing_version: Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        confidence: float | None | Omitted = OMITTED,
        status: Literal["Approved", "Rejected", "Suggested"] | None | Omitted = OMITTED,
        polygon: list[float] | None | Omitted = OMITTED,
        format_version: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.confidence = confidence
        self.status = status
        self.polygon = polygon
        self.format_version = format_version


class Cognite360ImageAnnotation(_Cognite360ImageAnnotationProperties, TypedEdge):
    """This represents the reading format of Cognite 360 image annotation.

    It is used when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image annotation.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence: The confidence that the annotation is a good match
        status: The status of the annotation
        polygon: List of floats representing the polygon. Format depends on formatVersion
        format_version: Specifies the storage representation for the polygon
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
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
        TypedEdge.__init__(
            self, space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time
        )
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.confidence = confidence
        self.status = status
        self.polygon = polygon
        self.format_version = format_version

    def as_write(self) -> Cognite360ImageAnnotationApply:
        return Cognite360ImageAnnotationApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            confidence=self.confidence,
            status=self.status,
            polygon=self.polygon,
            format_version=self.format_version,
            existing_version=self.version,
        )


class Cognite3DTransformationEdgeApply(_Cognite3DTransformationProperties, TypedEdgeApply):
    """This represents the writing format of Cognite 3D transformation edge.

    It is used when data is written to CDF.

    The Cognite3DTransformation object defines a comprehensive 3D transformation, enabling precise adjustments to an object's position, orientation, and size in the 3D coordinate system. It allows for the translation of objects along the three spatial axes, rotation around these axes using Euler angles, and scaling along each axis to modify the object's dimensions. The object's transformation is defined in "CDF space", a coordinate system where the positive Z axis is the up direction

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation edge.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        existing_version: Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        translation_x: float | None | Omitted = OMITTED,
        translation_y: float | None | Omitted = OMITTED,
        translation_z: float | None | Omitted = OMITTED,
        euler_rotation_x: float | None | Omitted = OMITTED,
        euler_rotation_y: float | None | Omitted = OMITTED,
        euler_rotation_z: float | None | Omitted = OMITTED,
        scale_x: float | None | Omitted = OMITTED,
        scale_y: float | None | Omitted = OMITTED,
        scale_z: float | None | Omitted = OMITTED,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.translation_x = translation_x
        self.translation_y = translation_y
        self.translation_z = translation_z
        self.euler_rotation_x = euler_rotation_x
        self.euler_rotation_y = euler_rotation_y
        self.euler_rotation_z = euler_rotation_z
        self.scale_x = scale_x
        self.scale_y = scale_y
        self.scale_z = scale_z


class Cognite3DTransformationEdge(_Cognite3DTransformationProperties, TypedEdge):
    """This represents the reading format of Cognite 3D transformation edge.

    It is used when data is read from CDF.

    The Cognite3DTransformation object defines a comprehensive 3D transformation, enabling precise adjustments to an object's position, orientation, and size in the 3D coordinate system. It allows for the translation of objects along the three spatial axes, rotation around these axes using Euler angles, and scaling along each axis to modify the object's dimensions. The object's transformation is defined in "CDF space", a coordinate system where the positive Z axis is the up direction

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation edge.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
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
        TypedEdge.__init__(
            self, space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time
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

    def as_write(self) -> Cognite3DTransformationEdgeApply:
        return Cognite3DTransformationEdgeApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            existing_version=self.version,
        )


class _CogniteAnnotationProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm", "CogniteAnnotation", "v1")


class CogniteAnnotationApply(_CogniteAnnotationProperties, TypedEdgeApply):
    """This represents the writing format of Cognite annotation.

    It is used when data is written to CDF.

    Annotation represents contextualization results or links

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite annotation.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence: The confidence that the annotation is a good match
        status: The status of the annotation
        existing_version: Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        confidence: float | None | Omitted = OMITTED,
        status: Literal["Approved", "Rejected", "Suggested"] | None | Omitted = OMITTED,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.confidence = confidence
        self.status = status


class CogniteAnnotation(_CogniteAnnotationProperties, TypedEdge):
    """This represents the reading format of Cognite annotation.

    It is used when data is read from CDF.

    Annotation represents contextualization results or links

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite annotation.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence: The confidence that the annotation is a good match
        status: The status of the annotation
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        confidence: float | None = None,
        status: Literal["Approved", "Rejected", "Suggested"] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedEdge.__init__(
            self, space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time
        )
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.confidence = confidence
        self.status = status

    def as_write(self) -> CogniteAnnotationApply:
        return CogniteAnnotationApply(
            self.space,
            self.external_id,
            self.type,
            self.start_node,
            self.end_node,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            confidence=self.confidence,
            status=self.status,
            existing_version=self.version,
        )


class CogniteDescribableEdgeApply(_CogniteDescribableProperties, TypedEdgeApply):
    """This represents the writing format of Cognite describable edge.

    It is used when data is written to CDF.

    The describable core concept is used as a standard way of holding the bare minimum of information about the instance

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable edge.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        existing_version: Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases


class CogniteDescribableEdge(_CogniteDescribableProperties, TypedEdge):
    """This represents the reading format of Cognite describable edge.

    It is used when data is read from CDF.

    The describable core concept is used as a standard way of holding the bare minimum of information about the instance

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable edge.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedEdge.__init__(
            self, space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time
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
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            existing_version=self.version,
        )


class _CogniteDiagramAnnotationProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
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
        return ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1")


class CogniteDiagramAnnotationApply(_CogniteDiagramAnnotationProperties, TypedEdgeApply):
    """This represents the writing format of Cognite diagram annotation.

    It is used when data is written to CDF.

    Annotation for diagrams

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite diagram annotation.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence: The confidence that the annotation is a good match
        status: The status of the annotation
        start_node_page_number: The number of the page on which this annotation is located in `startNode` File. The first page has number 1
        end_node_page_number: The number of the page on which this annotation is located in the endNode File if an endNode is present. The first page has number 1
        start_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less than startNodeXMax
        start_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more than startNodeXMin
        start_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less than startNodeYMax
        start_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more than startNodeYMin
        start_node_text: The text extracted from within the bounding box on the startNode
        end_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less than endNodeXMax. Only applicable if an endNode is defined
        end_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more than endNodeXMin. Only applicable if an endNode is defined
        end_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less than endNodeYMax. Only applicable if an endNode is defined
        end_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more than endNodeYMin. Only applicable if an endNode is defined
        end_node_text: The text extracted from within the bounding box on the endNode. Only applicable if an endNode is defined
        existing_version: Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        name: str | None | Omitted = OMITTED,
        description: str | None | Omitted = OMITTED,
        tags: list[str] | None | Omitted = OMITTED,
        aliases: list[str] | None | Omitted = OMITTED,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        confidence: float | None | Omitted = OMITTED,
        status: Literal["Approved", "Rejected", "Suggested"] | None | Omitted = OMITTED,
        start_node_page_number: int | None | Omitted = OMITTED,
        end_node_page_number: int | None | Omitted = OMITTED,
        start_node_x_min: float | None | Omitted = OMITTED,
        start_node_x_max: float | None | Omitted = OMITTED,
        start_node_y_min: float | None | Omitted = OMITTED,
        start_node_y_max: float | None | Omitted = OMITTED,
        start_node_text: str | None | Omitted = OMITTED,
        end_node_x_min: float | None | Omitted = OMITTED,
        end_node_x_max: float | None | Omitted = OMITTED,
        end_node_y_min: float | None | Omitted = OMITTED,
        end_node_y_max: float | None | Omitted = OMITTED,
        end_node_text: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.confidence = confidence
        self.status = status
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


class CogniteDiagramAnnotation(_CogniteDiagramAnnotationProperties, TypedEdge):
    """This represents the reading format of Cognite diagram annotation.

    It is used when data is read from CDF.

    Annotation for diagrams

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite diagram annotation.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the instance
        description: Description of the instance
        tags: Text based labels for generic use, limited to 1000
        aliases: Alternative names for the node
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence: The confidence that the annotation is a good match
        status: The status of the annotation
        start_node_page_number: The number of the page on which this annotation is located in `startNode` File. The first page has number 1
        end_node_page_number: The number of the page on which this annotation is located in the endNode File if an endNode is present. The first page has number 1
        start_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less than startNodeXMax
        start_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more than startNodeXMin
        start_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less than startNodeYMax
        start_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more than startNodeYMin
        start_node_text: The text extracted from within the bounding box on the startNode
        end_node_x_min: Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less than endNodeXMax. Only applicable if an endNode is defined
        end_node_x_max: Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more than endNodeXMin. Only applicable if an endNode is defined
        end_node_y_min: Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less than endNodeYMax. Only applicable if an endNode is defined
        end_node_y_max: Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more than endNodeYMin. Only applicable if an endNode is defined
        end_node_text: The text extracted from within the bounding box on the endNode. Only applicable if an endNode is defined
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
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
        TypedEdge.__init__(
            self, space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time
        )
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.confidence = confidence
        self.status = status
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
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            confidence=self.confidence,
            status=self.status,
            start_node_page_number=self.start_node_page_number,
            end_node_page_number=self.end_node_page_number,
            start_node_x_min=self.start_node_x_min,
            start_node_x_max=self.start_node_x_max,
            start_node_y_min=self.start_node_y_min,
            start_node_y_max=self.start_node_y_max,
            start_node_text=self.start_node_text,
            end_node_x_min=self.end_node_x_min,
            end_node_x_max=self.end_node_x_max,
            end_node_y_min=self.end_node_y_min,
            end_node_y_max=self.end_node_y_max,
            end_node_text=self.end_node_text,
            existing_version=self.version,
        )


class CogniteSourceableEdgeApply(_CogniteSourceableProperties, TypedEdgeApply):
    """This represents the writing format of Cognite sourceable edge.

    It is used when data is written to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite sourceable edge.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        existing_version: Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        source_id: str | None | Omitted = OMITTED,
        source_context: str | None | Omitted = OMITTED,
        source: DirectRelationReference | tuple[str, str] | None | Omitted = OMITTED,
        source_created_time: datetime | None | Omitted = OMITTED,
        source_updated_time: datetime | None | Omitted = OMITTED,
        source_created_user: str | None | Omitted = OMITTED,
        source_updated_user: str | None | Omitted = OMITTED,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user


class CogniteSourceableEdge(_CogniteSourceableProperties, TypedEdge):
    """This represents the reading format of Cognite sourceable edge.

    It is used when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite sourceable edge.
        type: The type of edge.
        start_node: Reference to the direct relation. The reference consists of a space and an external-id.
        end_node: Reference to the direct relation. The reference consists of a space and an external-id.
        version: DMS version.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        source_id: Identifier from the source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source: Direct relation to a source system
        source_created_time: When the instance was created in source system (if available)
        source_updated_time: When the instance was last updated in the source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedEdge.__init__(
            self, space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time
        )
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else source
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
            source_id=self.source_id,
            source_context=self.source_context,
            source=self.source,
            source_created_time=self.source_created_time,
            source_updated_time=self.source_updated_time,
            source_created_user=self.source_created_user,
            source_updated_user=self.source_updated_user,
            existing_version=self.version,
        )
