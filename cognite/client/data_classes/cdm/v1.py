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


class CogniteDescribableProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteDescribable", "v1")


class CogniteDescribableNodeApply(CogniteDescribableProperties, TypedNodeApply):
    """This represents the writing format of cognite describable node.

    It is used to when data is written to CDF.

    The describable core concept is used as a standard way of holding the bare minimum of information about the instance


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite describable node.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases


class CogniteDescribableNode(CogniteDescribableProperties, TypedNode):
    """This represents the reading format of cognite describable node.

    It is used to when data is read from CDF.

    The describable core concept is used as a standard way of holding the bare minimum of information about the instance


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite describable node.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
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


class CogniteSchedulableProperties:
    start_time = PropertyOptions("startTime")
    end_time = PropertyOptions("endTime")
    scheduled_start_time = PropertyOptions("scheduledStartTime")
    scheduled_end_time = PropertyOptions("scheduledEndTime")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteSchedulable", "v1")


class CogniteSchedulableApply(CogniteSchedulableProperties, TypedNodeApply):
    """This represents the writing format of cognite schedulable.

    It is used to when data is written to CDF.

    CogniteSchedulable represents the metadata about when an activity (or similar) starts and ends.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite schedulable.
        start_time (datetime | None): The actual start time of an activity (or similar that extends this)
        end_time (datetime | None): The actual end time of an activity (or similar that extends this)
        scheduled_start_time (datetime | None): The planned start time of an activity (or similar that extends this)
        scheduled_end_time (datetime | None): The planned end time of an activity (or similar that extends this)
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scheduled_start_time: datetime | None = None,
        scheduled_end_time: datetime | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_start_time = scheduled_start_time
        self.scheduled_end_time = scheduled_end_time


class CogniteSchedulable(CogniteSchedulableProperties, TypedNode):
    """This represents the reading format of cognite schedulable.

    It is used to when data is read from CDF.

    CogniteSchedulable represents the metadata about when an activity (or similar) starts and ends.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite schedulable.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        start_time (datetime | None): The actual start time of an activity (or similar that extends this)
        end_time (datetime | None): The actual end time of an activity (or similar that extends this)
        scheduled_start_time (datetime | None): The planned start time of an activity (or similar that extends this)
        scheduled_end_time (datetime | None): The planned end time of an activity (or similar that extends this)
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
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
    """This represents the writing format of cognite sourceable node.

    It is used to when data is written to CDF.
    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite sourceable node.
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
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
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else None
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user


class CogniteSourceableNode(CogniteSourceableProperties, TypedNode):
    """This represents the reading format of cognite sourceable node.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite sourceable node.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else None
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


class CogniteVisualizableProperties:
    object_3d = PropertyOptions("object3D")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteVisualizable", "v1")


class CogniteVisualizableApply(CogniteVisualizableProperties, TypedNodeApply):
    """This represents the writing format of cognite visualizable.

    It is used to when data is written to CDF.

    CogniteVisualizable defines the standard way to reference a related 3D resource

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite visualizable.
        object_3d (DirectRelationReference | tuple[str, str] | None): Direct relation to an Object3D instance representing the 3D resource
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        object_3d: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else None


class CogniteVisualizable(CogniteVisualizableProperties, TypedNode):
    """This represents the reading format of cognite visualizable.

    It is used to when data is read from CDF.

    CogniteVisualizable defines the standard way to reference a related 3D resource

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite visualizable.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        object_3d (DirectRelationReference | None): Direct relation to an Object3D instance representing the 3D resource
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else None

    def as_write(self) -> CogniteVisualizableApply:
        return CogniteVisualizableApply(
            self.space,
            self.external_id,
            object_3d=self.object_3d,
            existing_version=self.version,
            type=self.type,
        )


class CogniteRevision3DProperties:
    type_ = PropertyOptions("type")
    model_3d = PropertyOptions("model3D")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteRevision3D", "v1")


class CogniteRevision3DApply(CogniteRevision3DProperties, TypedNodeApply):
    """This represents the writing format of cognite revision 3d.

    It is used to when data is written to CDF.

    Shared revision information for various 3D data types. Normally not used directly, but through CognitePointCloudRevision, Image360Collection or CogniteCADRevision


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite revision 3d.
        status (Literal["Done", "Failed", "Processing", "Queued"] | None): The status field.
        published (Literal["Done", "Failed", "Processing", "Queued"] | None): The published field.
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): The type field.
        model_3d (DirectRelationReference | tuple[str, str] | None): The model 3d field.
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.status = status
        self.published = published
        self.type_ = type_
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else None


class CogniteRevision3D(CogniteRevision3DProperties, TypedNode):
    """This represents the reading format of cognite revision 3d.

    It is used to when data is read from CDF.

    Shared revision information for various 3D data types. Normally not used directly, but through CognitePointCloudRevision, Image360Collection or CogniteCADRevision


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite revision 3d.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status (Literal["Done", "Failed", "Processing", "Queued"] | None): The status field.
        published (Literal["Done", "Failed", "Processing", "Queued"] | None): The published field.
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): The type field.
        model_3d (DirectRelationReference | None): The model 3d field.
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.status = status
        self.published = published
        self.type_ = type_
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else None

    def as_write(self) -> CogniteRevision3DApply:
        return CogniteRevision3DApply(
            self.space,
            self.external_id,
            status=self.status,
            published=self.published,
            type_=self.type_,
            model_3d=self.model_3d,
            existing_version=self.version,
            type=self.type,
        )


class CogniteCubeMapProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteCubeMap", "v1")


class CogniteCubeMapApply(CogniteCubeMapProperties, TypedNodeApply):
    """This represents the writing format of cognite cube map.

    It is used to when data is written to CDF.

    The cube map holds references to projections for a cube surrounding an 3D entity extending this, such as CogniteImage360


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite cube map.
        front (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the front projection of the cube map
        back (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the back projection of the cube map
        left (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the left projection of the cube map
        right (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the right projection of the cube map
        top (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the top projection of the cube map
        bottom (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the bottom projection of the cube map
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        front: DirectRelationReference | tuple[str, str] | None = None,
        back: DirectRelationReference | tuple[str, str] | None = None,
        left: DirectRelationReference | tuple[str, str] | None = None,
        right: DirectRelationReference | tuple[str, str] | None = None,
        top: DirectRelationReference | tuple[str, str] | None = None,
        bottom: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
        self.front = DirectRelationReference.load(front) if front else None
        self.back = DirectRelationReference.load(back) if back else None
        self.left = DirectRelationReference.load(left) if left else None
        self.right = DirectRelationReference.load(right) if right else None
        self.top = DirectRelationReference.load(top) if top else None
        self.bottom = DirectRelationReference.load(bottom) if bottom else None


class CogniteCubeMap(CogniteCubeMapProperties, TypedNode):
    """This represents the reading format of cognite cube map.

    It is used to when data is read from CDF.

    The cube map holds references to projections for a cube surrounding an 3D entity extending this, such as CogniteImage360


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite cube map.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        front (DirectRelationReference | None): Direct relation to a file holding the front projection of the cube map
        back (DirectRelationReference | None): Direct relation to a file holding the back projection of the cube map
        left (DirectRelationReference | None): Direct relation to a file holding the left projection of the cube map
        right (DirectRelationReference | None): Direct relation to a file holding the right projection of the cube map
        top (DirectRelationReference | None): Direct relation to a file holding the top projection of the cube map
        bottom (DirectRelationReference | None): Direct relation to a file holding the bottom projection of the cube map
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
        self.front = DirectRelationReference.load(front) if front else None
        self.back = DirectRelationReference.load(back) if back else None
        self.left = DirectRelationReference.load(left) if left else None
        self.right = DirectRelationReference.load(right) if right else None
        self.top = DirectRelationReference.load(top) if top else None
        self.bottom = DirectRelationReference.load(bottom) if bottom else None

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
    """This represents the writing format of cognite transformation 3 d node.

    It is used to when data is written to CDF.

    The CogniteTransformation3D object defines a comprehensive 3D transformation, enabling precise adjustments to an object's position, orientation, and size in 3D space. It allows for the translation of objects along the three spatial axes, rotation around these axes using Euler angles, and scaling along each axis to modify the object's dimensions


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite transformation 3 d node.
        translation_x (float | None): The displacement of the object along the X-axis in 3D space
        translation_y (float | None): The displacement of the object along the Y-axis in 3D space
        translation_z (float | None): The displacement of the object along the Z-axis in 3D space
        euler_rotation_x (float | None): The rotation of the object around the X-axis, measured in degrees
        euler_rotation_y (float | None): The rotation of the object around the Y-axis, measured in degrees
        euler_rotation_z (float | None): The rotation of the object around the Z-axis, measured in degrees
        scale_x (float | None): The scaling factor applied to the object along the X-axis
        scale_y (float | None): The scaling factor applied to the object along the Y-axis
        scale_z (float | None): The scaling factor applied to the object along the Z-axis
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
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
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, None, type)
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
    """This represents the reading format of cognite transformation 3 d node.

    It is used to when data is read from CDF.

    The CogniteTransformation3D object defines a comprehensive 3D transformation, enabling precise adjustments to an object's position, orientation, and size in 3D space. It allows for the translation of objects along the three spatial axes, rotation around these axes using Euler angles, and scaling along each axis to modify the object's dimensions


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite transformation 3 d node.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        translation_x (float | None): The displacement of the object along the X-axis in 3D space
        translation_y (float | None): The displacement of the object along the Y-axis in 3D space
        translation_z (float | None): The displacement of the object along the Z-axis in 3D space
        euler_rotation_x (float | None): The rotation of the object around the X-axis, measured in degrees
        euler_rotation_y (float | None): The rotation of the object around the Y-axis, measured in degrees
        euler_rotation_z (float | None): The rotation of the object around the Z-axis, measured in degrees
        scale_x (float | None): The scaling factor applied to the object along the X-axis
        scale_y (float | None): The scaling factor applied to the object along the Y-axis
        scale_z (float | None): The scaling factor applied to the object along the Z-axis
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, None, type)
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


class CogniteAssetClassProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteAssetClass", "v1")


class CogniteAssetClassApply(CogniteAssetClassProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite asset clas.

    It is used to when data is written to CDF.

    This identifies the class of an asset

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite asset clas.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        code (str | None): A unique identifier for the class of asset
        standard (str | None): Textual string for which standard the code is from
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        standard: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.code = code
        self.standard = standard


class CogniteAssetClass(CogniteAssetClassProperties, CogniteDescribableNode):
    """This represents the reading format of cognite asset clas.

    It is used to when data is read from CDF.

    This identifies the class of an asset

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite asset clas.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        code (str | None): A unique identifier for the class of asset
        standard (str | None): Textual string for which standard the code is from
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
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


class CogniteAssetTypeProperties:
    asset_class = PropertyOptions("assetClass")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteAssetType", "v1")


class CogniteAssetTypeApply(CogniteAssetTypeProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite asset type.

    It is used to when data is written to CDF.

    This identifies the type of an asset

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite asset type.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        code (str | None): A unique identifier for the type of asset
        asset_class (DirectRelationReference | tuple[str, str] | None): Class of this type, direct relation to CogniteAssetClass
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        code: str | None = None,
        asset_class: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.code = code
        self.asset_class = DirectRelationReference.load(asset_class) if asset_class else None


class CogniteAssetType(CogniteAssetTypeProperties, CogniteDescribableNode):
    """This represents the reading format of cognite asset type.

    It is used to when data is read from CDF.

    This identifies the type of an asset

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite asset type.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        code (str | None): A unique identifier for the type of asset
        asset_class (DirectRelationReference | None): Class of this type, direct relation to CogniteAssetClass
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        asset_class: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        self.code = code
        self.asset_class = DirectRelationReference.load(asset_class) if asset_class else None

    def as_write(self) -> CogniteAssetTypeApply:
        return CogniteAssetTypeApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            code=self.code,
            asset_class=self.asset_class,
            existing_version=self.version,
            type=self.type,
        )


class CogniteCADNodeProperties:
    object_3d = PropertyOptions("object3D")
    model_3d = PropertyOptions("model3D")
    cad_node_reference = PropertyOptions("cadNodeReference")
    tree_indexes = PropertyOptions("treeIndexes")
    sub_tree_sizes = PropertyOptions("subTreeSizes")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteCADNode", "v1")


class CogniteCADNodeApply(CogniteCADNodeProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite cad node.

    It is used to when data is written to CDF.

    Represents nodes from the 3D model that have been contextualized

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite cad node.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        object_3d (DirectRelationReference | tuple[str, str] | None): Direct relation to object3D grouping for this node
        model_3d (DirectRelationReference | tuple[str, str] | None): Direct relation to CogniteModel3D
        cad_node_reference (str | None): Reference to a node within a CAD model from the 3D API
        revisions (list[DirectRelationReference | tuple[str, str]] | None): List of direct relations to instances of CogniteRevision3D which this CogniteCADNode exists in.
        tree_indexes (list[int] | None): List of tree indexes in the same order as revisions. Used by Reveal and similar applications to map from CogniteCADNode to tree index
        sub_tree_sizes (list[int] | None): List of subtree sizes in the same order as revisions. Used by Reveal and similar applications to know how many nodes exists below this node in the hierarchy
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        object_3d: DirectRelationReference | tuple[str, str] | None = None,
        model_3d: DirectRelationReference | tuple[str, str] | None = None,
        cad_node_reference: str | None = None,
        revisions: list[DirectRelationReference | tuple[str, str]] | None = None,
        tree_indexes: list[int] | None = None,
        sub_tree_sizes: list[int] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else None
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else None
        self.cad_node_reference = cad_node_reference
        self.revisions = [DirectRelationReference.load(revision) for revision in revisions] if revisions else None
        self.tree_indexes = tree_indexes
        self.sub_tree_sizes = sub_tree_sizes


class CogniteCADNode(CogniteCADNodeProperties, CogniteDescribableNode):
    """This represents the reading format of cognite cad node.

    It is used to when data is read from CDF.

    Represents nodes from the 3D model that have been contextualized

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite cad node.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        object_3d (DirectRelationReference | None): Direct relation to object3D grouping for this node
        model_3d (DirectRelationReference | None): Direct relation to CogniteModel3D
        cad_node_reference (str | None): Reference to a node within a CAD model from the 3D API
        revisions (list[DirectRelationReference] | None): List of direct relations to instances of CogniteRevision3D which this CogniteCADNode exists in.
        tree_indexes (list[int] | None): List of tree indexes in the same order as revisions. Used by Reveal and similar applications to map from CogniteCADNode to tree index
        sub_tree_sizes (list[int] | None): List of subtree sizes in the same order as revisions. Used by Reveal and similar applications to know how many nodes exists below this node in the hierarchy
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else None
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else None
        self.cad_node_reference = cad_node_reference
        self.revisions = [DirectRelationReference.load(revision) for revision in revisions] if revisions else None
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


class CogniteEquipmentTypeProperties:
    equipment_class = PropertyOptions("equipmentClass")
    standard_reference = PropertyOptions("standardReference")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteEquipmentType", "v1")


class CogniteEquipmentTypeApply(CogniteEquipmentTypeProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite equipment type.

    It is used to when data is written to CDF.

    This identifies the type of an equipment

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite equipment type.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        code (str | None): A unique identifier for the type of equipment
        equipment_class (str | None): Class of equipment
        standard (str | None): Identifier for which standard this equipment type is sourced from, such as ISO14224 or similar
        standard_reference (str | None): Reference to the source of the equipment specification
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
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
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.code = code
        self.equipment_class = equipment_class
        self.standard = standard
        self.standard_reference = standard_reference


class CogniteEquipmentType(CogniteEquipmentTypeProperties, CogniteDescribableNode):
    """This represents the reading format of cognite equipment type.

    It is used to when data is read from CDF.

    This identifies the type of an equipment

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite equipment type.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        code (str | None): A unique identifier for the type of equipment
        equipment_class (str | None): Class of equipment
        standard (str | None): Identifier for which standard this equipment type is sourced from, such as ISO14224 or similar
        standard_reference (str | None): Reference to the source of the equipment specification
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
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


class CogniteFileCategoryProperties:
    standard_reference = PropertyOptions("standardReference")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteFileCategory", "v1")


class CogniteFileCategoryApply(CogniteFileCategoryProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite file category.

    It is used to when data is written to CDF.

    This identifies the category of file as found through contextualization/categorization

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite file category.
        code (str): Identified category code, such as 'AA' for Accounting (from Norsok)
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        standard (str | None): Name of the standard the category originates from, such as 'Norsok'
        standard_reference (str | None): Reference to the source of the category standard
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
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
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.code = code
        self.standard = standard
        self.standard_reference = standard_reference


class CogniteFileCategory(CogniteFileCategoryProperties, CogniteDescribableNode):
    """This represents the reading format of cognite file category.

    It is used to when data is read from CDF.

    This identifies the category of file as found through contextualization/categorization

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite file category.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        code (str): Identified category code, such as 'AA' for Accounting (from Norsok)
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        standard (str | None): Name of the standard the category originates from, such as 'Norsok'
        standard_reference (str | None): Reference to the source of the category standard
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        self.code = code
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


class CogniteImage360StationProperties:
    group_type = PropertyOptions("groupType")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360Station", "v1")


class CogniteImage360StationApply(CogniteImage360StationProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite image 360 station.

    It is used to when data is written to CDF.

    Navigational aid for traversing multiple CogniteImage360 instances for a single station

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360 station.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        group_type (Literal['Station360'] | None): Type of group
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        group_type: Literal["Station360"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.group_type = group_type


class CogniteImage360Station(CogniteImage360StationProperties, CogniteDescribableNode):
    """This represents the reading format of cognite image 360 station.

    It is used to when data is read from CDF.

    Navigational aid for traversing multiple CogniteImage360 instances for a single station

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360 station.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        group_type (Literal['Station360'] | None): Type of group
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        self.group_type = group_type

    def as_write(self) -> CogniteImage360StationApply:
        return CogniteImage360StationApply(
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


class CogniteModel3DProperties:
    type_ = PropertyOptions("type")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteModel3D", "v1")


class CogniteModel3DApply(CogniteModel3DProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite model 3d.

    It is used to when data is written to CDF.

    Groups revisions of 3D data of various kinds together (CAD, PointCloud, CogniteImage360)

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite model 3d.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): CAD, PointCloud or CogniteImage360
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.type_ = type_


class CogniteModel3D(CogniteModel3DProperties, CogniteDescribableNode):
    """This represents the reading format of cognite model 3d.

    It is used to when data is read from CDF.

    Groups revisions of 3D data of various kinds together (CAD, PointCloud, CogniteImage360)

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite model 3d.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): CAD, PointCloud or CogniteImage360
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        self.type_ = type_

    def as_write(self) -> CogniteModel3DApply:
        return CogniteModel3DApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            type_=self.type_,
            existing_version=self.version,
            type=self.type,
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
    """This represents the writing format of cognite object 3d.

    It is used to when data is written to CDF.

    This is a virtual representation of an object in world space, tied to an asset and 3D resources.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite object 3d.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        x_min (float | None): Lowest X value
        x_max (float | None): Highest X value
        y_min (float | None): Lowest Y value
        y_max (float | None): Highest Y value
        z_min (float | None): Lowest Z value
        z_max (float | None): Highest Z value
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
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
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max
        self.z_min = z_min
        self.z_max = z_max


class CogniteObject3D(CogniteObject3DProperties, CogniteDescribableNode):
    """This represents the reading format of cognite object 3d.

    It is used to when data is read from CDF.

    This is a virtual representation of an object in world space, tied to an asset and 3D resources.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite object 3d.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        x_min (float | None): Lowest X value
        x_max (float | None): Highest X value
        y_min (float | None): Lowest Y value
        y_max (float | None): Highest Y value
        z_min (float | None): Lowest Z value
        z_max (float | None): Highest Z value
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
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


class CognitePointCloudVolumeProperties:
    object_3d = PropertyOptions("object3D")
    model_3d = PropertyOptions("model3D")
    volume_references = PropertyOptions("volumeReferences")
    volume_type = PropertyOptions("volumeType")
    format_version = PropertyOptions("formatVersion")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CognitePointCloudVolume", "v1")


class CognitePointCloudVolumeApply(CognitePointCloudVolumeProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite point cloud volume.

    It is used to when data is written to CDF.

    PointCloud volume definition

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite point cloud volume.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        object_3d (DirectRelationReference | tuple[str, str] | None): Direct relation to object3D grouping for this node
        model_3d (DirectRelationReference | tuple[str, str] | None): Direct relation to CogniteModel3D instance
        volume_references (list[str] | None): Unique volume metric hashes used to access the 3D specialized data storage
        revisions (list[DirectRelationReference | tuple[str, str]] | None): List of direct relations to revision information
        volume_type (Literal["Box", "Cylinder"] | None): Type of volume (Cylinder or Box)
        volume (list[float] | None): Relevant coordinates for the volume type, 9 floats in total, that defines the volume
        format_version (str | None): Specifies the version the 'volume' field is following. Volume definition is today 9 floats (property volume)
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        object_3d: DirectRelationReference | tuple[str, str] | None = None,
        model_3d: DirectRelationReference | tuple[str, str] | None = None,
        volume_references: list[str] | None = None,
        revisions: list[DirectRelationReference | tuple[str, str]] | None = None,
        volume_type: Literal["Box", "Cylinder"] | None = None,
        volume: list[float] | None = None,
        format_version: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else None
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else None
        self.volume_references = volume_references
        self.revisions = [DirectRelationReference.load(revision) for revision in revisions] if revisions else None
        self.volume_type = volume_type
        self.volume = volume
        self.format_version = format_version


class CognitePointCloudVolume(CognitePointCloudVolumeProperties, CogniteDescribableNode):
    """This represents the reading format of cognite point cloud volume.

    It is used to when data is read from CDF.

    PointCloud volume definition

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite point cloud volume.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        object_3d (DirectRelationReference | None): Direct relation to object3D grouping for this node
        model_3d (DirectRelationReference | None): Direct relation to CogniteModel3D instance
        volume_references (list[str] | None): Unique volume metric hashes used to access the 3D specialized data storage
        revisions (list[DirectRelationReference] | None): List of direct relations to revision information
        volume_type (Literal["Box", "Cylinder"] | None): Type of volume (Cylinder or Box)
        volume (list[float] | None): Relevant coordinates for the volume type, 9 floats in total, that defines the volume
        format_version (str | None): Specifies the version the 'volume' field is following. Volume definition is today 9 floats (property volume)
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        self.object_3d = DirectRelationReference.load(object_3d) if object_3d else None
        self.model_3d = DirectRelationReference.load(model_3d) if model_3d else None
        self.volume_references = volume_references
        self.revisions = [DirectRelationReference.load(revision) for revision in revisions] if revisions else None
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


class CogniteSourceSystemProperties:
    version_ = PropertyOptions("version")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteSourceSystem", "v1")


class CogniteSourceSystemApply(CogniteSourceSystemProperties, CogniteDescribableNodeApply):
    """This represents the writing format of cognite source system.

    It is used to when data is written to CDF.

    The CogniteSourceSystem core concept is used to standardize the way source system is stored.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite source system.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        version_ (str | None): Version identifier for the source system
        manufacturer (str | None): Manufacturer of the source system
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        version_: str | None = None,
        manufacturer: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        self.version_ = version_
        self.manufacturer = manufacturer


class CogniteSourceSystem(CogniteSourceSystemProperties, CogniteDescribableNode):
    """This represents the reading format of cognite source system.

    It is used to when data is read from CDF.

    The CogniteSourceSystem core concept is used to standardize the way source system is stored.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite source system.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        version_ (str | None): Version identifier for the source system
        manufacturer (str | None): Manufacturer of the source system
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        version_: str | None = None,
        manufacturer: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        self.version_ = version_
        self.manufacturer = manufacturer

    def as_write(self) -> CogniteSourceSystemApply:
        return CogniteSourceSystemApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            version_=self.version_,
            manufacturer=self.manufacturer,
            existing_version=self.version,
            type=self.type,
        )


class CogniteActivityProperties:
    time_series = PropertyOptions("timeSeries")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteActivity", "v1")


class CogniteActivityApply(
    CogniteActivityProperties, CogniteDescribableNodeApply, CogniteSourceableNodeApply, CogniteSchedulableApply
):
    """This represents the writing format of cognite activity.

    It is used to when data is written to CDF.

    Represent an activity

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite activity.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        start_time (datetime | None): The actual start time of an activity (or similar that extends this)
        end_time (datetime | None): The actual end time of an activity (or similar that extends this)
        scheduled_start_time (datetime | None): The planned start time of an activity (or similar that extends this)
        scheduled_end_time (datetime | None): The planned end time of an activity (or similar that extends this)
        assets (list[DirectRelationReference | tuple[str, str]] | None): List of assets this activity relates to
        equipment (list[DirectRelationReference | tuple[str, str]] | None): The list of equipment this activity relates to
        time_series (list[DirectRelationReference | tuple[str, str]] | None): The list of time series this activity relates to
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
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
            self,
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            existing_version=existing_version,
            type=type,
        )
        CogniteSchedulableApply.__init__(
            self,
            space,
            external_id,
            start_time=start_time,
            end_time=end_time,
            scheduled_start_time=scheduled_start_time,
            scheduled_end_time=scheduled_end_time,
            existing_version=existing_version,
            type=type,
        )
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.equipment = [DirectRelationReference.load(equipment) for equipment in equipment] if equipment else None
        self.time_series = (
            [DirectRelationReference.load(time_series) for time_series in time_series] if time_series else None
        )


class CogniteActivity(CogniteActivityProperties, CogniteDescribableNode, CogniteSourceableNode, CogniteSchedulable):
    """This represents the reading format of cognite activity.

    It is used to when data is read from CDF.

    Represent an activity

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite activity.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        start_time (datetime | None): The actual start time of an activity (or similar that extends this)
        end_time (datetime | None): The actual end time of an activity (or similar that extends this)
        scheduled_start_time (datetime | None): The planned start time of an activity (or similar that extends this)
        scheduled_end_time (datetime | None): The planned end time of an activity (or similar that extends this)
        assets (list[DirectRelationReference] | None): List of assets this activity relates to
        equipment (list[DirectRelationReference] | None): The list of equipment this activity relates to
        time_series (list[DirectRelationReference] | None): The list of time series this activity relates to
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        CogniteDescribableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteSchedulable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            start_time=start_time,
            end_time=end_time,
            scheduled_start_time=scheduled_start_time,
            scheduled_end_time=scheduled_end_time,
            type=type,
            deleted_time=deleted_time,
        )
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.equipment = [DirectRelationReference.load(equipment) for equipment in equipment] if equipment else None
        self.time_series = (
            [DirectRelationReference.load(time_series) for time_series in time_series] if time_series else None
        )

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


class CogniteEquipmentProperties:
    serial_number = PropertyOptions("serialNumber")
    equipment_type = PropertyOptions("equipmentType")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteEquipment", "v1")


class CogniteEquipmentApply(CogniteEquipmentProperties, CogniteDescribableNodeApply, CogniteSourceableNodeApply):
    """This represents the writing format of cognite equipment.

    It is used to when data is written to CDF.

    Represent a physical piece of equipment

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite equipment.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        serial_number (str | None): Serial number of the equipment
        manufacturer (str | None): Manufacturer of the equipment
        equipment_type (DirectRelationReference | tuple[str, str] | None): Type of this equipment, direct relation to CogniteEquipmentType
        files (list[DirectRelationReference | tuple[str, str]] | None): List of files this equipment relates to
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
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
            self,
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            existing_version=existing_version,
            type=type,
        )
        self.serial_number = serial_number
        self.manufacturer = manufacturer
        self.equipment_type = DirectRelationReference.load(equipment_type) if equipment_type else None
        self.files = [DirectRelationReference.load(file) for file in files] if files else None


class CogniteEquipment(CogniteEquipmentProperties, CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading format of cognite equipment.

    It is used to when data is read from CDF.

    Represent a physical piece of equipment

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite equipment.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        serial_number (str | None): Serial number of the equipment
        manufacturer (str | None): Manufacturer of the equipment
        equipment_type (DirectRelationReference | None): Type of this equipment, direct relation to CogniteEquipmentType
        files (list[DirectRelationReference] | None): List of files this equipment relates to
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        serial_number: str | None = None,
        manufacturer: str | None = None,
        equipment_type: DirectRelationReference | None = None,
        files: list[DirectRelationReference] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            type=type,
            deleted_time=deleted_time,
        )
        self.serial_number = serial_number
        self.manufacturer = manufacturer
        self.equipment_type = DirectRelationReference.load(equipment_type) if equipment_type else None
        self.files = [DirectRelationReference.load(file) for file in files] if files else None

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
            serial_number=self.serial_number,
            manufacturer=self.manufacturer,
            equipment_type=self.equipment_type,
            files=self.files,  # type: ignore[arg-type]
            existing_version=self.version,
            type=self.type,
        )


class CogniteFileProperties:
    mime_type = PropertyOptions("mimeType")
    is_uploaded = PropertyOptions("isUploaded")
    uploaded_time = PropertyOptions("uploadedTime")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteFile", "v1")


class CogniteFileApply(CogniteFileProperties, CogniteDescribableNodeApply, CogniteSourceableNodeApply):
    """This represents the writing format of cognite file.

    It is used to when data is written to CDF.

    This concept models the underlying file

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite file.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        assets (list[DirectRelationReference | tuple[str, str]] | None): List of assets this file relates to
        mime_type (str | None): MIME type of the file
        directory (str | None): Contains the path elements from the source (for when the source system has a file system hierarchy or similar)
        is_uploaded (bool | None): Whether the file content has been uploaded to Cognite Data Fusion
        uploaded_time (datetime | None): Point in time when the file upload was completed and the file was made available
        category (DirectRelationReference | tuple[str, str] | None): Direct relation to an instance of CogniteFileCategory representing the detected categorization/class for the file
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
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
            self,
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            existing_version=existing_version,
            type=type,
        )
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.mime_type = mime_type
        self.directory = directory
        self.is_uploaded = is_uploaded
        self.uploaded_time = uploaded_time
        self.category = DirectRelationReference.load(category) if category else None


class CogniteFile(CogniteFileProperties, CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading format of cognite file.

    It is used to when data is read from CDF.

    This concept models the underlying file

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite file.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        assets (list[DirectRelationReference] | None): List of assets this file relates to
        mime_type (str | None): MIME type of the file
        directory (str | None): Contains the path elements from the source (for when the source system has a file system hierarchy or similar)
        is_uploaded (bool | None): Whether the file content has been uploaded to Cognite Data Fusion
        uploaded_time (datetime | None): Point in time when the file upload was completed and the file was made available
        category (DirectRelationReference | None): Direct relation to an instance of CogniteFileCategory representing the detected categorization/class for the file
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        assets: list[DirectRelationReference] | None = None,
        mime_type: str | None = None,
        directory: str | None = None,
        is_uploaded: bool | None = None,
        uploaded_time: datetime | None = None,
        category: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            type=type,
            deleted_time=deleted_time,
        )
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.mime_type = mime_type
        self.directory = directory
        self.is_uploaded = is_uploaded
        self.uploaded_time = uploaded_time
        self.category = DirectRelationReference.load(category) if category else None

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
            is_uploaded=self.is_uploaded,
            uploaded_time=self.uploaded_time,
            category=self.category,
            existing_version=self.version,
            type=self.type,
        )


class CogniteTimeSeriesProperties:
    type_ = PropertyOptions("type")
    is_step = PropertyOptions("isStep")
    source_unit = PropertyOptions("sourceUnit")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteTimeSeries", "v1")


class CogniteTimeSeriesApply(CogniteTimeSeriesProperties, CogniteDescribableNodeApply, CogniteSourceableNodeApply):
    """This represents the writing format of cognite time series.

    It is used to when data is written to CDF.
    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite time series.
        type_ (Literal["numeric", "string"]): Defines data type of the data points.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        is_step (bool | None): Defines whether the time series is a step series or not.
        source_unit (str | None): Unit as specified in the source system
        unit (DirectRelationReference | tuple[str, str] | None): direct relation to unit in the `cdf_units` space
        assets (list[DirectRelationReference | tuple[str, str]] | None): The asset field.
        equipment (list[DirectRelationReference | tuple[str, str]] | None): The equipment field.
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
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
            self,
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            existing_version=existing_version,
            type=type,
        )
        self.type_ = type_
        self.is_step = is_step
        self.source_unit = source_unit
        self.unit = DirectRelationReference.load(unit) if unit else None
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.equipment = [DirectRelationReference.load(equipment) for equipment in equipment] if equipment else None


class CogniteTimeSeries(CogniteTimeSeriesProperties, CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading format of cognite time series.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite time series.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type_ (Literal["numeric", "string"]): Defines data type of the data points.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        is_step (bool | None): Defines whether the time series is a step series or not.
        source_unit (str | None): Unit as specified in the source system
        unit (DirectRelationReference | None): direct relation to unit in the `cdf_units` space
        assets (list[DirectRelationReference] | None): The asset field.
        equipment (list[DirectRelationReference] | None): The equipment field.
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        type_: Literal["numeric", "string"],
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
        is_step: bool | None = None,
        source_unit: str | None = None,
        unit: DirectRelationReference | None = None,
        assets: list[DirectRelationReference] | None = None,
        equipment: list[DirectRelationReference] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            type=type,
            deleted_time=deleted_time,
        )
        self.type_ = type_
        self.is_step = is_step
        self.source_unit = source_unit
        self.unit = DirectRelationReference.load(unit) if unit else None
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.equipment = [DirectRelationReference.load(equipment) for equipment in equipment] if equipment else None

    def as_write(self) -> CogniteTimeSeriesApply:
        return CogniteTimeSeriesApply(
            self.space,
            self.external_id,
            type_=self.type_,
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
            is_step=self.is_step,
            source_unit=self.source_unit,
            unit=self.unit,
            assets=self.assets,  # type: ignore[arg-type]
            equipment=self.equipment,  # type: ignore[arg-type]
            existing_version=self.version,
            type=self.type,
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
    """This represents the writing format of cognite asset.

    It is used to when data is written to CDF.

    The asset is the bare bone representation of assets in our asset centric world

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite asset.
        object_3d (DirectRelationReference | tuple[str, str] | None): Direct relation to an Object3D instance representing the 3D resource
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        parent (DirectRelationReference | tuple[str, str] | None): Parent of this asset
        root (DirectRelationReference | tuple[str, str] | None): Asset at the top of the hierarchy.
        path (list[DirectRelationReference | tuple[str, str]] | None): Materialized path of this asset
        last_path_materialization_time (datetime | None): Last time the path materializer updated the path of this asset
        equipment (DirectRelationReference | tuple[str, str] | None): Equipment associated with this asset
        asset_class (DirectRelationReference | tuple[str, str] | None): Class of this asset
        type_ (DirectRelationReference | tuple[str, str] | None): Type of this asset
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        object_3d: DirectRelationReference | tuple[str, str] | None = None,
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
        CogniteVisualizableApply.__init__(
            self, space, external_id, object_3d=object_3d, existing_version=existing_version, type=type
        )
        CogniteDescribableNodeApply.__init__(
            self,
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        CogniteSourceableNodeApply.__init__(
            self,
            space,
            external_id,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            existing_version=existing_version,
            type=type,
        )
        self.parent = DirectRelationReference.load(parent) if parent else None
        self.root = DirectRelationReference.load(root) if root else None
        self.path = [DirectRelationReference.load(path) for path in path] if path else None
        self.last_path_materialization_time = last_path_materialization_time
        self.equipment = DirectRelationReference.load(equipment) if equipment else None
        self.asset_class = DirectRelationReference.load(asset_class) if asset_class else None
        self.type_ = DirectRelationReference.load(type_) if type_ else None


class CogniteAsset(CogniteAssetProperties, CogniteVisualizable, CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading format of cognite asset.

    It is used to when data is read from CDF.

    The asset is the bare bone representation of assets in our asset centric world

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite asset.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        object_3d (DirectRelationReference | None): Direct relation to an Object3D instance representing the 3D resource
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        parent (DirectRelationReference | None): Parent of this asset
        root (DirectRelationReference | None): Asset at the top of the hierarchy.
        path (list[DirectRelationReference] | None): Materialized path of this asset
        last_path_materialization_time (datetime | None): Last time the path materializer updated the path of this asset
        equipment (DirectRelationReference | None): Equipment associated with this asset
        asset_class (DirectRelationReference | None): Class of this asset
        type_ (DirectRelationReference | None): Type of this asset
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        last_path_materialization_time: datetime | None = None,
        equipment: DirectRelationReference | None = None,
        asset_class: DirectRelationReference | None = None,
        type_: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteVisualizable.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            object_3d=object_3d,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteDescribableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteSourceableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            type=type,
            deleted_time=deleted_time,
        )
        self.parent = DirectRelationReference.load(parent) if parent else None
        self.root = DirectRelationReference.load(root) if root else None
        self.path = [DirectRelationReference.load(path) for path in path] if path else None
        self.last_path_materialization_time = last_path_materialization_time
        self.equipment = DirectRelationReference.load(equipment) if equipment else None
        self.asset_class = DirectRelationReference.load(asset_class) if asset_class else None
        self.type_ = DirectRelationReference.load(type_) if type_ else None

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
            root=self.root,
            path=self.path,  # type: ignore[arg-type]
            last_path_materialization_time=self.last_path_materialization_time,
            equipment=self.equipment,
            asset_class=self.asset_class,
            type_=self.type_,
            existing_version=self.version,
            type=self.type,
        )


class CogniteCADRevisionProperties:
    revision_id = PropertyOptions("revisionId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteCADRevision", "v1")


class CogniteCADRevisionApply(CogniteCADRevisionProperties, CogniteRevision3DApply):
    """This represents the writing format of cognite cad revision.

    It is used to when data is written to CDF.
    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite cad revision.
        status (Literal["Done", "Failed", "Processing", "Queued"] | None): The status field.
        published (Literal["Done", "Failed", "Processing", "Queued"] | None): The published field.
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): The type field.
        model_3d (DirectRelationReference | tuple[str, str] | None): .
        revision_id (int | None): The 3D API revision identifier for this CAD model
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | tuple[str, str] | None = None,
        revision_id: int | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            status=status,
            published=published,
            type_=type_,
            model_3d=model_3d,
            existing_version=existing_version,
            type=type,
        )
        self.revision_id = revision_id


class CogniteCADRevision(CogniteCADRevisionProperties, CogniteRevision3D):
    """This represents the reading format of cognite cad revision.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite cad revision.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status (Literal["Done", "Failed", "Processing", "Queued"] | None): The status field.
        published (Literal["Done", "Failed", "Processing", "Queued"] | None): The published field.
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): The type field.
        model_3d (DirectRelationReference | None): .
        revision_id (int | None): The 3D API revision identifier for this CAD model
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | None = None,
        revision_id: int | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            status=status,
            published=published,
            type_=type_,
            model_3d=model_3d,
            type=type,
            deleted_time=deleted_time,
        )
        self.revision_id = revision_id

    def as_write(self) -> CogniteCADRevisionApply:
        return CogniteCADRevisionApply(
            self.space,
            self.external_id,
            status=self.status,
            published=self.published,
            type_=self.type_,
            model_3d=self.model_3d,
            revision_id=self.revision_id,
            existing_version=self.version,
            type=self.type,
        )


class CogniteImage360CollectionProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360Collection", "v1")


class CogniteImage360CollectionApply(
    CogniteImage360CollectionProperties, CogniteDescribableNodeApply, CogniteRevision3DApply
):
    """This represents the writing format of cognite image 360 collection.

    It is used to when data is written to CDF.

    Represents a logical collection of CogniteImage360 instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360 collection.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        status (Literal["Done", "Failed", "Processing", "Queued"] | None): The status field.
        published (Literal["Done", "Failed", "Processing", "Queued"] | None): The published field.
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): The type field.
        model_3d (DirectRelationReference | tuple[str, str] | None): The model 3d field.
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | tuple[str, str] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        CogniteDescribableNodeApply.__init__(
            self,
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
            type=type,
        )
        CogniteRevision3DApply.__init__(
            self,
            space,
            external_id,
            status=status,
            published=published,
            type_=type_,
            model_3d=model_3d,
            existing_version=existing_version,
            type=type,
        )


class CogniteImage360Collection(CogniteImage360CollectionProperties, CogniteDescribableNode, CogniteRevision3D):
    """This represents the reading format of cognite image 360 collection.

    It is used to when data is read from CDF.

    Represents a logical collection of CogniteImage360 instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360 collection.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        status (Literal["Done", "Failed", "Processing", "Queued"] | None): The status field.
        published (Literal["Done", "Failed", "Processing", "Queued"] | None): The published field.
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): The type field.
        model_3d (DirectRelationReference | None): The model 3d field.
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        CogniteDescribableNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteRevision3D.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            status=status,
            published=published,
            type_=type_,
            model_3d=model_3d,
            type=type,
            deleted_time=deleted_time,
        )

    def as_write(self) -> CogniteImage360CollectionApply:
        return CogniteImage360CollectionApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            status=self.status,
            published=self.published,
            type_=self.type_,
            model_3d=self.model_3d,
            existing_version=self.version,
            type=self.type,
        )


class CognitePointCloudRevisionProperties:
    revision_id = PropertyOptions("revisionId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CognitePointCloudRevision", "v1")


class CognitePointCloudRevisionApply(CognitePointCloudRevisionProperties, CogniteRevision3DApply):
    """This represents the writing format of cognite point cloud revision.

    It is used to when data is written to CDF.

    Navigational aid for traversing CognitePointCloudRevision instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite point cloud revision.
        status (Literal["Done", "Failed", "Processing", "Queued"] | None): The status field.
        published (Literal["Done", "Failed", "Processing", "Queued"] | None): The published field.
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): The type field.
        model_3d (DirectRelationReference | tuple[str, str] | None): .
        revision_id (int | None): The 3D API revision identifier for this PointCloud model
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        status: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | tuple[str, str] | None = None,
        revision_id: int | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            status=status,
            published=published,
            type_=type_,
            model_3d=model_3d,
            existing_version=existing_version,
            type=type,
        )
        self.revision_id = revision_id


class CognitePointCloudRevision(CognitePointCloudRevisionProperties, CogniteRevision3D):
    """This represents the reading format of cognite point cloud revision.

    It is used to when data is read from CDF.

    Navigational aid for traversing CognitePointCloudRevision instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite point cloud revision.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        status (Literal["Done", "Failed", "Processing", "Queued"] | None): The status field.
        published (Literal["Done", "Failed", "Processing", "Queued"] | None): The published field.
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): The type field.
        model_3d (DirectRelationReference | None): .
        revision_id (int | None): The 3D API revision identifier for this PointCloud model
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        published: Literal["Done", "Failed", "Processing", "Queued"] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        model_3d: DirectRelationReference | None = None,
        revision_id: int | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            status=status,
            published=published,
            type_=type_,
            model_3d=model_3d,
            type=type,
            deleted_time=deleted_time,
        )
        self.revision_id = revision_id

    def as_write(self) -> CognitePointCloudRevisionApply:
        return CognitePointCloudRevisionApply(
            self.space,
            self.external_id,
            status=self.status,
            published=self.published,
            type_=self.type_,
            model_3d=self.model_3d,
            revision_id=self.revision_id,
            existing_version=self.version,
            type=self.type,
        )


class CogniteImage360Properties:
    collection_360 = PropertyOptions("collection360")
    station_360 = PropertyOptions("station360")
    taken_at = PropertyOptions("takenAt")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360", "v1")


class CogniteImage360Apply(CogniteImage360Properties, CogniteTransformation3DNodeApply, CogniteCubeMapApply):
    """This represents the writing format of cognite image 360.

    It is used to when data is written to CDF.
    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360.
        translation_x (float | None): The displacement of the object along the X-axis in 3D space
        translation_y (float | None): The displacement of the object along the Y-axis in 3D space
        translation_z (float | None): The displacement of the object along the Z-axis in 3D space
        euler_rotation_x (float | None): The rotation of the object around the X-axis, measured in degrees
        euler_rotation_y (float | None): The rotation of the object around the Y-axis, measured in degrees
        euler_rotation_z (float | None): The rotation of the object around the Z-axis, measured in degrees
        scale_x (float | None): The scaling factor applied to the object along the X-axis
        scale_y (float | None): The scaling factor applied to the object along the Y-axis
        scale_z (float | None): The scaling factor applied to the object along the Z-axis
        front (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the front projection of the cube map
        back (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the back projection of the cube map
        left (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the left projection of the cube map
        right (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the right projection of the cube map
        top (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the top projection of the cube map
        bottom (DirectRelationReference | tuple[str, str] | None): Direct relation to a file holding the bottom projection of the cube map
        collection_360 (DirectRelationReference | tuple[str, str] | None): Direct relation to CogniteImage360Collection
        station_360 (DirectRelationReference | tuple[str, str] | None): Direct relation to CogniteGroup3D instance that groups different CogniteImage360 instances to the same station
        taken_at (datetime | None): The timestamp when the 6 photos were taken.
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
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
            translation_x=translation_x,
            translation_y=translation_y,
            translation_z=translation_z,
            euler_rotation_x=euler_rotation_x,
            euler_rotation_y=euler_rotation_y,
            euler_rotation_z=euler_rotation_z,
            scale_x=scale_x,
            scale_y=scale_y,
            scale_z=scale_z,
            existing_version=existing_version,
            type=type,
        )
        CogniteCubeMapApply.__init__(
            self,
            space,
            external_id,
            front=front,
            back=back,
            left=left,
            right=right,
            top=top,
            bottom=bottom,
            existing_version=existing_version,
            type=type,
        )
        self.collection_360 = DirectRelationReference.load(collection_360) if collection_360 else None
        self.station_360 = DirectRelationReference.load(station_360) if station_360 else None
        self.taken_at = taken_at


class CogniteImage360(CogniteImage360Properties, CogniteTransformation3DNode, CogniteCubeMap):
    """This represents the reading format of cognite image 360.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        translation_x (float | None): The displacement of the object along the X-axis in 3D space
        translation_y (float | None): The displacement of the object along the Y-axis in 3D space
        translation_z (float | None): The displacement of the object along the Z-axis in 3D space
        euler_rotation_x (float | None): The rotation of the object around the X-axis, measured in degrees
        euler_rotation_y (float | None): The rotation of the object around the Y-axis, measured in degrees
        euler_rotation_z (float | None): The rotation of the object around the Z-axis, measured in degrees
        scale_x (float | None): The scaling factor applied to the object along the X-axis
        scale_y (float | None): The scaling factor applied to the object along the Y-axis
        scale_z (float | None): The scaling factor applied to the object along the Z-axis
        front (DirectRelationReference | None): Direct relation to a file holding the front projection of the cube map
        back (DirectRelationReference | None): Direct relation to a file holding the back projection of the cube map
        left (DirectRelationReference | None): Direct relation to a file holding the left projection of the cube map
        right (DirectRelationReference | None): Direct relation to a file holding the right projection of the cube map
        top (DirectRelationReference | None): Direct relation to a file holding the top projection of the cube map
        bottom (DirectRelationReference | None): Direct relation to a file holding the bottom projection of the cube map
        collection_360 (DirectRelationReference | None): Direct relation to CogniteImage360Collection
        station_360 (DirectRelationReference | None): Direct relation to CogniteGroup3D instance that groups different CogniteImage360 instances to the same station
        taken_at (datetime | None): The timestamp when the 6 photos were taken.
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        CogniteTransformation3DNode.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            translation_x=translation_x,
            translation_y=translation_y,
            translation_z=translation_z,
            euler_rotation_x=euler_rotation_x,
            euler_rotation_y=euler_rotation_y,
            euler_rotation_z=euler_rotation_z,
            scale_x=scale_x,
            scale_y=scale_y,
            scale_z=scale_z,
            type=type,
            deleted_time=deleted_time,
        )
        CogniteCubeMap.__init__(
            self,
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            front=front,
            back=back,
            left=left,
            right=right,
            top=top,
            bottom=bottom,
            type=type,
            deleted_time=deleted_time,
        )
        self.collection_360 = DirectRelationReference.load(collection_360) if collection_360 else None
        self.station_360 = DirectRelationReference.load(station_360) if station_360 else None
        self.taken_at = taken_at

    def as_write(self) -> CogniteImage360Apply:
        return CogniteImage360Apply(
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


class CogniteCADModelProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteCADModel", "v1")


class CogniteCADModelApply(CogniteCADModelProperties, CogniteModel3DApply):
    """This represents the writing format of cognite cad model.

    It is used to when data is written to CDF.

    Navigational aid for traversing CogniteCADModel instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite cad model.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): CAD, PointCloud or CogniteImage360
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type_=type_,
            existing_version=existing_version,
            type=type,
        )


class CogniteCADModel(CogniteCADModelProperties, CogniteModel3D):
    """This represents the reading format of cognite cad model.

    It is used to when data is read from CDF.

    Navigational aid for traversing CogniteCADModel instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite cad model.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): CAD, PointCloud or CogniteImage360
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type_=type_,
            type=type,
            deleted_time=deleted_time,
        )

    def as_write(self) -> CogniteCADModelApply:
        return CogniteCADModelApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            type_=self.type_,
            existing_version=self.version,
            type=self.type,
        )


class CogniteImage360ModelProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360Model", "v1")


class CogniteImage360ModelApply(CogniteImage360ModelProperties, CogniteModel3DApply):
    """This represents the writing format of cognite image 360 model.

    It is used to when data is written to CDF.

    Navigational aid for traversing CogniteImage360Model instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360 model.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): CAD, PointCloud or CogniteImage360
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type_=type_,
            existing_version=existing_version,
            type=type,
        )


class CogniteImage360Model(CogniteImage360ModelProperties, CogniteModel3D):
    """This represents the reading format of cognite image 360 model.

    It is used to when data is read from CDF.

    Navigational aid for traversing CogniteImage360Model instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360 model.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): CAD, PointCloud or CogniteImage360
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type_=type_,
            type=type,
            deleted_time=deleted_time,
        )

    def as_write(self) -> CogniteImage360ModelApply:
        return CogniteImage360ModelApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            type_=self.type_,
            existing_version=self.version,
            type=self.type,
        )


class CognitePointCloudModelProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CognitePointCloudModel", "v1")


class CognitePointCloudModelApply(CognitePointCloudModelProperties, CogniteModel3DApply):
    """This represents the writing format of cognite point cloud model.

    It is used to when data is written to CDF.

    Navigational aid for traversing CognitePointCloudModel instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite point cloud model.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): CAD, PointCloud or CogniteImage360
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type_=type_,
            existing_version=existing_version,
            type=type,
        )


class CognitePointCloudModel(CognitePointCloudModelProperties, CogniteModel3D):
    """This represents the reading format of cognite point cloud model.

    It is used to when data is read from CDF.

    Navigational aid for traversing CognitePointCloudModel instances

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite point cloud model.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        type_ (Literal["CAD", "Image360", "PointCloud"] | None): CAD, PointCloud or CogniteImage360
        type (DirectRelationReference | None): Direct relation pointing to the type node.
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        type_: Literal["CAD", "Image360", "PointCloud"] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            type_=type_,
            type=type,
            deleted_time=deleted_time,
        )

    def as_write(self) -> CognitePointCloudModelApply:
        return CognitePointCloudModelApply(
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            aliases=self.aliases,
            type_=self.type_,
            existing_version=self.version,
            type=self.type,
        )


class CogniteDescribableEdgeApply(CogniteDescribableProperties, TypedEdgeApply):
    """This represents the writing format of cognite describable edge.

    It is used to when data is written to CDF.

    The describable core concept is used as a standard way of holding the bare minimum of information about the instance


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite describable edge.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        existing_version (int | None): Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        aliases: list[str] | None = None,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases


class CogniteDescribableEdge(CogniteDescribableProperties, TypedEdge):
    """This represents the reading format of cognite describable edge.

    It is used to when data is read from CDF.

    The describable core concept is used as a standard way of holding the bare minimum of information about the instance


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite describable edge.
        type (DirectRelationReference): The type of edge.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
            self,
            space,
            external_id,
            version,
            type,
            last_updated_time,
            created_time,
            start_node,
            end_node,
            deleted_time,
            None,
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


class CogniteSourceableEdgeApply(CogniteSourceableProperties, TypedEdgeApply):
    """This represents the writing format of cognite sourceable edge.

    It is used to when data is written to CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite sourceable edge.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        existing_version (int | None): Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
        source_id: str | None = None,
        source_context: str | None = None,
        source: DirectRelationReference | tuple[str, str] | None = None,
        source_created_time: datetime | None = None,
        source_updated_time: datetime | None = None,
        source_created_user: str | None = None,
        source_updated_user: str | None = None,
        existing_version: int | None = None,
    ) -> None:
        TypedEdgeApply.__init__(self, space, external_id, type, start_node, end_node, existing_version)
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else None
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user


class CogniteSourceableEdge(CogniteSourceableProperties, TypedEdge):
    """This represents the reading format of cognite sourceable edge.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite sourceable edge.
        type (DirectRelationReference): The type of edge.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
            self,
            space,
            external_id,
            version,
            type,
            last_updated_time,
            created_time,
            start_node,
            end_node,
            deleted_time,
            None,
        )
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else None
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


class CogniteTransformation3DEdgeApply(CogniteTransformation3DProperties, TypedEdgeApply):
    """This represents the writing format of cognite transformation 3 d edge.

    It is used to when data is written to CDF.

    The CogniteTransformation3D object defines a comprehensive 3D transformation, enabling precise adjustments to an object's position, orientation, and size in 3D space. It allows for the translation of objects along the three spatial axes, rotation around these axes using Euler angles, and scaling along each axis to modify the object's dimensions


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite transformation 3 d edge.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        translation_x (float | None): The displacement of the object along the X-axis in 3D space
        translation_y (float | None): The displacement of the object along the Y-axis in 3D space
        translation_z (float | None): The displacement of the object along the Z-axis in 3D space
        euler_rotation_x (float | None): The rotation of the object around the X-axis, measured in degrees
        euler_rotation_y (float | None): The rotation of the object around the Y-axis, measured in degrees
        euler_rotation_z (float | None): The rotation of the object around the Z-axis, measured in degrees
        scale_x (float | None): The scaling factor applied to the object along the X-axis
        scale_y (float | None): The scaling factor applied to the object along the Y-axis
        scale_z (float | None): The scaling factor applied to the object along the Z-axis
        existing_version (int | None): Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
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


class CogniteTransformation3DEdge(CogniteTransformation3DProperties, TypedEdge):
    """This represents the reading format of cognite transformation 3 d edge.

    It is used to when data is read from CDF.

    The CogniteTransformation3D object defines a comprehensive 3D transformation, enabling precise adjustments to an object's position, orientation, and size in 3D space. It allows for the translation of objects along the three spatial axes, rotation around these axes using Euler angles, and scaling along each axis to modify the object's dimensions


    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite transformation 3 d edge.
        type (DirectRelationReference): The type of edge.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        translation_x (float | None): The displacement of the object along the X-axis in 3D space
        translation_y (float | None): The displacement of the object along the Y-axis in 3D space
        translation_z (float | None): The displacement of the object along the Z-axis in 3D space
        euler_rotation_x (float | None): The rotation of the object around the X-axis, measured in degrees
        euler_rotation_y (float | None): The rotation of the object around the Y-axis, measured in degrees
        euler_rotation_z (float | None): The rotation of the object around the Z-axis, measured in degrees
        scale_x (float | None): The scaling factor applied to the object along the X-axis
        scale_y (float | None): The scaling factor applied to the object along the Y-axis
        scale_z (float | None): The scaling factor applied to the object along the Z-axis
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
            self,
            space,
            external_id,
            version,
            type,
            last_updated_time,
            created_time,
            start_node,
            end_node,
            deleted_time,
            None,
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


class CogniteAnnotationProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteAnnotation", "v1")


class CogniteAnnotationApply(CogniteAnnotationProperties, CogniteDescribableEdgeApply, CogniteSourceableEdgeApply):
    """This represents the writing format of cognite annotation.

    It is used to when data is written to CDF.

    Annotation represents contextualization results or links

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite annotation.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence (float | None): The confidence that the annotation is a good match
        status (Literal["Approved", "Rejected", "Suggested"] | None): The confidence that the annotation is a good match
        existing_version (int | None): Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
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
            self,
            space,
            external_id,
            type,
            start_node,
            end_node,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            existing_version=existing_version,
        )
        CogniteSourceableEdgeApply.__init__(
            self,
            space,
            external_id,
            type,
            start_node,
            end_node,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            existing_version=existing_version,
        )
        self.confidence = confidence
        self.status = status


class CogniteAnnotation(CogniteAnnotationProperties, CogniteDescribableEdge, CogniteSourceableEdge):
    """This represents the reading format of cognite annotation.

    It is used to when data is read from CDF.

    Annotation represents contextualization results or links

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite annotation.
        type (DirectRelationReference): The type of edge.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence (float | None): The confidence that the annotation is a good match
        status (Literal["Approved", "Rejected", "Suggested"] | None): The confidence that the annotation is a good match
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            deleted_time=deleted_time,
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
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            deleted_time=deleted_time,
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
    """This represents the writing format of cognite diagram annotation.

    It is used to when data is written to CDF.

    Annotation for diagrams

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite diagram annotation.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence (float | None): The confidence that the annotation is a good match
        status (Literal["Approved", "Rejected", "Suggested"] | None): The confidence that the annotation is a good match
        start_node_page_number (int | None): The number of the page on which this annotation is located in `startNode` File. The first page has number 1
        end_node_page_number (int | None): The number of the page on which this annotation is located in the endNode File if an endNode is present. The first page has number 1
        start_node_x_min (float | None): Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less than startNodeXMax
        start_node_x_max (float | None): Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more than startNodeXMin
        start_node_y_min (float | None): Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less than startNodeYMax
        start_node_y_max (float | None): Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more than startNodeYMin
        start_node_text (str | None): The text extracted from within the bounding box on the startNode
        end_node_x_min (float | None): Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less than endNodeXMax. Only applicable if an endNode is defined
        end_node_x_max (float | None): Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more than endNodeXMin. Only applicable if an endNode is defined
        end_node_y_min (float | None): Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less than endNodeYMax. Only applicable if an endNode is defined
        end_node_y_max (float | None): Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more than endNodeYMin. Only applicable if an endNode is defined
        end_node_text (str | None): The text extracted from within the bounding box on the endNode. Only applicable if an endNode is defined
        existing_version (int | None): Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
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
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            confidence=confidence,
            status=status,
            existing_version=existing_version,
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
    """This represents the reading format of cognite diagram annotation.

    It is used to when data is read from CDF.

    Annotation for diagrams

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite diagram annotation.
        type (DirectRelationReference): The type of edge.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence (float | None): The confidence that the annotation is a good match
        status (Literal["Approved", "Rejected", "Suggested"] | None): The confidence that the annotation is a good match
        start_node_page_number (int | None): The number of the page on which this annotation is located in `startNode` File. The first page has number 1
        end_node_page_number (int | None): The number of the page on which this annotation is located in the endNode File if an endNode is present. The first page has number 1
        start_node_x_min (float | None): Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less than startNodeXMax
        start_node_x_max (float | None): Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more than startNodeXMin
        start_node_y_min (float | None): Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less than startNodeYMax
        start_node_y_max (float | None): Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more than startNodeYMin
        start_node_text (str | None): The text extracted from within the bounding box on the startNode
        end_node_x_min (float | None): Value between [0,1]. Minimum abscissa of the bounding box (left edge). Must be strictly less than endNodeXMax. Only applicable if an endNode is defined
        end_node_x_max (float | None): Value between [0,1]. Maximum abscissa of the bounding box (right edge). Must be strictly more than endNodeXMin. Only applicable if an endNode is defined
        end_node_y_min (float | None): Value between [0,1]. Minimum ordinate of the bounding box (bottom edge). Must be strictly less than endNodeYMax. Only applicable if an endNode is defined
        end_node_y_max (float | None): Value between [0,1]. Maximum ordinate of the bounding box (top edge). Must be strictly more than endNodeYMin. Only applicable if an endNode is defined
        end_node_text (str | None): The text extracted from within the bounding box on the endNode. Only applicable if an endNode is defined
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            type,
            start_node,
            end_node,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            confidence=confidence,
            status=status,
            deleted_time=deleted_time,
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


class CogniteImage360AnnotationProperties:
    format_version = PropertyOptions("formatVersion")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_cdm_experimental", "CogniteImage360Annotation", "v1")


class CogniteImage360AnnotationApply(CogniteImage360AnnotationProperties, CogniteAnnotationApply):
    """This represents the writing format of cognite image 360 annotation.

    It is used to when data is written to CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360 annotation.
        type (DirectRelationReference | tuple[str, str]): The type of edge.
        start_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference | tuple[str, str]): Reference to the direct relation. The reference consists of a space and an external-id.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | tuple[str, str] | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence (float | None): The confidence that the annotation is a good match
        status (Literal["Approved", "Rejected", "Suggested"] | None): The confidence that the annotation is a good match
        polygon (list[float] | None): List of floats representing the polygon. Format depends on formatVersion
        format_version (str | None): Specifies the storage representation for the polygon property
        existing_version (int | None): Fail the ingestion request if the edge's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or edge). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        type: DirectRelationReference | tuple[str, str],
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        *,
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
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            confidence=confidence,
            status=status,
            existing_version=existing_version,
        )
        self.polygon = polygon
        self.format_version = format_version


class CogniteImage360Annotation(CogniteImage360AnnotationProperties, CogniteAnnotation):
    """This represents the reading format of cognite image 360 annotation.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the cognite image 360 annotation.
        type (DirectRelationReference): The type of edge.
        start_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        end_node (DirectRelationReference): Reference to the direct relation. The reference consists of a space and an external-id.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        name (str | None): Name of the instance
        description (str | None): Description of the instance
        tags (list[str] | None): Text based labels for generic use, limited to 1000
        aliases (list[str] | None): Alternative names for the node
        source_id (str | None): Identifier from the source system
        source_context (str | None): Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source (DirectRelationReference | None): Direct relation to a source system
        source_created_time (datetime | None): When the instance was created in source system (if available)
        source_updated_time (datetime | None): When the instance was last updated in the source system (if available)
        source_created_user (str | None): User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_updated_user (str | None): User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        confidence (float | None): The confidence that the annotation is a good match
        status (Literal["Approved", "Rejected", "Suggested"] | None): The confidence that the annotation is a good match
        polygon (list[float] | None): List of floats representing the polygon. Format depends on formatVersion
        format_version (str | None): Specifies the storage representation for the polygon property
        deleted_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances are filtered out of query results, but present in sync results
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
        super().__init__(
            space,
            external_id,
            type,
            start_node,
            end_node,
            version,
            last_updated_time,
            created_time,
            name=name,
            description=description,
            tags=tags,
            aliases=aliases,
            source_id=source_id,
            source_context=source_context,
            source=source,
            source_created_time=source_created_time,
            source_updated_time=source_updated_time,
            source_created_user=source_created_user,
            source_updated_user=source_updated_user,
            confidence=confidence,
            status=status,
            deleted_time=deleted_time,
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
