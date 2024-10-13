from __future__ import annotations

from datetime import datetime
from typing import Literal

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    PropertyOptions,
    TypedNode,
    TypedNodeApply,
)


class _CogniteExtractorDataProperties:
    extracted_data = PropertyOptions("extractedData")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_extraction_extensions", "CogniteExtractorData", "v1")


class CogniteExtractorDataApply(_CogniteExtractorDataProperties, TypedNodeApply):
    """This represents the writing format of Cognite extractor datum.

    It is used to when data is written to CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the Cognite extractor datum.
        extracted_data (dict | None): Unstructured information extracted from source system
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        extracted_data: dict | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.extracted_data = extracted_data


class CogniteExtractorData(_CogniteExtractorDataProperties, TypedNode):
    """This represents the reading format of Cognite extractor datum.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the Cognite extractor datum.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        extracted_data (dict | None): Unstructured information extracted from source system
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
        extracted_data: dict | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.extracted_data = extracted_data

    def as_write(self) -> CogniteExtractorDataApply:
        return CogniteExtractorDataApply(
            self.space,
            self.external_id,
            extracted_data=self.extracted_data,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteExtractorFileProperties:
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
    mime_type = PropertyOptions("mimeType")
    is_uploaded = PropertyOptions("isUploaded")
    uploaded_time = PropertyOptions("uploadedTime")
    extracted_data = PropertyOptions("extractedData")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_extraction_extensions", "CogniteExtractorFile", "v1")


class CogniteExtractorFileApply(_CogniteExtractorFileProperties, TypedNodeApply):
    """This represents the writing format of Cognite extractor file.

    It is used to when data is written to CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the Cognite extractor file.
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
        extracted_data (dict | None): Unstructured information extracted from source system
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
        extracted_data: dict | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.tags = tags
        self.aliases = aliases
        self.source_id = source_id
        self.source_context = source_context
        self.source = DirectRelationReference.load(source) if source else None
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.mime_type = mime_type
        self.directory = directory
        self.is_uploaded = is_uploaded
        self.uploaded_time = uploaded_time
        self.category = DirectRelationReference.load(category) if category else None
        self.extracted_data = extracted_data


class CogniteExtractorFile(_CogniteExtractorFileProperties, TypedNode):
    """This represents the reading format of Cognite extractor file.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the Cognite extractor file.
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
        extracted_data (dict | None): Unstructured information extracted from source system
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
        extracted_data: dict | None = None,
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
        self.source = DirectRelationReference.load(source) if source else None
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.mime_type = mime_type
        self.directory = directory
        self.is_uploaded = is_uploaded
        self.uploaded_time = uploaded_time
        self.category = DirectRelationReference.load(category) if category else None
        self.extracted_data = extracted_data

    def as_write(self) -> CogniteExtractorFileApply:
        return CogniteExtractorFileApply(
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
            extracted_data=self.extracted_data,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteExtractorTimeSeriesProperties:
    is_step = PropertyOptions("isStep")
    time_series_type = PropertyOptions("type")
    source_id = PropertyOptions("sourceId")
    source_context = PropertyOptions("sourceContext")
    source_created_time = PropertyOptions("sourceCreatedTime")
    source_updated_time = PropertyOptions("sourceUpdatedTime")
    source_created_user = PropertyOptions("sourceCreatedUser")
    source_updated_user = PropertyOptions("sourceUpdatedUser")
    source_unit = PropertyOptions("sourceUnit")
    extracted_data = PropertyOptions("extractedData")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_extraction_extensions", "CogniteExtractorTimeSeries", "v1")


class CogniteExtractorTimeSeriesApply(_CogniteExtractorTimeSeriesProperties, TypedNodeApply):
    """This represents the writing format of Cognite extractor time series.

    It is used to when data is written to CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the Cognite extractor time series.
        is_step (bool): Defines whether the time series is a step series or not.
        time_series_type (Literal['numeric', 'string']): Defines data type of the data points.
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
        source_unit (str | None): Unit as specified in the source system
        unit (DirectRelationReference | tuple[str, str] | None): direct relation to the unit of the time series
        assets (list[DirectRelationReference | tuple[str, str]] | None): The asset field.
        equipment (list[DirectRelationReference | tuple[str, str]] | None): The equipment field.
        extracted_data (dict | None): Unstructured information extracted from source system
        existing_version (int | None): Fail the ingestion request if the node's version is greater than or equal to this value. If no existingVersion is specified, the ingestion will always overwrite any existing data for the node (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
        type (DirectRelationReference | tuple[str, str] | None): Direct relation pointing to the type node.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        *,
        is_step: bool,
        time_series_type: Literal["numeric", "string"],
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
        source_unit: str | None = None,
        unit: DirectRelationReference | tuple[str, str] | None = None,
        assets: list[DirectRelationReference | tuple[str, str]] | None = None,
        equipment: list[DirectRelationReference | tuple[str, str]] | None = None,
        extracted_data: dict | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
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
        self.source = DirectRelationReference.load(source) if source else None
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.source_unit = source_unit
        self.unit = DirectRelationReference.load(unit) if unit else None
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.equipment = [DirectRelationReference.load(equipment) for equipment in equipment] if equipment else None
        self.extracted_data = extracted_data


class CogniteExtractorTimeSeries(_CogniteExtractorTimeSeriesProperties, TypedNode):
    """This represents the reading format of Cognite extractor time series.

    It is used to when data is read from CDF.

    Args:
        space (str): The space where the node is located.
        external_id (str): The external id of the Cognite extractor time series.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        is_step (bool): Defines whether the time series is a step series or not.
        time_series_type (Literal['numeric', 'string']): Defines data type of the data points.
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
        source_unit (str | None): Unit as specified in the source system
        unit (DirectRelationReference | None): direct relation to the unit of the time series
        assets (list[DirectRelationReference] | None): The asset field.
        equipment (list[DirectRelationReference] | None): The equipment field.
        extracted_data (dict | None): Unstructured information extracted from source system
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
        extracted_data: dict | None = None,
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
        self.source = DirectRelationReference.load(source) if source else None
        self.source_created_time = source_created_time
        self.source_updated_time = source_updated_time
        self.source_created_user = source_created_user
        self.source_updated_user = source_updated_user
        self.source_unit = source_unit
        self.unit = DirectRelationReference.load(unit) if unit else None
        self.assets = [DirectRelationReference.load(asset) for asset in assets] if assets else None
        self.equipment = [DirectRelationReference.load(equipment) for equipment in equipment] if equipment else None
        self.extracted_data = extracted_data

    def as_write(self) -> CogniteExtractorTimeSeriesApply:
        return CogniteExtractorTimeSeriesApply(
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
            extracted_data=self.extracted_data,
            existing_version=self.version,
            type=self.type,
        )
