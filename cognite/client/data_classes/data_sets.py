from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.shared import TimestampRange

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class DataSetCore(WriteableCogniteResource["DataSetWrite"]):
    """Data sets let you document and track data lineage, ensure data integrity, and allow 3rd parties to write their insights securely back to a Cognite Data Fusion (CDF) project.
    This is the read version of the DataSet, which is used when retrieving from CDF.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the data set.
        description (str | None): The description of the data set.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        write_protected (bool | None): To write data to a write-protected data set, you need to be a member of a group that has the "datasets:owner" action for the data set. To learn more about write-protected data sets, follow this [guide](/cdf/data_governance/concepts/datasets/#write-protection)
    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        write_protected: bool | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.description = description
        self.metadata = metadata
        self.write_protected = write_protected


class DataSet(DataSetCore):
    """Data sets let you document and track data lineage, ensure data integrity, and allow 3rd parties to write their insights securely back to a Cognite Data Fusion (CDF) project.
    This is the read version of the DataSet, which is used when retrieving from CDF.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the data set.
        description (str | None): The description of the data set.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        write_protected (bool | None): To write data to a write-protected data set, you need to be a member of a group that has the "datasets:owner" action for the data set. To learn more about write-protected data sets, follow this [guide](/cdf/data_governance/concepts/datasets/#write-protection)
        id (int | None): A server-generated ID for the object.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        write_protected: bool | None = None,
        id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            description=description,
            metadata=metadata,
            write_protected=write_protected,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> DataSetWrite:
        return DataSetWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            metadata=self.metadata,
            write_protected=self.write_protected,
        )


class DataSetWrite(DataSetCore):
    """Data sets let you document and track data lineage, ensure data integrity, and allow 3rd parties to write their insights securely back to a Cognite Data Fusion (CDF) project.
    This is the read version of the DataSet, which is used when retrieving from CDF.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the data set.
        description (str | None): The description of the data set.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        write_protected (bool | None): To write data to a write-protected data set, you need to be a member of a group that has the "datasets:owner" action for the data set. To learn more about write-protected data sets, follow this [guide](/cdf/data_governance/concepts/datasets/#write-protection)
    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        write_protected: bool | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            description=description,
            metadata=metadata,
            write_protected=write_protected,
        )

    def as_write(self) -> DataSetWrite:
        """Returns this DataSetWrite instance."""
        return self


class DataSetFilter(CogniteFilter):
    """Filter on data sets with strict matching.

    Args:
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
        write_protected (bool | None): No description.
    """

    def __init__(
        self,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
    ) -> None:
        self.metadata = metadata
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix
        self.write_protected = write_protected

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)
        if self.created_time and isinstance(self.created_time, TimestampRange):
            dumped["createdTime"] = self.created_time.dump(camel_case=camel_case)
        if self.last_updated_time and isinstance(self.last_updated_time, TimestampRange):
            dumped["lastUpdatedTime"] = self.last_updated_time.dump(camel_case=camel_case)
        return dumped


class DataSetUpdate(CogniteUpdate):
    """Update applied to single data set

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveDataSetUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> DataSetUpdate:
            return self._set(value)

    class _ObjectDataSetUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> DataSetUpdate:
            return self._set(value)

        def add(self, value: dict) -> DataSetUpdate:
            return self._add(value)

        def remove(self, value: list) -> DataSetUpdate:
            return self._remove(value)

    class _ListDataSetUpdate(CogniteListUpdate):
        def set(self, value: list) -> DataSetUpdate:
            return self._set(value)

        def add(self, value: list) -> DataSetUpdate:
            return self._add(value)

        def remove(self, value: list) -> DataSetUpdate:
            return self._remove(value)

    class _LabelDataSetUpdate(CogniteLabelUpdate):
        def add(self, value: list) -> DataSetUpdate:
            return self._add(value)

        def remove(self, value: list) -> DataSetUpdate:
            return self._remove(value)

    @property
    def external_id(self) -> _PrimitiveDataSetUpdate:
        return DataSetUpdate._PrimitiveDataSetUpdate(self, "externalId")

    @property
    def name(self) -> _PrimitiveDataSetUpdate:
        return DataSetUpdate._PrimitiveDataSetUpdate(self, "name")

    @property
    def description(self) -> _PrimitiveDataSetUpdate:
        return DataSetUpdate._PrimitiveDataSetUpdate(self, "description")

    @property
    def metadata(self) -> _ObjectDataSetUpdate:
        return DataSetUpdate._ObjectDataSetUpdate(self, "metadata")

    @property
    def write_protected(self) -> _PrimitiveDataSetUpdate:
        return DataSetUpdate._PrimitiveDataSetUpdate(self, "writeProtected")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name"),
            PropertySpec("description"),
            PropertySpec("metadata", is_object=True),
            PropertySpec("write_protected", is_nullable=False),
        ]


class DataSetWriteList(CogniteResourceList[DataSetWrite], ExternalIDTransformerMixin):
    _RESOURCE = DataSetWrite


class DataSetList(WriteableCogniteResourceList[DataSetWrite, DataSet], IdTransformerMixin):
    _RESOURCE = DataSet

    def as_write(self) -> DataSetWriteList:
        return DataSetWriteList([ds.as_write() for ds in self.data], cognite_client=self._get_cognite_client())
