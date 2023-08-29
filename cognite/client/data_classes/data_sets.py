from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    IdTransformerMixin,
    PropertySpec,
)
from cognite.client.data_classes.shared import TimestampRange

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class DataSet(CogniteResource):
    """No description.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the data set.
        description (str | None): The description of the data set.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        write_protected (bool | None): To write data to a write-protected data set, you need to be a member of a group that has the "datasets:owner" action for the data set.  To learn more about write-protected data sets, follow this [guide](/cdf/data_governance/concepts/datasets/#write-protection)
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
        self.external_id = external_id
        self.name = name
        self.description = description
        self.metadata = metadata
        self.write_protected = write_protected
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)


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
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name"),
            PropertySpec("description"),
            PropertySpec("metadata", is_container=True),
            PropertySpec("write_protected", is_nullable=False),
        ]


class DataSetAggregate(dict):
    """Aggregation group of data sets

    Args:
        count (int | None): Size of the aggregation group
        **kwargs (Any): No description.
    """

    def __init__(self, count: int | None = None, **kwargs: Any) -> None:
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class DataSetList(CogniteResourceList[DataSet], IdTransformerMixin):
    _RESOURCE = DataSet
