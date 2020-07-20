from typing import *

from cognite.client.data_classes._base import *
from cognite.client.data_classes.shared import TimestampRange


class DataSet(CogniteResource):
    """No description.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the data set.
        description (str): The description of the data set.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        write_protected (bool): To write data to a write-protected data set, you need to be a member of a group that has the "datasets:owner" action for the data set.  To learn more about write-protected data sets, follow this [guide](/cdf/data_governance/concepts/datasets/#write-protection)
        id (int): A server-generated ID for the object.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        description: str = None,
        metadata: Dict[str, str] = None,
        write_protected: bool = None,
        id: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.description = description
        self.metadata = metadata
        self.write_protected = write_protected
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client


class DataSetFilter(CogniteFilter):
    """Filter on data sets with strict matching.

    Args:
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        write_protected (bool): No description.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        metadata: Dict[str, str] = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        write_protected: bool = None,
        cognite_client=None,
    ):
        self.metadata = metadata
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix
        self.write_protected = write_protected
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(DataSetFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance


class DataSetUpdate(CogniteUpdate):
    """Update applied to single data set

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveDataSetUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "DataSetUpdate":
            return self._set(value)

    class _ObjectDataSetUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> "DataSetUpdate":
            return self._set(value)

        def add(self, value: Dict) -> "DataSetUpdate":
            return self._add(value)

        def remove(self, value: List) -> "DataSetUpdate":
            return self._remove(value)

    class _ListDataSetUpdate(CogniteListUpdate):
        def set(self, value: List) -> "DataSetUpdate":
            return self._set(value)

        def add(self, value: List) -> "DataSetUpdate":
            return self._add(value)

        def remove(self, value: List) -> "DataSetUpdate":
            return self._remove(value)

    class _LabelDataSetUpdate(CogniteLabelUpdate):
        def add(self, value: List) -> "DataSetUpdate":
            return self._add(value)

        def remove(self, value: List) -> "DataSetUpdate":
            return self._remove(value)

    @property
    def external_id(self):
        return DataSetUpdate._PrimitiveDataSetUpdate(self, "externalId")

    @property
    def name(self):
        return DataSetUpdate._PrimitiveDataSetUpdate(self, "name")

    @property
    def description(self):
        return DataSetUpdate._PrimitiveDataSetUpdate(self, "description")

    @property
    def metadata(self):
        return DataSetUpdate._ObjectDataSetUpdate(self, "metadata")

    @property
    def write_protected(self):
        return DataSetUpdate._PrimitiveDataSetUpdate(self, "writeProtected")


class DataSetAggregate(dict):
    """Aggregation group of data sets

    Args:
        count (int): Size of the aggregation group
    """

    def __init__(self, count: int = None, **kwargs):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class DataSetList(CogniteResourceList):
    _RESOURCE = DataSet
    _UPDATE = DataSetUpdate
