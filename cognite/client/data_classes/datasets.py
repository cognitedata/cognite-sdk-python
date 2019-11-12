from typing import *

from cognite.client.data_classes._base import *
from cognite.client.data_classes.shared import TimestampRange


# GenClass: DataSet
class Dataset(CogniteResource):
    """No description.

    Args:
        external_id (str): External Id provided by client. Should be unique within the project.
        name (str): Name of dataset
        description (str): Description of dataset
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value
        id (int): Javascript friendly internal ID given to the object.
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
        id: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.description = description
        self.metadata = metadata
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop


# GenClass: DataSetFilter
class DatasetFilter(CogniteFilter):
    """Filter on datasets with exact match

    Args:
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        metadata: Dict[str, str] = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        cognite_client=None,
    ):
        self.metadata = metadata
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(DatasetFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance

    # GenStop


# GenUpdateClass: DataSetUpdate
class DatasetUpdate(CogniteUpdate):
    """Update applied to singe DataSet

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    @property
    def external_id(self):
        return _PrimitiveDatasetUpdate(self, "externalId")

    @property
    def name(self):
        return _PrimitiveDatasetUpdate(self, "name")

    @property
    def description(self):
        return _PrimitiveDatasetUpdate(self, "description")

    @property
    def metadata(self):
        return _ObjectDatasetUpdate(self, "metadata")


class _PrimitiveDatasetUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> DatasetUpdate:
        return self._set(value)


class _ObjectDatasetUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> DatasetUpdate:
        return self._set(value)

    def add(self, value: Dict) -> DatasetUpdate:
        return self._add(value)

    def remove(self, value: List) -> DatasetUpdate:
        return self._remove(value)


class _ListDatasetUpdate(CogniteListUpdate):
    def set(self, value: List) -> DatasetUpdate:
        return self._set(value)

    def add(self, value: List) -> DatasetUpdate:
        return self._add(value)

    def remove(self, value: List) -> DatasetUpdate:
        return self._remove(value)

    # GenStop


class DatasetList(CogniteResourceList):
    _RESOURCE = Dataset
    _UPDATE = DatasetUpdate
