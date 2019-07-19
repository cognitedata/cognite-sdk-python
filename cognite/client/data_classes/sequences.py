from typing import *
from typing import List

from cognite.client.data_classes._base import *


# GenClass: GetSequenceDTO
class Sequence(CogniteResource):
    """Information about the sequence stored in the database

    Args:
        id (int): Unique cognite-provided identifier for the sequence
        name (str): Name of the sequence
        description (str): Description of the sequence
        asset_id (int): Optional asset this sequence is associated with
        external_id (str): Projectwide unique identifier for the sequence
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        columns (List[Dict[str, Any]]): List of column definitions
        created_time (int): Time when this asset was created in CDP in milliseconds since Jan 1, 1970.
        last_updated_time (int): The last time this asset was updated in CDP, in milliseconds since Jan 1, 1970.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        name: str = None,
        description: str = None,
        asset_id: int = None,
        external_id: str = None,
        metadata: Dict[str, Any] = None,
        columns: List[Dict[str, Any]] = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.asset_id = asset_id
        self.external_id = external_id
        self.metadata = metadata
        self.columns = columns
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop


# GenClass: SequenceFilter
class SequenceFilter(CogniteFilter):
    """No description.

    Args:
        name (str): Filter out sequences that do not have this *exact* name.
        external_id_prefix (str): Filter out sequences that do not have this string as the start of the externalId
        metadata (Dict[str, Any]): Filter out sequences that do not match these metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
        asset_ids (List[int]): Filter out sequences that are not linked to any of these assets.
        created_time (Dict[str, Any]): Filter out sequences with createdTime outside this range.
        last_updated_time (Dict[str, Any]): Filter out sequences with lastUpdatedTime outside this range.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        external_id_prefix: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        cognite_client=None,
    ):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    # GenStop


# GenUpdateClass: SequencesUpdate
class SequenceUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    @property
    def name(self):
        return _PrimitiveSequenceUpdate(self, "name")

    @property
    def description(self):
        return _PrimitiveSequenceUpdate(self, "description")

    @property
    def asset_id(self):
        return _PrimitiveSequenceUpdate(self, "assetId")

    @property
    def external_id(self):
        return _PrimitiveSequenceUpdate(self, "externalId")

    @property
    def metadata(self):
        return _ObjectSequenceUpdate(self, "metadata")


class _PrimitiveSequenceUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> SequenceUpdate:
        return self._set(value)


class _ObjectSequenceUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> SequenceUpdate:
        return self._set(value)

    def add(self, value: Dict) -> SequenceUpdate:
        return self._add(value)

    def remove(self, value: List) -> SequenceUpdate:
        return self._remove(value)


class _ListSequenceUpdate(CogniteListUpdate):
    def set(self, value: List) -> SequenceUpdate:
        return self._set(value)

    def add(self, value: List) -> SequenceUpdate:
        return self._add(value)

    def remove(self, value: List) -> SequenceUpdate:
        return self._remove(value)

    # GenStop


class SequenceList(CogniteResourceList):
    _RESOURCE = Sequence
    _UPDATE = SequenceUpdate
