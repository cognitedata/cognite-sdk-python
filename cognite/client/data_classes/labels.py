from typing import *

from typing import Any, Dict, List

from cognite.client.data_classes._base import *
from cognite.client.data_classes.shared import TimestampRange


# GenClass: LabelDefinitionSpec, LabelDefinition
class Label(CogniteResource):
    """No description.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): Name of the label.
        description (str): Description of the label.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        description: str = None,
        created_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.description = description
        self.created_time = created_time
        self._cognite_client = cognite_client

    # GenStop


# GenClass: LabelDefinitionFilter.filter
class LabelFilter(CogniteFilter):
    """Filter on labels with strict matching.

    Args:
        name (str): Returns the label definitions matching that name.
        external_id_prefix (str): filter external ids starting with the prefix specified
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str = None, external_id_prefix: str = None, cognite_client=None):
        self.name = name
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cognite_client

    # GenStop


class LabelList(CogniteResourceList):
    _RESOURCE = Label
    _UPDATE = None
    _ASSERT_CLASSES = False  # because no Update
