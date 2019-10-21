from typing import *

from cognite.client.data_classes._base import *


# GenClass: EpochTimestampRange
class EpochTimestampRange(dict):
    """Range between two timestamps.

    Args:
        max (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, max: int = None, min: int = None, cognite_client=None):
        self.max = max
        self.min = min
        self._cognite_client = cognite_client

    # GenStop
