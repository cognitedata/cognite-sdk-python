from typing import *

from cognite.client.data_classes._base import CognitePropertyClassUtil


# GenPropertyClass: EpochTimestampRange
class EpochTimestampRange(dict):
    """Range between two timestamps.

    Args:
        max (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(self, max: int = None, min: int = None):
        self.max = max
        self.min = min

    max = CognitePropertyClassUtil.declare_property("max")
    min = CognitePropertyClassUtil.declare_property("min")

    # GenStop
