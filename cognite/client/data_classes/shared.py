from typing import *

from cognite.client.data_classes._base import CognitePropertyClassUtil


# GenPropertyClass: EpochTimestampRange
class TimestampRange(dict):
    """Range between two timestamps.

    Args:
        max (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(self, max: int = None, min: int = None, **kwargs):
        self.max = max
        self.min = min
        self.update(kwargs)

    max = CognitePropertyClassUtil.declare_property("max")
    min = CognitePropertyClassUtil.declare_property("min")

    # GenStop


# GenPropertyClass: DataSetIdIsNull
class DataSetIdIsNull(dict):
    """No description.

    Args:
        is_null (bool): Set to true if you want to search for data with dataSetId field value not set, false to search for cases where some value is present.
    """

    def __init__(self, is_null: bool = None, **kwargs):
        self.is_null = is_null
        self.update(kwargs)

    is_null = CognitePropertyClassUtil.declare_property("isNull")

    # GenStop
