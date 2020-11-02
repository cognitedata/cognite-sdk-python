from typing import *

from cognite.client.data_classes._base import *


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


class AggregateResult(dict):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
    """

    def __init__(self, count: int = None, **kwargs):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class AggregateUniqueValuesResult(AggregateResult):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
        value (Union(int, str)): A unique value from the requested field
    """

    def __init__(self, count: int = None, value: Union[int, str] = None, **kwargs):
        super().__init__(count, **kwargs)
        self.value = value

    value = CognitePropertyClassUtil.declare_property("value")
