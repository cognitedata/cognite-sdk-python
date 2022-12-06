from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AggregateDatapoint(_message.Message):
    __slots__ = ["average", "continuousVariance", "count", "discreteVariance", "interpolation", "max", "min", "stepInterpolation", "sum", "timestamp", "totalVariation"]
    AVERAGE_FIELD_NUMBER: _ClassVar[int]
    CONTINUOUSVARIANCE_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    DISCRETEVARIANCE_FIELD_NUMBER: _ClassVar[int]
    INTERPOLATION_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    MIN_FIELD_NUMBER: _ClassVar[int]
    STEPINTERPOLATION_FIELD_NUMBER: _ClassVar[int]
    SUM_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TOTALVARIATION_FIELD_NUMBER: _ClassVar[int]
    average: float
    continuousVariance: float
    count: float
    discreteVariance: float
    interpolation: float
    max: float
    min: float
    stepInterpolation: float
    sum: float
    timestamp: int
    totalVariation: float
    def __init__(self, timestamp: _Optional[int] = ..., average: _Optional[float] = ..., max: _Optional[float] = ..., min: _Optional[float] = ..., count: _Optional[float] = ..., sum: _Optional[float] = ..., interpolation: _Optional[float] = ..., stepInterpolation: _Optional[float] = ..., continuousVariance: _Optional[float] = ..., discreteVariance: _Optional[float] = ..., totalVariation: _Optional[float] = ...) -> None: ...

class AggregateDatapoints(_message.Message):
    __slots__ = ["datapoints"]
    DATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    datapoints: _containers.RepeatedCompositeFieldContainer[AggregateDatapoint]
    def __init__(self, datapoints: _Optional[_Iterable[_Union[AggregateDatapoint, _Mapping]]] = ...) -> None: ...

class NumericDatapoint(_message.Message):
    __slots__ = ["timestamp", "value"]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    value: float
    def __init__(self, timestamp: _Optional[int] = ..., value: _Optional[float] = ...) -> None: ...

class NumericDatapoints(_message.Message):
    __slots__ = ["datapoints"]
    DATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    datapoints: _containers.RepeatedCompositeFieldContainer[NumericDatapoint]
    def __init__(self, datapoints: _Optional[_Iterable[_Union[NumericDatapoint, _Mapping]]] = ...) -> None: ...

class StringDatapoint(_message.Message):
    __slots__ = ["timestamp", "value"]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    value: str
    def __init__(self, timestamp: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class StringDatapoints(_message.Message):
    __slots__ = ["datapoints"]
    DATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    datapoints: _containers.RepeatedCompositeFieldContainer[StringDatapoint]
    def __init__(self, datapoints: _Optional[_Iterable[_Union[StringDatapoint, _Mapping]]] = ...) -> None: ...
