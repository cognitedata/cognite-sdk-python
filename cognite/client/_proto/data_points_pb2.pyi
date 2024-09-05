from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Status(_message.Message):
    __slots__ = ("code", "symbol")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    code: int
    symbol: str
    def __init__(self, code: _Optional[int] = ..., symbol: _Optional[str] = ...) -> None: ...

class NumericDatapoint(_message.Message):
    __slots__ = ("timestamp", "value", "status", "nullValue")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NULLVALUE_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    value: float
    status: Status
    nullValue: bool
    def __init__(self, timestamp: _Optional[int] = ..., value: _Optional[float] = ..., status: _Optional[_Union[Status, _Mapping]] = ..., nullValue: bool = ...) -> None: ...

class NumericDatapoints(_message.Message):
    __slots__ = ("datapoints",)
    DATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    datapoints: _containers.RepeatedCompositeFieldContainer[NumericDatapoint]
    def __init__(self, datapoints: _Optional[_Iterable[_Union[NumericDatapoint, _Mapping]]] = ...) -> None: ...

class StringDatapoint(_message.Message):
    __slots__ = ("timestamp", "value", "status", "nullValue")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NULLVALUE_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    value: str
    status: Status
    nullValue: bool
    def __init__(self, timestamp: _Optional[int] = ..., value: _Optional[str] = ..., status: _Optional[_Union[Status, _Mapping]] = ..., nullValue: bool = ...) -> None: ...

class StringDatapoints(_message.Message):
    __slots__ = ("datapoints",)
    DATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    datapoints: _containers.RepeatedCompositeFieldContainer[StringDatapoint]
    def __init__(self, datapoints: _Optional[_Iterable[_Union[StringDatapoint, _Mapping]]] = ...) -> None: ...

class AggregateDatapoint(_message.Message):
    __slots__ = ("timestamp", "average", "max", "min", "count", "sum", "interpolation", "stepInterpolation", "continuousVariance", "discreteVariance", "totalVariation", "countGood", "countUncertain", "countBad", "durationGood", "durationUncertain", "durationBad")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    AVERAGE_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    MIN_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    SUM_FIELD_NUMBER: _ClassVar[int]
    INTERPOLATION_FIELD_NUMBER: _ClassVar[int]
    STEPINTERPOLATION_FIELD_NUMBER: _ClassVar[int]
    CONTINUOUSVARIANCE_FIELD_NUMBER: _ClassVar[int]
    DISCRETEVARIANCE_FIELD_NUMBER: _ClassVar[int]
    TOTALVARIATION_FIELD_NUMBER: _ClassVar[int]
    COUNTGOOD_FIELD_NUMBER: _ClassVar[int]
    COUNTUNCERTAIN_FIELD_NUMBER: _ClassVar[int]
    COUNTBAD_FIELD_NUMBER: _ClassVar[int]
    DURATIONGOOD_FIELD_NUMBER: _ClassVar[int]
    DURATIONUNCERTAIN_FIELD_NUMBER: _ClassVar[int]
    DURATIONBAD_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    average: float
    max: float
    min: float
    count: float
    sum: float
    interpolation: float
    stepInterpolation: float
    continuousVariance: float
    discreteVariance: float
    totalVariation: float
    countGood: float
    countUncertain: float
    countBad: float
    durationGood: float
    durationUncertain: float
    durationBad: float
    def __init__(self, timestamp: _Optional[int] = ..., average: _Optional[float] = ..., max: _Optional[float] = ..., min: _Optional[float] = ..., count: _Optional[float] = ..., sum: _Optional[float] = ..., interpolation: _Optional[float] = ..., stepInterpolation: _Optional[float] = ..., continuousVariance: _Optional[float] = ..., discreteVariance: _Optional[float] = ..., totalVariation: _Optional[float] = ..., countGood: _Optional[float] = ..., countUncertain: _Optional[float] = ..., countBad: _Optional[float] = ..., durationGood: _Optional[float] = ..., durationUncertain: _Optional[float] = ..., durationBad: _Optional[float] = ...) -> None: ...

class AggregateDatapoints(_message.Message):
    __slots__ = ("datapoints",)
    DATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    datapoints: _containers.RepeatedCompositeFieldContainer[AggregateDatapoint]
    def __init__(self, datapoints: _Optional[_Iterable[_Union[AggregateDatapoint, _Mapping]]] = ...) -> None: ...

class InstanceId(_message.Message):
    __slots__ = ("space", "externalId")
    SPACE_FIELD_NUMBER: _ClassVar[int]
    EXTERNALID_FIELD_NUMBER: _ClassVar[int]
    space: str
    externalId: str
    def __init__(self, space: _Optional[str] = ..., externalId: _Optional[str] = ...) -> None: ...
