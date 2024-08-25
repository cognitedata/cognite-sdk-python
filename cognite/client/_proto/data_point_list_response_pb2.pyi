import cognite.client._proto.data_points_pb2 as _data_points_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataPointListItem(_message.Message):
    __slots__ = ("id", "externalId", "instanceId", "isString", "isStep", "unit", "nextCursor", "unitExternalId", "numericDatapoints", "stringDatapoints", "aggregateDatapoints")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNALID_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    ISSTRING_FIELD_NUMBER: _ClassVar[int]
    ISSTEP_FIELD_NUMBER: _ClassVar[int]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    NEXTCURSOR_FIELD_NUMBER: _ClassVar[int]
    UNITEXTERNALID_FIELD_NUMBER: _ClassVar[int]
    NUMERICDATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    STRINGDATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    AGGREGATEDATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    id: int
    externalId: str
    instanceId: _data_points_pb2.InstanceId
    isString: bool
    isStep: bool
    unit: str
    nextCursor: str
    unitExternalId: str
    numericDatapoints: _data_points_pb2.NumericDatapoints
    stringDatapoints: _data_points_pb2.StringDatapoints
    aggregateDatapoints: _data_points_pb2.AggregateDatapoints
    def __init__(self, id: _Optional[int] = ..., externalId: _Optional[str] = ..., instanceId: _Optional[_Union[_data_points_pb2.InstanceId, _Mapping]] = ..., isString: bool = ..., isStep: bool = ..., unit: _Optional[str] = ..., nextCursor: _Optional[str] = ..., unitExternalId: _Optional[str] = ..., numericDatapoints: _Optional[_Union[_data_points_pb2.NumericDatapoints, _Mapping]] = ..., stringDatapoints: _Optional[_Union[_data_points_pb2.StringDatapoints, _Mapping]] = ..., aggregateDatapoints: _Optional[_Union[_data_points_pb2.AggregateDatapoints, _Mapping]] = ...) -> None: ...

class DataPointListResponse(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[DataPointListItem]
    def __init__(self, items: _Optional[_Iterable[_Union[DataPointListItem, _Mapping]]] = ...) -> None: ...
