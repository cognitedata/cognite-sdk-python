import cognite.client._proto.data_points_pb2 as _data_points_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataPointInsertionItem(_message.Message):
    __slots__ = ("id", "externalId", "instanceId", "numericDatapoints", "stringDatapoints", "stateDatapoints")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNALID_FIELD_NUMBER: _ClassVar[int]
    INSTANCEID_FIELD_NUMBER: _ClassVar[int]
    NUMERICDATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    STRINGDATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    STATEDATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    id: int
    externalId: str
    instanceId: _data_points_pb2.InstanceId
    numericDatapoints: _data_points_pb2.NumericDatapoints
    stringDatapoints: _data_points_pb2.StringDatapoints
    stateDatapoints: _data_points_pb2.StateDatapoints
    def __init__(self, id: _Optional[int] = ..., externalId: _Optional[str] = ..., instanceId: _Optional[_Union[_data_points_pb2.InstanceId, _Mapping]] = ..., numericDatapoints: _Optional[_Union[_data_points_pb2.NumericDatapoints, _Mapping]] = ..., stringDatapoints: _Optional[_Union[_data_points_pb2.StringDatapoints, _Mapping]] = ..., stateDatapoints: _Optional[_Union[_data_points_pb2.StateDatapoints, _Mapping]] = ...) -> None: ...

class DataPointInsertionRequest(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[DataPointInsertionItem]
    def __init__(self, items: _Optional[_Iterable[_Union[DataPointInsertionItem, _Mapping]]] = ...) -> None: ...
