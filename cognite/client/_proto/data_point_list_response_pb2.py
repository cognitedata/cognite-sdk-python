from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
_sym_db = _symbol_database.Default()
import cognite.client._proto.data_points_pb2 as data__points__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1edata_point_list_response.proto\x12\x1fcom.cognite.v1.timeseries.proto\x1a\x11data_points.proto"\xfd\x02\n\x11DataPointListItem\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x12\n\nexternalId\x18\x02 \x01(\t\x12\x10\n\x08isString\x18\x06 \x01(\x08\x12\x0e\n\x06isStep\x18\x07 \x01(\x08\x12\x0c\n\x04unit\x18\x08 \x01(\t\x12\x12\n\nnextCursor\x18\t \x01(\t\x12O\n\x11numericDatapoints\x18\x03 \x01(\x0b22.com.cognite.v1.timeseries.proto.NumericDatapointsH\x00\x12M\n\x10stringDatapoints\x18\x04 \x01(\x0b21.com.cognite.v1.timeseries.proto.StringDatapointsH\x00\x12S\n\x13aggregateDatapoints\x18\x05 \x01(\x0b24.com.cognite.v1.timeseries.proto.AggregateDatapointsH\x00B\x0f\n\rdatapointType"Z\n\x15DataPointListResponse\x12A\n\x05items\x18\x01 \x03(\x0b22.com.cognite.v1.timeseries.proto.DataPointListItemB\x02P\x01b\x06proto3')
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'data_point_list_response_pb2', globals())
if (_descriptor._USE_C_DESCRIPTORS == False):
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b'P\x01'
    _DATAPOINTLISTITEM._serialized_start = 87
    _DATAPOINTLISTITEM._serialized_end = 468
    _DATAPOINTLISTRESPONSE._serialized_start = 470
    _DATAPOINTLISTRESPONSE._serialized_end = 560
