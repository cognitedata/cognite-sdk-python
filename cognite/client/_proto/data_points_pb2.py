
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
_sym_db = _symbol_database.Default()
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11data_points.proto\x12\x1fcom.cognite.v1.timeseries.proto"4\n\x10NumericDatapoint\x12\x11\n\ttimestamp\x18\x01 \x01(\x03\x12\r\n\x05value\x18\x02 \x01(\x01"Z\n\x11NumericDatapoints\x12E\n\ndatapoints\x18\x01 \x03(\x0b21.com.cognite.v1.timeseries.proto.NumericDatapoint"3\n\x0fStringDatapoint\x12\x11\n\ttimestamp\x18\x01 \x01(\x03\x12\r\n\x05value\x18\x02 \x01(\t"X\n\x10StringDatapoints\x12D\n\ndatapoints\x18\x01 \x03(\x0b20.com.cognite.v1.timeseries.proto.StringDatapoint"\xee\x01\n\x12AggregateDatapoint\x12\x11\n\ttimestamp\x18\x01 \x01(\x03\x12\x0f\n\x07average\x18\x02 \x01(\x01\x12\x0b\n\x03max\x18\x03 \x01(\x01\x12\x0b\n\x03min\x18\x04 \x01(\x01\x12\r\n\x05count\x18\x05 \x01(\x01\x12\x0b\n\x03sum\x18\x06 \x01(\x01\x12\x15\n\rinterpolation\x18\x07 \x01(\x01\x12\x19\n\x11stepInterpolation\x18\x08 \x01(\x01\x12\x1a\n\x12continuousVariance\x18\t \x01(\x01\x12\x18\n\x10discreteVariance\x18\n \x01(\x01\x12\x16\n\x0etotalVariation\x18\x0b \x01(\x01"^\n\x13AggregateDatapoints\x12G\n\ndatapoints\x18\x01 \x03(\x0b23.com.cognite.v1.timeseries.proto.AggregateDatapointB\x02P\x01b\x06proto3')
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'data_points_pb2', globals())
if (_descriptor._USE_C_DESCRIPTORS == False):
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b'P\x01'
    _NUMERICDATAPOINT._serialized_start = 54
    _NUMERICDATAPOINT._serialized_end = 106
    _NUMERICDATAPOINTS._serialized_start = 108
    _NUMERICDATAPOINTS._serialized_end = 198
    _STRINGDATAPOINT._serialized_start = 200
    _STRINGDATAPOINT._serialized_end = 251
    _STRINGDATAPOINTS._serialized_start = 253
    _STRINGDATAPOINTS._serialized_end = 341
    _AGGREGATEDATAPOINT._serialized_start = 344
    _AGGREGATEDATAPOINT._serialized_end = 582
    _AGGREGATEDATAPOINTS._serialized_start = 584
    _AGGREGATEDATAPOINTS._serialized_end = 678
