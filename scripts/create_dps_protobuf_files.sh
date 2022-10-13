# Get the .proto files from here:
# https://github.com/cognitedata/protobuf-files
# - data_point_list_response.proto
# - data_points.proto

# Then, run `protoc` from the same directory:
# $ protoc --python_out=cognite/client/proto data_point_list_response.proto data_points.proto
