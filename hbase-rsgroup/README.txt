ON PROTOBUFS
This maven module has protobuf definition files ('.protos') used by hbase
Coprocessor Endpoints that ship with hbase core including tests. Coprocessor
Endpoints are meant to be standalone, independent code not reliant on hbase
internals. They define their Service using protobuf. The protobuf version
they use can be distinct from that used by HBase internally since HBase started
shading its protobuf references. Endpoints have no access to the shaded protobuf
hbase uses. They do have access to the content of hbase-protocol -- the
.protos found in here -- but avoid using as much of this as you can as it is
liable to change.

Generation of java files from protobuf .proto files included here is done as
part of the build.
