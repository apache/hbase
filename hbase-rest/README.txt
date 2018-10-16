ON PROTOBUFS
This maven module has core protobuf definition files ('.protos') used by hbase
REST that ship with hbase core including tests.  The protobuf version
they use can be distinct from that used by HBase internally since HBase started
shading its protobuf references. REST Endpoints have no access to the shaded protobuf
hbase uses. They do have access to the content of hbase-protocol -- the
.protos found in here -- but avoid using as much of this as you can as it is
liable to change.

Generation of java files from protobuf .proto files included here is done as
part of the build.
