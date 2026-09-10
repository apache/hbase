ON PROTOBUFS
This module defines protobuf message types used by the HBase REST gateway
(see src/main/protobuf/). These are the REST-specific wire formats for
resources like cells, tables, scanners, and cluster status -- they are
separate from the internal HBase RPC protobufs that live in
hbase-protocol-shaded.

The REST protos are compiled and shaded using the same pipeline as
hbase-protocol-shaded (protobuf-maven-plugin + replacer plugin rewriting
com.google.protobuf to the hbase-thirdparty shaded package). The build
config comment in pom.xml notes it is "copied directly from
hbase-shaded-protocol, and should be kept in sync."

Java sources are generated from the .proto files as part of the build.
