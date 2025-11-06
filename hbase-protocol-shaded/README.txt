This module has proto files used by core. These protos
overlap with protos that are used by coprocessor endpoints
(CPEP) in the module hbase-protocol. To make sure that the
core versions have a different names, these classes are
using a different package;
they are in
org.apache.hadoop.hbase.shaded.protobuf.generated.*
, while the hbase-protocol classes are in
org.apache.hadoop.hbase.protobuf.generated.*.
