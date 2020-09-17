This module has proto files used by core. These protos
overlap with protos that are used by coprocessor endpoints
(CPEP) in the module hbase-protocol. So core versions have
a different name, the generated classes are relocated
-- i.e. shaded -- to a new location; they are moved from
org.apache.hadoop.hbase.* to org.apache.hadoop.hbase.shaded.

proto files layout:
protobuf/client - client to server messages, client rpc service and protos, used in hbase-client exclusively;
protobuf/rest - hbase-rest messages;
protobuf/rpc - rpc and post-rpc tracing messages;
protobuf/server/coprocessor - coprocessor rpc services;
protobuf/server/coprocessor/example - coprocessors rpc services examples from hbase-examples;
protobuf/server/io - filesystem and hbase-server/io protos;
protobuf/server/maser - master rpc services and messages;
protobuf/server/region - region rpc services and messages (except client rpc service, which is in Client.proto);
protobuf/server/rsgroup - rsgroup protos;
protobuf/server/zookeeper - protos for zookeeper and ones used exclusively in hbase-zookeeper module;
protobuf/server - protos used across other server protos;
protobuf/test - protos used in tests;
protobuf/ - protos used across other protos, exclusive for hbase-mapreduce and hbase-backup, other protos.
