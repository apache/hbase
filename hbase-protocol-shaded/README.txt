This module has proto files used by core.
For historical reasons and to signify that the generated classes are using
the relocated hbase-thirdparty protobuf-java library the generated classes are in
the org.apache.hadoop.hbase.shaded.protobuf.generated.* package, instead of the old
org.apache.hadoop.hbase.protobuf.generated.* package, which is not used at all by
Hbase 3 and later versions.

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
