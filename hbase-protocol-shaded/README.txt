This module has proto files used by core. These protos
overlap with protos that are used by coprocessor endpoints
(CPEP) in the module hbase-protocol. So core versions have
a different name, the generated classes are relocated
-- i.e. shaded -- to a new location; they are moved from
org.apache.hadoop.hbase.* to org.apache.hadoop.hbase.shaded.
