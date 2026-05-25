This module contains coprocessor endpoint implementations that ship with
HBase core. Coprocessor endpoints are standalone RPC services deployed on
region servers (or master) via the coprocessor framework.

Included endpoints:
  - AggregateImplementation -- server-side aggregation (sum, min, max, avg, etc.)
  - Export -- server-side table export to HDFS

Client helpers for invoking these endpoints are in the
org.apache.hadoop.hbase.client.coprocessor package (AggregationClient,
AsyncAggregationClient).

The protobuf service definitions used by these endpoints live in
hbase-protocol-shaded, not in this module.
