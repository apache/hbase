/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.TableReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ZKReplicationPeerStorage;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to create replication storage(peer, queue) classes.
 */
@InterfaceAudience.Private
public final class ReplicationStorageFactory {

  public static final String REPLICATION_QUEUE_TABLE_NAME = "hbase.replication.queue.table.name";

  public static final TableName REPLICATION_QUEUE_TABLE_NAME_DEFAULT =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "replication");

  public static TableDescriptor createReplicationQueueTableDescriptor(TableName tableName)
    throws IOException {
    return TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TableReplicationQueueStorage.QUEUE_FAMILY))
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.of(TableReplicationQueueStorage.LAST_SEQUENCE_ID_FAMILY))
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.of(TableReplicationQueueStorage.HFILE_REF_FAMILY))
      .setValue("hbase.regionserver.region.split_restriction.type", "DelimitedKeyPrefix")
      .setValue("hbase.regionserver.region.split_restriction.delimiter", "-")
      .setCoprocessor(CoprocessorDescriptorBuilder
        .newBuilder("org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint")
        .setPriority(Coprocessor.PRIORITY_SYSTEM).build())
      .build();
  }

  private ReplicationStorageFactory() {
  }

  /**
   * Create a new {@link ReplicationPeerStorage}.
   */
  public static ReplicationPeerStorage getReplicationPeerStorage(ZKWatcher zk, Configuration conf) {
    return new ZKReplicationPeerStorage(zk, conf);
  }

  /**
   * Create a new {@link ReplicationQueueStorage}.
   */
  public static ReplicationQueueStorage getReplicationQueueStorage(Connection conn,
    Configuration conf) throws IOException {
    TableName replicationQueueTable = TableName.valueOf(conf.get(REPLICATION_QUEUE_TABLE_NAME,
      REPLICATION_QUEUE_TABLE_NAME_DEFAULT.getNameAsString()));
    if (conf.getBoolean("hbase.replication.syncup.enabled", false)) {
      return getOfflineTableReplicationQueueStorage(conf, replicationQueueTable);
    } else {
      return getReplicationQueueStorage(conn, replicationQueueTable);
    }
  }

  /**
   * Create a new {@link ReplicationQueueStorage}.
   */
  public static ReplicationQueueStorage getReplicationQueueStorage(Connection conn,
    TableName tableName) {
    return new TableReplicationQueueStorage(conn, tableName);
  }

  /**
   * Create a new {@link ReplicationQueueStorage}.
   */
  public static ReplicationQueueStorage getOfflineTableReplicationQueueStorage(Configuration conf,
    TableName tableName) throws IOException {
    return new OfflineTableReplicationQueueStorage(conf, tableName);
  }
}
