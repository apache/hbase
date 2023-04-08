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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to create replication storage(peer, queue) classes.
 */
@InterfaceAudience.Private
public final class ReplicationStorageFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationStorageFactory.class);

  public static final String REPLICATION_PEER_STORAGE_IMPL = "hbase.replication.peer.storage.impl";

  // must use zookeeper here, otherwise when user upgrading from an old version without changing the
  // config file, they will loss all the replication peer data.
  public static final ReplicationPeerStorageType DEFAULT_REPLICATION_PEER_STORAGE_IMPL =
    ReplicationPeerStorageType.ZOOKEEPER;

  public static final String REPLICATION_QUEUE_TABLE_NAME = "hbase.replication.queue.table.name";

  public static final TableName REPLICATION_QUEUE_TABLE_NAME_DEFAULT =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "replication");

  public static final String REPLICATION_QUEUE_IMPL = "hbase.replication.queue.storage.impl";

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

  private static Class<? extends ReplicationPeerStorage>
    getReplicationPeerStorageClass(Configuration conf) {
    try {
      ReplicationPeerStorageType type = ReplicationPeerStorageType.valueOf(
        conf.get(REPLICATION_PEER_STORAGE_IMPL, DEFAULT_REPLICATION_PEER_STORAGE_IMPL.name())
          .toUpperCase());
      return type.getClazz();
    } catch (IllegalArgumentException e) {
      return conf.getClass(REPLICATION_PEER_STORAGE_IMPL,
        DEFAULT_REPLICATION_PEER_STORAGE_IMPL.getClazz(), ReplicationPeerStorage.class);
    }
  }

  /**
   * Create a new {@link ReplicationPeerStorage}.
   */
  public static ReplicationPeerStorage getReplicationPeerStorage(FileSystem fs, ZKWatcher zk,
    Configuration conf) {
    Class<? extends ReplicationPeerStorage> clazz = getReplicationPeerStorageClass(conf);
    for (Constructor<?> c : clazz.getConstructors()) {
      if (c.getParameterCount() != 2) {
        continue;
      }
      if (c.getParameterTypes()[0].isAssignableFrom(FileSystem.class)) {
        return ReflectionUtils.newInstance(clazz, fs, conf);
      } else if (c.getParameterTypes()[0].isAssignableFrom(ZKWatcher.class)) {
        return ReflectionUtils.newInstance(clazz, zk, conf);
      }
    }
    throw new IllegalArgumentException(
      "Can not create replication peer storage with type " + clazz);
  }

  /**
   * Create a new {@link ReplicationQueueStorage}.
   */
  public static ReplicationQueueStorage getReplicationQueueStorage(Connection conn,
    Configuration conf) {
    return getReplicationQueueStorage(conn, conf, TableName.valueOf(conf
      .get(REPLICATION_QUEUE_TABLE_NAME, REPLICATION_QUEUE_TABLE_NAME_DEFAULT.getNameAsString())));
  }

  /**
   * Create a new {@link ReplicationQueueStorage}.
   */
  public static ReplicationQueueStorage getReplicationQueueStorage(Connection conn,
    Configuration conf, TableName tableName) {
    Class<? extends ReplicationQueueStorage> clazz = conf.getClass(REPLICATION_QUEUE_IMPL,
      TableReplicationQueueStorage.class, ReplicationQueueStorage.class);
    try {
      Constructor<? extends ReplicationQueueStorage> c =
        clazz.getConstructor(Connection.class, TableName.class);
      return c.newInstance(conn, tableName);
    } catch (Exception e) {
      LOG.debug(
        "failed to create ReplicationQueueStorage with Connection, try creating with Configuration",
        e);
      return ReflectionUtils.newInstance(clazz, conf, tableName);
    }
  }

  public static boolean isReplicationQueueTable(Configuration conf, TableName tableName) {
    TableName replicationQueueTableName = TableName.valueOf(conf.get(REPLICATION_QUEUE_TABLE_NAME,
      REPLICATION_QUEUE_TABLE_NAME_DEFAULT.getNameAsString()));
    return replicationQueueTableName.equals(tableName);
  }
}
