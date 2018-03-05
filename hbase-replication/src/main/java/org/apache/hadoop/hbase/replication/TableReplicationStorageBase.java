/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class TableReplicationStorageBase {
  protected final ZKWatcher zookeeper;
  protected final Configuration conf;

  public static final TableName REPLICATION_TABLE =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "replication");

  // Peer family, the row key would be peer id.
  public static final byte[] FAMILY_PEER = Bytes.toBytes("peer");
  public static final byte[] QUALIFIER_PEER_CONFIG = Bytes.toBytes("config");
  public static final byte[] QUALIFIER_PEER_STATE = Bytes.toBytes("state");

  // Region server state family, the row key would be name of region server.
  public static final byte[] FAMILY_RS_STATE = Bytes.toBytes("rs_state");
  public static final byte[] QUALIFIER_STATE_ENABLED = Bytes.toBytes("enabled");

  // Queue and wal family, the row key would be name of region server.
  public static final byte[] FAMILY_QUEUE = Bytes.toBytes("queue");
  public static final byte[] FAMILY_WAL = Bytes.toBytes("wal");

  // HFile-Refs family, the row key would be peer id.
  public static final byte[] FAMILY_HFILE_REFS = Bytes.toBytes("hfile-refs");

  // Region family, the row key would be peer id.
  public static final byte[] FAMILY_REGIONS = Bytes.toBytes("regions");

  private Connection connection;

  protected static byte[] getServerNameRowKey(ServerName serverName) {
    return Bytes.toBytes(serverName.toString());
  }

  protected static byte[] getRegionQualifier(String encodedRegionName) {
    return Bytes.toBytes(encodedRegionName);
  }

  @VisibleForTesting
  public static TableDescriptorBuilder createReplicationTableDescBuilder(final Configuration conf)
      throws IOException {
    int metaMaxVersion =
        conf.getInt(HConstants.HBASE_META_VERSIONS, HConstants.DEFAULT_HBASE_META_VERSIONS);
    int metaBlockSize =
        conf.getInt(HConstants.HBASE_META_BLOCK_SIZE, HConstants.DEFAULT_HBASE_META_BLOCK_SIZE);
    return TableDescriptorBuilder
        .newBuilder(REPLICATION_TABLE)
        .addColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_PEER).setMaxVersions(metaMaxVersion)
              .setInMemory(true).setBlocksize(metaBlockSize)
              .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setBloomFilterType(BloomType.NONE)
              .build())
        .addColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_RS_STATE).setMaxVersions(metaMaxVersion)
              .setInMemory(true).setBlocksize(metaBlockSize)
              .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setBloomFilterType(BloomType.NONE)
              .build())
        .addColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_QUEUE).setMaxVersions(metaMaxVersion)
              .setInMemory(true).setBlocksize(metaBlockSize)
              .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setBloomFilterType(BloomType.NONE)
              .build())
        .addColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_WAL)
              .setMaxVersions(HConstants.ALL_VERSIONS).setInMemory(true)
              .setBlocksize(metaBlockSize).setScope(HConstants.REPLICATION_SCOPE_LOCAL)
              .setBloomFilterType(BloomType.NONE).build())
        .addColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_REGIONS).setMaxVersions(metaMaxVersion)
              .setInMemory(true).setBlocksize(metaBlockSize)
              .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setBloomFilterType(BloomType.NONE)
              .build())
        .addColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_HFILE_REFS)
              .setMaxVersions(metaMaxVersion).setInMemory(true).setBlocksize(metaBlockSize)
              .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setBloomFilterType(BloomType.NONE)
              .build());
  }

  protected TableReplicationStorageBase(ZKWatcher zookeeper, Configuration conf)
      throws IOException {
    this.zookeeper = zookeeper;
    this.conf = conf;
    this.connection = ConnectionFactory.createConnection(conf);
  }

  protected Table getReplicationMetaTable() throws IOException {
    return this.connection.getTable(REPLICATION_TABLE);
  }
}
