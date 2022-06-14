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
package org.apache.hadoop.hbase.testing;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Setup two clusters for replication.
 */
public abstract class TestingHBaseClusterReplicationTestBase {

  protected TestingHBaseCluster sourceCluster;

  protected TestingHBaseCluster peerCluster;

  private Connection sourceConn;

  private Connection peerConn;

  private TableName tableName = TableName.valueOf("test_rep");

  private byte[] family = Bytes.toBytes("family");

  private String peerId = "peer_id";

  private String getPeerClusterKey() {
    return ZKConfig.getZooKeeperClusterKey(peerCluster.getConf());
  }

  @Before
  public void setUp() throws Exception {
    startClusters();
    sourceConn = ConnectionFactory.createConnection(sourceCluster.getConf());
    peerConn = ConnectionFactory.createConnection(peerCluster.getConf());
    TableDescriptor desc =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(family).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build();
    try (Admin admin = sourceConn.getAdmin()) {
      admin.createTable(desc);
      admin.addReplicationPeer(peerId, ReplicationPeerConfig.newBuilder()
        .setClusterKey(getPeerClusterKey()).setReplicateAllUserTables(true).build());
    }
    try (Admin admin = peerConn.getAdmin()) {
      admin.createTable(desc);
    }
  }

  @After
  public void tearDown() throws Exception {
    Closeables.close(sourceConn, true);
    Closeables.close(peerConn, true);
    if (sourceCluster != null) {
      sourceCluster.stop();
    }
    if (peerCluster != null) {
      peerCluster.stop();
    }
    stopClusters();
  }

  @Test
  public void testReplication() throws IOException {
    byte[] row = Bytes.toBytes("row");
    byte[] qual = Bytes.toBytes("qual");
    byte[] value = Bytes.toBytes("value");
    try (Table sourceTable = sourceConn.getTable(tableName);
      Table peerTable = peerConn.getTable(tableName);) {
      sourceTable.put(new Put(row).addColumn(family, qual, value));
      Waiter.waitFor(sourceCluster.getConf(), 30000,
        () -> peerTable.exists(new Get(row).addColumn(family, qual)));
      byte[] actual = peerTable.get(new Get(row)).getValue(family, qual);
      assertArrayEquals(value, actual);
    }
  }

  protected abstract void startClusters() throws Exception;

  protected abstract void stopClusters() throws Exception;
}
