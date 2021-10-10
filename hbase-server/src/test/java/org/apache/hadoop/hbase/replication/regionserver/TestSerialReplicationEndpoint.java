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

package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestSerialReplicationEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSerialReplicationEndpoint.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration CONF;
  private static Connection CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster();
    CONF = UTIL.getConfiguration();
    CONF.setLong(RpcServer.MAX_REQUEST_SIZE, 102400);
    CONN = UTIL.getConnection();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  private String getZKClusterKey() {
    return String.format("127.0.0.1:%d:%s", UTIL.getZkCluster().getClientPort(),
      CONF.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
  }

  private void testHBaseReplicationEndpoint(String tableNameStr, String peerId, boolean isSerial)
      throws IOException {
    TestEndpoint.reset();
    int cellNum = 10000;

    TableName tableName = TableName.valueOf(tableNameStr);
    byte[] family = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    TableDescriptor td =
        TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder
            .newBuilder(family).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build();
    UTIL.createTable(td, null);

    try (Admin admin = CONN.getAdmin()) {
      ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
          .setClusterKey(getZKClusterKey()).setReplicationEndpointImpl(TestEndpoint.class.getName())
          .setReplicateAllUserTables(false).setSerial(isSerial)
          .setTableCFsMap(ImmutableMap.of(tableName, ImmutableList.of())).build();
      admin.addReplicationPeer(peerId, peerConfig);
    }

    try (Table table = CONN.getTable(tableName)) {
      for (int i = 0; i < cellNum; i++) {
        Put put = new Put(Bytes.toBytes(i)).addColumn(family, qualifier, System.currentTimeMillis(),
          Bytes.toBytes(i));
        table.put(put);
      }
    }
    Waiter.waitFor(CONF, 60000, () -> TestEndpoint.getEntries().size() >= cellNum);

    int index = 0;
    Assert.assertEquals(TestEndpoint.getEntries().size(), cellNum);
    if (!isSerial) {
      Collections.sort(TestEndpoint.getEntries(), (a, b) -> {
        long seqA = a.getKey().getSequenceId();
        long seqB = b.getKey().getSequenceId();
        return seqA == seqB ? 0 : (seqA < seqB ? -1 : 1);
      });
    }
    for (Entry entry : TestEndpoint.getEntries()) {
      Assert.assertEquals(entry.getKey().getTableName(), tableName);
      Assert.assertEquals(entry.getEdit().getCells().size(), 1);
      Cell cell = entry.getEdit().getCells().get(0);
      Assert.assertArrayEquals(
        Bytes.copy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
        Bytes.toBytes(index));
      index++;
    }
    Assert.assertEquals(index, cellNum);
  }

  @Test
  public void testSerialReplicate() throws Exception {
    testHBaseReplicationEndpoint("testSerialReplicate", "100", true);
  }

  @Test
  public void testParallelReplicate() throws Exception {
    testHBaseReplicationEndpoint("testParallelReplicate", "101", false);
  }

  public static class TestEndpoint extends HBaseInterClusterReplicationEndpoint {

    private final static BlockingQueue<Entry> entryQueue = new LinkedBlockingQueue<>();

    public static void reset() {
      entryQueue.clear();
    }

    public static List<Entry> getEntries() {
      return new ArrayList<>(entryQueue);
    }

    @Override
    public boolean canReplicateToSameCluster() {
      return true;
    }

    @Override
    protected Callable<Integer> createReplicator(List<Entry> entries, int ordinal, int timeout) {
      return () -> {
        entryQueue.addAll(entries);
        return ordinal;
      };
    }

    @Override
    public synchronized List<ServerName> getRegionServers() {
      // Return multiple server names for endpoint parallel replication.
      return new ArrayList<>(
          ImmutableList.of(ServerName.valueOf("www.example.com", 12016, 1525245876026L),
            ServerName.valueOf("www.example2.com", 12016, 1525245876026L),
            ServerName.valueOf("www.example3.com", 12016, 1525245876026L),
            ServerName.valueOf("www.example4.com", 12016, 1525245876026L),
            ServerName.valueOf("www.example4.com", 12016, 1525245876026L)));
    }
  }
}
