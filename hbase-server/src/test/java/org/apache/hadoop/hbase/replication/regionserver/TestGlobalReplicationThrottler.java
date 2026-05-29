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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestGlobalReplicationThrottler {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestGlobalReplicationThrottler.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestGlobalReplicationThrottler.class);
  private static final int REPLICATION_SOURCE_QUOTA = 200;
  private static int numOfPeer = 0;
  private static Configuration conf1;
  private static Configuration conf2;

  private static HBaseTestingUtil utility1;
  private static HBaseTestingUtil utility2;

  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[] ROW = Bytes.toBytes("r");
  private static final byte[][] ROWS = HTestConst.makeNAscii(ROW, 100);

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setLong("replication.source.sleepforretries", 100);
    // Each WAL is about 120 bytes
    conf1.setInt(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY, REPLICATION_SOURCE_QUOTA);
    conf1.setLong("replication.source.per.peer.node.bandwidth", 100L);

    utility1 = new HBaseTestingUtil(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    new ZKWatcher(conf1, "cluster1", null, true).close();

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    utility2 = new HBaseTestingUtil(conf2);
    utility2.setZkCluster(miniZK);
    new ZKWatcher(conf2, "cluster2", null, true).close();

    utility1.startMiniCluster();
    utility2.startMiniCluster();

    ReplicationPeerConfig rpc =
      ReplicationPeerConfig.newBuilder().setClusterKey(utility2.getRpcConnnectionURI()).build();

    try (Connection connection = ConnectionFactory.createConnection(utility1.getConfiguration());
      Admin admin1 = connection.getAdmin()) {
      admin1.addReplicationPeer("peer1", rpc);
      admin1.addReplicationPeer("peer2", rpc);
      admin1.addReplicationPeer("peer3", rpc);
      numOfPeer = admin1.listReplicationPeers().size();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  private volatile boolean testQuotaPass = false;
  private volatile boolean testQuotaNonZero = false;

  @Test
  public void testQuota() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(famName).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build();
    utility1.getAdmin().createTable(tableDescriptor);
    utility2.getAdmin().createTable(tableDescriptor);

    Thread watcher = new Thread(() -> {
      Replication replication = (Replication) utility1.getMiniHBaseCluster().getRegionServer(0)
        .getReplicationSourceService();
      testQuotaPass = true;
      while (!Thread.interrupted()) {
        long size = replication.getReplicationManager().getTotalBufferUsed();
        if (size > 0) {
          testQuotaNonZero = true;
        }
        // the reason here doing "numOfPeer + 1" is because by using method addEntryToBatch(), even
        // the batch size (after added last entry) exceeds quota, it still keeps the last one in the
        // batch so total used buffer size can be one "replication.total.buffer.quota" larger than
        // expected
        if (size > REPLICATION_SOURCE_QUOTA * (numOfPeer + 1)) {
          // We read logs first then check throttler, so if the buffer quota limiter doesn't
          // take effect, it will push many logs and exceed the quota.
          testQuotaPass = false;
        }
        Threads.sleep(50);
      }
    });
    watcher.start();

    try (Table t1 = utility1.getConnection().getTable(tableName)) {
      for (int i = 0; i < 50; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(famName, VALUE, VALUE);
        t1.put(put);
      }
    }
    utility2.waitFor(180000, () -> {
      try (Table t2 = utility2.getConnection().getTable(tableName);
        ResultScanner results = t2.getScanner(new Scan().setCaching(50))) {
        int count = Iterables.size(results);
        return count >= 50;
      }
    });

    watcher.interrupt();
    assertTrue(testQuotaPass);
    assertTrue(testQuotaNonZero);
  }

}
