/*
 *
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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestGlobalThrottler {
  private static final Log LOG = LogFactory.getLog(TestGlobalThrottler.class);
  private static Configuration conf1;
  private static Configuration conf2;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;

  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[] ROW = Bytes.toBytes("r");
  private static final byte[][] ROWS = HTestConst.makeNAscii(ROW, 100);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setLong("replication.source.sleepforretries", 100);
    // Each WAL is about 120 bytes
    conf1.setInt(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY, 200);
    conf1.setLong("replication.source.per.peer.node.bandwidth", 100L);

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    new ZooKeeperWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf2, "cluster2", null, true);

    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    admin1.addPeer("peer1", rpc, null);
    admin1.addPeer("peer2", rpc, null);
    admin1.addPeer("peer3", rpc, null);

    utility1.startMiniCluster(1, 1);
    utility2.startMiniCluster(1, 1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }


  volatile private boolean testQuotaPass = false;
  volatile private boolean testQuotaNonZero = false;
  @Test
  public void testQuota() throws IOException {
    TableName tableName = TableName.valueOf("testQuota");
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_SERIAL);
    table.addFamily(fam);
    utility1.getHBaseAdmin().createTable(table);
    utility2.getHBaseAdmin().createTable(table);

    Thread watcher = new Thread(new Runnable() {
      @Override
      public void run() {
        Replication replication = (Replication) utility1.getMiniHBaseCluster()
            .getRegionServer(0).getReplicationSourceService();
        AtomicLong bufferUsed = replication.getReplicationManager().getTotalBufferUsed();
        testQuotaPass = true;
        while (!Thread.interrupted()) {
          long size = bufferUsed.get();
          if (size > 0) {
            testQuotaNonZero = true;
          }
          if (size > 600) {
            // We read logs first then check throttler, so if the buffer quota limiter doesn't
            // take effect, it will push many logs and exceed the quota.
            testQuotaPass = false;
          }
          Threads.sleep(50);
        }
      }
    });

    watcher.start();

    try(Table t1 = utility1.getConnection().getTable(tableName);
        Table t2 = utility2.getConnection().getTable(tableName)) {
      for (int i = 0; i < 50; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(famName, VALUE, VALUE);
        t1.put(put);
      }
      long start = EnvironmentEdgeManager.currentTime();
      while (EnvironmentEdgeManager.currentTime() - start < 180000) {
        Scan scan = new Scan();
        scan.setCaching(50);
        int count = 0;
        try (ResultScanner results = t2.getScanner(scan)) {
          for (Result result : results) {
            count++;
          }
        }
        if (count < 50) {
          LOG.info("Waiting all logs pushed to slave. Expected 50 , actual " + count);
          Threads.sleep(200);
          continue;
        }
        break;
      }
    }

    watcher.interrupt();
    Assert.assertTrue(testQuotaPass);
    Assert.assertTrue(testQuotaNonZero);
  }

  private List<Integer> getRowNumbers(List<Cell> cells) {
    List<Integer> listOfRowNumbers = new ArrayList<>();
    for (Cell c : cells) {
      listOfRowNumbers.add(Integer.parseInt(Bytes
          .toString(c.getRowArray(), c.getRowOffset() + ROW.length,
              c.getRowLength() - ROW.length)));
    }
    return listOfRowNumbers;
  }
}
