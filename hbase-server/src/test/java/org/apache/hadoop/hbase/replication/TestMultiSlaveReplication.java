/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.cleaner.ReplicationZKLockCleanerChore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ReplicationTests.class, LargeTests.class})
public class TestMultiSlaveReplication {

  private static final Log LOG = LogFactory.getLog(TestReplicationBase.class);

  private static Configuration conf1;
  private static Configuration conf2;
  private static Configuration conf3;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static HBaseTestingUtility utility3;
  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 100;

  private static final TableName tableName = TableName.valueOf("test");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");
  private static final byte[] row3 = Bytes.toBytes("row3");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");

  private static HTableDescriptor table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // smaller block size and capacity to trigger more operations
    // and test them
    conf1.setInt("hbase.regionserver.hlog.blocksize", 1024*20);
    conf1.setInt("replication.source.size.capacity", 1024);
    conf1.setLong("replication.source.sleepforretries", 100);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.replication.TestMasterReplication$CoprocessorCounter");
    conf1.setBoolean(HConstants.ZOOKEEPER_USEMULTI , false);// for testZKLockCleaner
    conf1.setInt("hbase.master.cleaner.interval", 5 * 1000);
    conf1.setClass("hbase.region.replica.replication.replicationQueues.class",
        ReplicationQueuesZKImpl.class, ReplicationQueues.class);
    conf1.setLong(ReplicationZKLockCleanerChore.TTL_CONFIG_KEY, 0L);


    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    new ZooKeeperWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    conf3 = new Configuration(conf1);
    conf3.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf2, "cluster2", null, true);

    utility3 = new HBaseTestingUtility(conf3);
    utility3.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf3, "cluster3", null, true);

    table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    fam = new HColumnDescriptor(noRepfamName);
    table.addFamily(fam);
  }

  @Test(timeout=300000)
  public void testMultiSlaveReplication() throws Exception {
    LOG.info("testCyclicReplication");
    MiniHBaseCluster master = utility1.startMiniCluster();
    utility2.startMiniCluster();
    utility3.startMiniCluster();
    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);

    utility1.getHBaseAdmin().createTable(table);
    utility2.getHBaseAdmin().createTable(table);
    utility3.getHBaseAdmin().createTable(table);
    Table htable1 = utility1.getConnection().getTable(tableName);
    htable1.setWriteBufferSize(1024);
    Table htable2 = utility2.getConnection().getTable(tableName);
    htable2.setWriteBufferSize(1024);
    Table htable3 = utility3.getConnection().getTable(tableName);
    htable3.setWriteBufferSize(1024);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    admin1.addPeer("1", rpc, null);

    // put "row" and wait 'til it got around, then delete
    putAndWait(row, famName, htable1, htable2);
    deleteAndWait(row, htable1, htable2);
    // check it wasn't replication to cluster 3
    checkRow(row,0,htable3);

    putAndWait(row2, famName, htable1, htable2);

    // now roll the region server's logs
    rollWALAndWait(utility1, htable1.getName(), row2);

    // after the log was rolled put a new row
    putAndWait(row3, famName, htable1, htable2);

    rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility3.getClusterKey());
    admin1.addPeer("2", rpc, null);

    // put a row, check it was replicated to all clusters
    putAndWait(row1, famName, htable1, htable2, htable3);
    // delete and verify
    deleteAndWait(row1, htable1, htable2, htable3);

    // make sure row2 did not get replicated after
    // cluster 3 was added
    checkRow(row2,0,htable3);

    // row3 will get replicated, because it was in the
    // latest log
    checkRow(row3,1,htable3);

    Put p = new Put(row);
    p.addColumn(famName, row, row);
    htable1.put(p);
    // now roll the logs again
    rollWALAndWait(utility1, htable1.getName(), row);

    // cleanup "row2", also conveniently use this to wait replication
    // to finish
    deleteAndWait(row2, htable1, htable2, htable3);
    // Even if the log was rolled in the middle of the replication
    // "row" is still replication.
    checkRow(row, 1, htable2);
    // Replication thread of cluster 2 may be sleeping, and since row2 is not there in it, 
    // we should wait before checking.
    checkWithWait(row, 1, htable3);

    // cleanup the rest
    deleteAndWait(row, htable1, htable2, htable3);
    deleteAndWait(row3, htable1, htable2, htable3);

    utility3.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  @Test
  public void testZKLockCleaner() throws Exception {
    MiniHBaseCluster cluster = utility1.startMiniCluster(1, 1);
    HBaseAdmin admin = utility1.getHBaseAdmin();
    admin.createTable(table);
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf1);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    replicationAdmin.addPeer("2", rpc, null);
    HRegionServer rs = cluster.getRegionServer(0);
    ReplicationQueuesZKImpl zk = new ReplicationQueuesZKImpl(rs.getZooKeeper(), conf1, rs);
    zk.init(rs.getServerName().toString());
    List<String> replicators = zk.getListOfReplicators();
    assertEquals(2, replicators.size());
    String zNode = cluster.getMaster().getServerName().toString();

    assertTrue(zk.lockOtherRS(zNode));
    assertTrue(zk.checkLockExists(zNode));
    Thread.sleep(10000);
    assertTrue(zk.checkLockExists(zNode));
    cluster.abortRegionServer(0);
    Thread.sleep(10000);
    HRegionServer rs1 = cluster.getMaster();
    zk = new ReplicationQueuesZKImpl(rs1.getZooKeeper(), conf1, rs1);
    zk.init(rs1.getServerName().toString());
    assertFalse(zk.checkLockExists(zNode));

    utility1.shutdownMiniCluster();
  }

  private void rollWALAndWait(final HBaseTestingUtility utility, final TableName table,
      final byte[] row) throws IOException {
    final Admin admin = utility.getHBaseAdmin();
    final MiniHBaseCluster cluster = utility.getMiniHBaseCluster();

    // find the region that corresponds to the given row.
    HRegion region = null;
    for (HRegion candidate : cluster.getRegions(table)) {
      if (HRegion.rowIsInRange(candidate.getRegionInfo(), row)) {
        region = candidate;
        break;
      }
    }
    assertNotNull("Couldn't find the region for row '" + Arrays.toString(row) + "'", region);

    final CountDownLatch latch = new CountDownLatch(1);

    // listen for successful log rolls
    final WALActionsListener listener = new WALActionsListener.Base() {
          @Override
          public void postLogRoll(final Path oldPath, final Path newPath) throws IOException {
            latch.countDown();
          }
        };
    region.getWAL().registerWALActionsListener(listener);

    // request a roll
    admin.rollWALWriter(cluster.getServerHoldingRegion(region.getTableDesc().getTableName(),
      region.getRegionInfo().getRegionName()));

    // wait
    try {
      latch.await();
    } catch (InterruptedException exception) {
      LOG.warn("Interrupted while waiting for the wal of '" + region + "' to roll. If later " +
          "replication tests fail, it's probably because we should still be waiting.");
      Thread.currentThread().interrupt();
    }
    region.getWAL().unregisterWALActionsListener(listener);
  }

 
  private void checkWithWait(byte[] row, int count, Table table) throws Exception {
    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time while getting the row.");
      }
      boolean rowReplicated = false;
      Result res = table.get(get);
      if (res.size() >= 1) {
        LOG.info("Row is replicated");
        rowReplicated = true;
        assertEquals("Table '" + table + "' did not have the expected number of  results.",
            count, res.size());
        break;
      }
      if (rowReplicated) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }
  
  private void checkRow(byte[] row, int count, Table... tables) throws IOException {
    Get get = new Get(row);
    for (Table table : tables) {
      Result res = table.get(get);
      assertEquals("Table '" + table + "' did not have the expected number of results.",
          count, res.size());
    }
  }

  private void deleteAndWait(byte[] row, Table source, Table... targets)
  throws Exception {
    Delete del = new Delete(row);
    source.delete(del);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      boolean removedFromAll = true;
      for (Table target : targets) {
        Result res = target.get(get);
        if (res.size() >= 1) {
          LOG.info("Row not deleted");
          removedFromAll = false;
          break;
        }
      }
      if (removedFromAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);        
      }
    }
  }

  private void putAndWait(byte[] row, byte[] fam, Table source, Table... targets)
  throws Exception {
    Put put = new Put(row);
    put.addColumn(fam, row, row);
    source.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      boolean replicatedToAll = true;
      for (Table target : targets) {
        Result res = target.get(get);
        if (res.size() == 0) {
          LOG.info("Row not available");
          replicatedToAll = false;
          break;
        } else {
          assertArrayEquals(res.value(), row);
        }
      }
      if (replicatedToAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

}

