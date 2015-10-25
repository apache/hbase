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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ReplicationTests.class, LargeTests.class})
public class TestMasterReplication {

  private static final Log LOG = LogFactory.getLog(TestReplicationBase.class);

  private Configuration baseConfiguration;

  private HBaseTestingUtility[] utilities;
  private Configuration[] configurations;
  private MiniZooKeeperCluster miniZK;

  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 10;

  private static final TableName tableName = TableName.valueOf("test");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");
  private static final byte[] row3 = Bytes.toBytes("row3");
  private static final byte[] row4 = Bytes.toBytes("row4");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");

  private static final byte[] count = Bytes.toBytes("count");
  private static final byte[] put = Bytes.toBytes("put");
  private static final byte[] delete = Bytes.toBytes("delete");

  private HTableDescriptor table;

  @Before
  public void setUp() throws Exception {
    baseConfiguration = HBaseConfiguration.create();
    // smaller block size and capacity to trigger more operations
    // and test them
    baseConfiguration.setInt("hbase.regionserver.hlog.blocksize", 1024 * 20);
    baseConfiguration.setInt("replication.source.size.capacity", 1024);
    baseConfiguration.setLong("replication.source.sleepforretries", 100);
    baseConfiguration.setInt("hbase.regionserver.maxlogs", 10);
    baseConfiguration.setLong("hbase.master.logcleaner.ttl", 10);
    baseConfiguration.setBoolean(HConstants.REPLICATION_ENABLE_KEY,
        HConstants.REPLICATION_ENABLE_DEFAULT);
    baseConfiguration.setBoolean("dfs.support.append", true);
    baseConfiguration.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    baseConfiguration.setStrings(
        CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        CoprocessorCounter.class.getName());

    table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    fam = new HColumnDescriptor(noRepfamName);
    table.addFamily(fam);
  }

  /**
   * It tests the replication scenario involving 0 -> 1 -> 0. It does it by
   * adding and deleting a row to a table in each cluster, checking if it's
   * replicated. It also tests that the puts and deletes are not replicated back
   * to the originating cluster.
   */
  @Test(timeout = 300000)
  public void testCyclicReplication1() throws Exception {
    LOG.info("testSimplePutDelete");
    int numClusters = 2;
    Table[] htables = null;
    try {
      startMiniClusters(numClusters);
      createTableOnClusters(table);

      htables = getHTablesOnClusters(tableName);

      // Test the replication scenarios of 0 -> 1 -> 0
      addPeer("1", 0, 1);
      addPeer("1", 1, 0);

      int[] expectedCounts = new int[] { 2, 2 };

      // add rows to both clusters,
      // make sure they are both replication
      putAndWait(row, famName, htables[0], htables[1]);
      putAndWait(row1, famName, htables[1], htables[0]);
      validateCounts(htables, put, expectedCounts);

      deleteAndWait(row, htables[0], htables[1]);
      deleteAndWait(row1, htables[1], htables[0]);
      validateCounts(htables, delete, expectedCounts);
    } finally {
      close(htables);
      shutDownMiniClusters();
    }
  }

  /**
   * Tests the cyclic replication scenario of 0 -> 1 -> 2 -> 0 by adding and
   * deleting rows to a table in each clusters and ensuring that the each of
   * these clusters get the appropriate mutations. It also tests the grouping
   * scenario where a cluster needs to replicate the edits originating from
   * itself and also the edits that it received using replication from a
   * different cluster. The scenario is explained in HBASE-9158
   */
  @Test(timeout = 300000)
  public void testCyclicReplication2() throws Exception {
    LOG.info("testCyclicReplication1");
    int numClusters = 3;
    Table[] htables = null;
    try {
      startMiniClusters(numClusters);
      createTableOnClusters(table);

      // Test the replication scenario of 0 -> 1 -> 2 -> 0
      addPeer("1", 0, 1);
      addPeer("1", 1, 2);
      addPeer("1", 2, 0);

      htables = getHTablesOnClusters(tableName);

      // put "row" and wait 'til it got around
      putAndWait(row, famName, htables[0], htables[2]);
      putAndWait(row1, famName, htables[1], htables[0]);
      putAndWait(row2, famName, htables[2], htables[1]);

      deleteAndWait(row, htables[0], htables[2]);
      deleteAndWait(row1, htables[1], htables[0]);
      deleteAndWait(row2, htables[2], htables[1]);

      int[] expectedCounts = new int[] { 3, 3, 3 };
      validateCounts(htables, put, expectedCounts);
      validateCounts(htables, delete, expectedCounts);

      // Test HBASE-9158
      disablePeer("1", 2);
      // we now have an edit that was replicated into cluster originating from
      // cluster 0
      putAndWait(row3, famName, htables[0], htables[1]);
      // now add a local edit to cluster 1
      htables[1].put(new Put(row4).addColumn(famName, row4, row4));
      // re-enable replication from cluster 2 to cluster 0
      enablePeer("1", 2);
      // without HBASE-9158 the edit for row4 would have been marked with
      // cluster 0's id
      // and hence not replicated to cluster 0
      wait(row4, htables[0], true);
    } finally {
      close(htables);
      shutDownMiniClusters();
    }
  }

  /**
   * Tests cyclic replication scenario of 0 -> 1 -> 2 -> 1.
   */
  @Test(timeout = 300000)
  public void testCyclicReplication3() throws Exception {
    LOG.info("testCyclicReplication2");
    int numClusters = 3;
    Table[] htables = null;
    try {
      startMiniClusters(numClusters);
      createTableOnClusters(table);

      // Test the replication scenario of 0 -> 1 -> 2 -> 1
      addPeer("1", 0, 1);
      addPeer("1", 1, 2);
      addPeer("1", 2, 1);

      htables = getHTablesOnClusters(tableName);

      // put "row" and wait 'til it got around
      putAndWait(row, famName, htables[0], htables[2]);
      putAndWait(row1, famName, htables[1], htables[2]);
      putAndWait(row2, famName, htables[2], htables[1]);

      deleteAndWait(row, htables[0], htables[2]);
      deleteAndWait(row1, htables[1], htables[2]);
      deleteAndWait(row2, htables[2], htables[1]);

      int[] expectedCounts = new int[] { 1, 3, 3 };
      validateCounts(htables, put, expectedCounts);
      validateCounts(htables, delete, expectedCounts);
    } finally {
      close(htables);
      shutDownMiniClusters();
    }
  }

  @After
  public void tearDown() throws IOException {
    configurations = null;
    utilities = null;
  }

  @SuppressWarnings("resource")
  private void startMiniClusters(int numClusters) throws Exception {
    Random random = new Random();
    utilities = new HBaseTestingUtility[numClusters];
    configurations = new Configuration[numClusters];
    for (int i = 0; i < numClusters; i++) {
      Configuration conf = new Configuration(baseConfiguration);
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/" + i + random.nextInt());
      HBaseTestingUtility utility = new HBaseTestingUtility(conf);
      if (i == 0) {
        utility.startMiniZKCluster();
        miniZK = utility.getZkCluster();
      } else {
        utility.setZkCluster(miniZK);
      }
      utility.startMiniCluster();
      utilities[i] = utility;
      configurations[i] = conf;
      new ZooKeeperWatcher(conf, "cluster" + i, null, true);
    }
  }

  private void shutDownMiniClusters() throws Exception {
    int numClusters = utilities.length;
    for (int i = numClusters - 1; i >= 0; i--) {
      if (utilities[i] != null) {
        utilities[i].shutdownMiniCluster();
      }
    }
    miniZK.shutdown();
  }

  private void createTableOnClusters(HTableDescriptor table) throws Exception {
    for (HBaseTestingUtility utility : utilities) {
      utility.getHBaseAdmin().createTable(table);
    }
  }

  private void addPeer(String id, int masterClusterNumber,
      int slaveClusterNumber) throws Exception {
    ReplicationAdmin replicationAdmin = null;
    try {
      replicationAdmin = new ReplicationAdmin(
          configurations[masterClusterNumber]);
      replicationAdmin.addPeer(id,
          utilities[slaveClusterNumber].getClusterKey());
    } finally {
      close(replicationAdmin);
    }
  }

  private void disablePeer(String id, int masterClusterNumber) throws Exception {
    ReplicationAdmin replicationAdmin = null;
    try {
      replicationAdmin = new ReplicationAdmin(
          configurations[masterClusterNumber]);
      replicationAdmin.disablePeer(id);
    } finally {
      close(replicationAdmin);
    }
  }

  private void enablePeer(String id, int masterClusterNumber) throws Exception {
    ReplicationAdmin replicationAdmin = null;
    try {
      replicationAdmin = new ReplicationAdmin(
          configurations[masterClusterNumber]);
      replicationAdmin.enablePeer(id);
    } finally {
      close(replicationAdmin);
    }
  }

  private void close(Closeable... closeables) {
    try {
      if (closeables != null) {
        for (Closeable closeable : closeables) {
          closeable.close();
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception occured while closing the object:", e);
    }
  }

  @SuppressWarnings("resource")
  private Table[] getHTablesOnClusters(TableName tableName) throws Exception {
    int numClusters = utilities.length;
    Table[] htables = new Table[numClusters];
    for (int i = 0; i < numClusters; i++) {
      Table htable = ConnectionFactory.createConnection(configurations[i]).getTable(tableName);
      htable.setWriteBufferSize(1024);
      htables[i] = htable;
    }
    return htables;
  }

  private void validateCounts(Table[] htables, byte[] type,
      int[] expectedCounts) throws IOException {
    for (int i = 0; i < htables.length; i++) {
      assertEquals(Bytes.toString(type) + " were replicated back ",
          expectedCounts[i], getCount(htables[i], type));
    }
  }

  private int getCount(Table t, byte[] type) throws IOException {
    Get test = new Get(row);
    test.setAttribute("count", new byte[] {});
    Result res = t.get(test);
    return Bytes.toInt(res.getValue(count, type));
  }

  private void deleteAndWait(byte[] row, Table source, Table target)
      throws Exception {
    Delete del = new Delete(row);
    source.delete(del);
    wait(row, target, true);
  }

  private void putAndWait(byte[] row, byte[] fam, Table source, Table target)
      throws Exception {
    Put put = new Put(row);
    put.addColumn(fam, row, row);
    source.put(put);
    wait(row, target, false);
  }

  private void wait(byte[] row, Table target, boolean isDeleted)
      throws Exception {
    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for replication. Row:" + Bytes.toString(row)
            + ". IsDeleteReplication:" + isDeleted);
      }
      Result res = target.get(get);
      boolean sleep = isDeleted ? res.size() > 0 : res.size() == 0;
      if (sleep) {
        LOG.info("Waiting for more time for replication. Row:"
            + Bytes.toString(row) + ". IsDeleteReplication:" + isDeleted);
        Thread.sleep(SLEEP_TIME);
      } else {
        if (!isDeleted) {
          assertArrayEquals(res.value(), row);
        }
        LOG.info("Obtained row:"
            + Bytes.toString(row) + ". IsDeleteReplication:" + isDeleted);
        break;
      }
    }
  }

  /**
   * Use a coprocessor to count puts and deletes. as KVs would be replicated back with the same
   * timestamp there is otherwise no way to count them.
   */
  public static class CoprocessorCounter extends BaseRegionObserver {
    private int nCount = 0;
    private int nDelete = 0;

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      nCount++;
    }

    @Override
    public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
      nDelete++;
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Get get, final List<Cell> result) throws IOException {
      if (get.getAttribute("count") != null) {
        result.clear();
        // order is important!
        result.add(new KeyValue(count, count, delete, Bytes.toBytes(nDelete)));
        result.add(new KeyValue(count, count, put, Bytes.toBytes(nCount)));
        c.bypass();
      }
    }
  }

}
