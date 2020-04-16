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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.replication.regionserver.TestRegionReplicaReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests failover of secondary region replicas.
 */
@Category(LargeTests.class)
public class TestRegionReplicaFailover {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicaFailover.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegionReplicaReplicationEndpoint.class);

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  private static final int NB_SERVERS = 3;

  protected final byte[][] families = new byte[][] {HBaseTestingUtility.fam1,
      HBaseTestingUtility.fam2, HBaseTestingUtility.fam3};
  protected final byte[] fam = HBaseTestingUtility.fam1;
  protected final byte[] qual1 = Bytes.toBytes("qual1");
  protected final byte[] value1 = Bytes.toBytes("value1");
  protected final byte[] row = Bytes.toBytes("rowA");
  protected final byte[] row2 = Bytes.toBytes("rowB");

  @Rule public TestName name = new TestName();

  private HTableDescriptor htd;

  @Before
  public void before() throws Exception {
    Configuration conf = HTU.getConfiguration();
   // Up the handlers; this test needs more than usual.
    conf.setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY, true);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);

    HTU.startMiniCluster(NB_SERVERS);
    htd = HTU.createTableDescriptor(
      TableName.valueOf(name.getMethodName().substring(0, name.getMethodName().length()-3)),
      HColumnDescriptor.DEFAULT_MIN_VERSIONS, 3, HConstants.FOREVER,
      HColumnDescriptor.DEFAULT_KEEP_DELETED);
    htd.setRegionReplication(3);
    HTU.getAdmin().createTable(htd);
  }

  @After
  public void after() throws Exception {
    HTU.deleteTableIfAny(htd.getTableName());
    HTU.shutdownMiniCluster();
  }

  /**
   * Tests the case where a newly created table with region replicas and no data, the secondary
   * region replicas are available to read immediately.
   */
  @Test
  public void testSecondaryRegionWithEmptyRegion() throws IOException {
    // Create a new table with region replication, don't put any data. Test that the secondary
    // region replica is available to read.
    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
        Table table = connection.getTable(htd.getTableName())) {

      Get get = new Get(row);
      get.setConsistency(Consistency.TIMELINE);
      get.setReplicaId(1);
      table.get(get); // this should not block
    }
  }

  /**
   * Tests the case where if there is some data in the primary region, reopening the region replicas
   * (enable/disable table, etc) makes the region replicas readable.
   * @throws IOException
   */
  @Test
  public void testSecondaryRegionWithNonEmptyRegion() throws IOException {
    // Create a new table with region replication and load some data
    // than disable and enable the table again and verify the data from secondary
    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
        Table table = connection.getTable(htd.getTableName())) {

      HTU.loadNumericRows(table, fam, 0, 1000);

      HTU.getAdmin().disableTable(htd.getTableName());
      HTU.getAdmin().enableTable(htd.getTableName());

      HTU.verifyNumericRows(table, fam, 0, 1000, 1);
    }
  }

  /**
   * Tests the case where killing a primary region with unflushed data recovers
   */
  @Test
  public void testPrimaryRegionKill() throws Exception {
    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
        Table table = connection.getTable(htd.getTableName())) {

      HTU.loadNumericRows(table, fam, 0, 1000);

      // wal replication is async, we have to wait until the replication catches up, or we timeout
      verifyNumericRowsWithTimeout(table, fam, 0, 1000, 1, 30000);
      verifyNumericRowsWithTimeout(table, fam, 0, 1000, 2, 30000);

      // we should not have flushed files now, but data in memstores of primary and secondary
      // kill the primary region replica now, and ensure that when it comes back up, we can still
      // read from it the same data from primary and secondaries
      boolean aborted = false;
      for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
        for (Region r : rs.getRegionServer().getRegions(htd.getTableName())) {
          if (r.getRegionInfo().getReplicaId() == 0) {
            LOG.info("Aborting region server hosting primary region replica");
            rs.getRegionServer().abort("for test");
            aborted = true;
            break;
          }
        }
      }
      assertTrue(aborted);

      // wal replication is async, we have to wait until the replication catches up, or we timeout
      verifyNumericRowsWithTimeout(table, fam, 0, 1000, 0, 30000);
      verifyNumericRowsWithTimeout(table, fam, 0, 1000, 1, 30000);
      verifyNumericRowsWithTimeout(table, fam, 0, 1000, 2, 30000);
    }

    // restart the region server
    HTU.getMiniHBaseCluster().startRegionServer();
  }

  /** wal replication is async, we have to wait until the replication catches up, or we timeout
   */
  private void verifyNumericRowsWithTimeout(final Table table, final byte[] f, final int startRow,
      final int endRow, final int replicaId, final long timeout) throws Exception {
    try {
      HTU.waitFor(timeout, new Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          try {
            HTU.verifyNumericRows(table, f, startRow, endRow, replicaId);
            return true;
          } catch (AssertionError ae) {
            return false;
          }
        }
      });
    } catch (Throwable t) {
      // ignore this, but redo the verify do get the actual exception
      HTU.verifyNumericRows(table, f, startRow, endRow, replicaId);
    }
  }

  /**
   * Tests the case where killing a secondary region with unflushed data recovers, and the replica
   * becomes available to read again shortly.
   */
  @Test
  public void testSecondaryRegionKill() throws Exception {
    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
        Table table = connection.getTable(htd.getTableName())) {
      HTU.loadNumericRows(table, fam, 0, 1000);

      // wait for some time to ensure that async wal replication does it's magic
      verifyNumericRowsWithTimeout(table, fam, 0, 1000, 1, 30000);
      verifyNumericRowsWithTimeout(table, fam, 0, 1000, 2, 30000);

      // we should not have flushed files now, but data in memstores of primary and secondary
      // kill the secondary region replica now, and ensure that when it comes back up, we can still
      // read from it the same data
      boolean aborted = false;
      for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
        for (Region r : rs.getRegionServer().getRegions(htd.getTableName())) {
          if (r.getRegionInfo().getReplicaId() == 1) {
            LOG.info("Aborting region server hosting secondary region replica");
            rs.getRegionServer().abort("for test");
            aborted = true;
            break;
          }
        }
      }
      assertTrue(aborted);

      // It takes extra time for replica region is ready for read as during
      // region open process, it needs to ask primary region to do a flush and replica region
      // can open newly flushed hfiles to avoid data out-of-sync.
      verifyNumericRowsWithTimeout(table, fam, 0, 1000, 1, 30000);
      HTU.verifyNumericRows(table, fam, 0, 1000, 2);
    }

    // restart the region server
    HTU.getMiniHBaseCluster().startRegionServer();
  }

  /**
   * Tests the case where there are 3 region replicas and the primary is continuously accepting
   * new writes while one of the secondaries is killed. Verification is done for both of the
   * secondary replicas.
   */
  @Test
  public void testSecondaryRegionKillWhilePrimaryIsAcceptingWrites() throws Exception {
    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
        Table table = connection.getTable(htd.getTableName());
        Admin admin = connection.getAdmin()) {
      // start a thread to do the loading of primary
      HTU.loadNumericRows(table, fam, 0, 1000); // start with some base
      admin.flush(table.getName());
      HTU.loadNumericRows(table, fam, 1000, 2000);

      final AtomicReference<Throwable> ex = new AtomicReference<>(null);
      final AtomicBoolean done = new AtomicBoolean(false);
      final AtomicInteger key = new AtomicInteger(2000);

      Thread loader = new Thread() {
        @Override
        public void run() {
          while (!done.get()) {
            try {
              HTU.loadNumericRows(table, fam, key.get(), key.get()+1000);
              key.addAndGet(1000);
            } catch (Throwable e) {
              ex.compareAndSet(null, e);
            }
          }
        }
      };
      loader.start();

      Thread aborter = new Thread() {
        @Override
        public void run() {
          try {
            boolean aborted = false;
            for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
              for (Region r : rs.getRegionServer().getRegions(htd.getTableName())) {
                if (r.getRegionInfo().getReplicaId() == 1) {
                  LOG.info("Aborting region server hosting secondary region replica");
                  rs.getRegionServer().abort("for test");
                  aborted = true;
                }
              }
            }
            assertTrue(aborted);
          } catch (Throwable e) {
            ex.compareAndSet(null, e);
          }
        }
      };

      aborter.start();
      aborter.join();
      done.set(true);
      loader.join();

      assertNull(ex.get());

      assertTrue(key.get() > 1000); // assert that the test is working as designed
      LOG.info("Loaded up to key :" + key.get());
      verifyNumericRowsWithTimeout(table, fam, 0, key.get(), 0, 30000);
      verifyNumericRowsWithTimeout(table, fam, 0, key.get(), 1, 30000);
      verifyNumericRowsWithTimeout(table, fam, 0, key.get(), 2, 30000);
    }

    // restart the region server
    HTU.getMiniHBaseCluster().startRegionServer();
  }

  /**
   * Tests the case where we are creating a table with a lot of regions and replicas. Opening region
   * replicas should not block handlers on RS indefinitely.
   */
  @Test
  public void testLotsOfRegionReplicas() throws IOException {
    int numRegions = NB_SERVERS * 20;
    int regionReplication = 10;
    String tableName = htd.getTableName().getNameAsString() + "2";
    htd = HTU.createTableDescriptor(TableName.valueOf(tableName),
      HColumnDescriptor.DEFAULT_MIN_VERSIONS, 3, HConstants.FOREVER,
      HColumnDescriptor.DEFAULT_KEEP_DELETED);
    htd.setRegionReplication(regionReplication);

    // dont care about splits themselves too much
    byte[] startKey = Bytes.toBytes("aaa");
    byte[] endKey = Bytes.toBytes("zzz");
    byte[][] splits = HTU.getRegionSplitStartKeys(startKey, endKey, numRegions);
    HTU.getAdmin().createTable(htd, startKey, endKey, numRegions);

    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
        Table table = connection.getTable(htd.getTableName())) {

      for (int i = 1; i < splits.length; i++) {
        for (int j = 0; j < regionReplication; j++) {
          Get get = new Get(splits[i]);
          get.setConsistency(Consistency.TIMELINE);
          get.setReplicaId(j);
          table.get(get); // this should not block. Regions should be coming online
        }
      }
    }

    HTU.deleteTableIfAny(TableName.valueOf(tableName));
  }
}
