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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestReplication {

  private static final Log LOG = LogFactory.getLog(TestReplication.class);

  private static Configuration conf1;
  private static Configuration conf2;
  private static Configuration CONF_WITH_LOCALFS;

  private static ZooKeeperWatcher zkw1;
  private static ZooKeeperWatcher zkw2;

  private static ReplicationAdmin admin;

  private static HTable htable1;
  private static HTable htable2;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static final int NB_ROWS_IN_BATCH = 100;
  private static final int NB_ROWS_IN_BIG_BATCH =
      NB_ROWS_IN_BATCH * 10;
  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 10;

  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");

  /**
   * @throws java.lang.Exception
   */
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
    conf1.setInt("zookeeper.recovery.retry", 1);
    conf1.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    // Have to reget conf1 in case zk cluster location different
    // than default
    conf1 = utility1.getConfiguration();  
    zkw1 = new ZooKeeperWatcher(conf1, "cluster1", null, true);
    admin = new ReplicationAdmin(conf1);
    LOG.info("Setup first Zk");

    // Base conf2 on conf1 so it gets the right zk cluster.
    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf2.setInt("hbase.client.retries.number", 6);
    conf2.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf2.setBoolean("dfs.support.append", true);

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    zkw2 = new ZooKeeperWatcher(conf2, "cluster2", null, true);

    admin.addPeer("2", utility2.getClusterKey());
    setIsReplication(true);

    LOG.info("Setup second Zk");
    CONF_WITH_LOCALFS = HBaseConfiguration.create(conf1);
    utility1.startMiniCluster(3);
    utility2.startMiniCluster(3);

    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    fam = new HColumnDescriptor(noRepfamName);
    table.addFamily(fam);
    HBaseAdmin admin1 = new HBaseAdmin(conf1);
    HBaseAdmin admin2 = new HBaseAdmin(conf2);
    admin1.createTable(table);
    admin2.createTable(table);
    htable1 = new HTable(conf1, tableName);
    htable1.setWriteBufferSize(1024);
    htable2 = new HTable(conf2, tableName);
  }

  private static void setIsReplication(boolean rep) throws Exception {
    LOG.info("Set rep " + rep);
    admin.setReplicating(rep);
    Thread.sleep(SLEEP_TIME);
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {

    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for ( JVMClusterUtil.RegionServerThread r :
        utility1.getHBaseCluster().getRegionServerThreads()) {
      r.getRegionServer().getWAL().rollWriter();
    }
    utility1.truncateTable(tableName);
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call truncateTable on
    // utility2 since late writes could make it to the slave in some way.
    // Instead, we truncate the first table and wait for all the Deletes to
    // make it to the slave.
    Scan scan = new Scan();
    int lastCount = 0;
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for truncate");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(NB_ROWS_IN_BIG_BATCH);
      scanner.close();
      if (res.length != 0) {
       if (res.length < lastCount) {
          i--; // Don't increment timeout if we make progress
        }
        lastCount = res.length;
        LOG.info("Still got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  /**
   * Verify that version and column delete marker types are replicated
   * correctly.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testDeleteTypes() throws Exception {
    LOG.info("testDeleteTypes");
    final byte[] v1 = Bytes.toBytes("v1");
    final byte[] v2 = Bytes.toBytes("v2");
    final byte[] v3 = Bytes.toBytes("v3");
    htable1 = new HTable(conf1, tableName);

    long t = EnvironmentEdgeManager.currentTimeMillis();
    // create three versions for "row"
    Put put = new Put(row);
    put.add(famName, row, t, v1);
    htable1.put(put);

    put = new Put(row);
    put.add(famName, row, t+1, v2);
    htable1.put(put);
    
    put = new Put(row);
    put.add(famName, row, t+2, v3);
    htable1.put(put);
    
    Get get = new Get(row);
    get.setMaxVersions();
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() < 3) {
        LOG.info("Rows not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.raw()[0].getValue(), v3);
        assertArrayEquals(res.raw()[1].getValue(), v2);
        assertArrayEquals(res.raw()[2].getValue(), v1);
        break;
      }
    }
    // place a version delete marker (delete last version)
    Delete d = new Delete(row);
    d.deleteColumn(famName, row, t);
    htable1.delete(d);

    get = new Get(row);
    get.setMaxVersions();
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() > 2) {
        LOG.info("Version not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.raw()[0].getValue(), v3);
        assertArrayEquals(res.raw()[1].getValue(), v2);
        break;
      }
    }

    // place a column delete marker
    d = new Delete(row);
    d.deleteColumns(famName, row, t+2);
    htable1.delete(d);

    // now *both* of the remaining version should be deleted
    // at the replica
    get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        LOG.info("Rows not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  /**
   * Add a row, check it's replicated, delete it, check's gone
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testSimplePutDelete() throws Exception {
    LOG.info("testSimplePutDelete");
    Put put = new Put(row);
    put.add(famName, row, row);

    htable1 = new HTable(conf1, tableName);
    htable1.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }

    Delete del = new Delete(row);
    htable1.delete(del);

    get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        LOG.info("Row not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  /**
   * Try a small batch upload using the write buffer, check it's replicated
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testSmallBatch() throws Exception {
    LOG.info("testSmallBatch");
    Put put;
    // normal Batch tests
    htable1.setAutoFlush(false);
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      put = new Put(Bytes.toBytes(i));
      put.add(famName, row, row);
      htable1.put(put);
    }
    htable1.flushCommits();

    Scan scan = new Scan();

    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);

    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for normal batch replication");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(NB_ROWS_IN_BATCH);
      scanner.close();
      if (res.length != NB_ROWS_IN_BATCH) {
        LOG.info("Only got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }

    htable1.setAutoFlush(true);

  }

  /**
   * Test stopping replication, trying to insert, make sure nothing's
   * replicated, enable it, try replicating and it should work
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testStartStop() throws Exception {

    // Test stopping replication
    setIsReplication(false);

    Put put = new Put(Bytes.toBytes("stop start"));
    put.add(famName, row, row);
    htable1.put(put);

    Get get = new Get(Bytes.toBytes("stop start"));
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        break;
      }
      Result res = htable2.get(get);
      if(res.size() >= 1) {
        fail("Replication wasn't stopped");

      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    // Test restart replication
    setIsReplication(true);

    htable1.put(put);

    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if(res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }

    put = new Put(Bytes.toBytes("do not rep"));
    put.add(noRepfamName, row, row);
    htable1.put(put);

    get = new Get(Bytes.toBytes("do not rep"));
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES-1) {
        break;
      }
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        fail("Not supposed to be replicated");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

  }

  /**
   * Test disable/enable replication, trying to insert, make sure nothing's
   * replicated, enable it, the insert should be replicated
   *
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testDisableEnable() throws Exception {

    // Test disabling replication
    admin.disablePeer("2");

    byte[] rowkey = Bytes.toBytes("disable enable");
    Put put = new Put(rowkey);
    put.add(famName, row, row);
    htable1.put(put);

    Get get = new Get(rowkey);
    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        fail("Replication wasn't disabled");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    // Test enable replication
    admin.enablePeer("2");

    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = htable2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        return;
      }
    }
    fail("Waited too much time for put replication");
  }

  /**
   * Test disabling an inactive peer. Add a peer which is inactive, trying to
   * insert, disable the peer, then activate the peer and make sure nothing is
   * replicated. In Addition, enable the peer and check the updates are
   * replicated.
   *
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void testDisableInactivePeer() throws Exception {

    // enabling and shutdown the peer
    admin.enablePeer("2");
    utility2.shutdownMiniHBaseCluster();

    byte[] rowkey = Bytes.toBytes("disable inactive peer");
    Put put = new Put(rowkey);
    put.add(famName, row, row);
    htable1.put(put);

    // wait for the sleep interval of the master cluster to become long
    Thread.sleep(SLEEP_TIME * NB_RETRIES);

    // disable and start the peer
    admin.disablePeer("2");
    utility2.startMiniHBaseCluster(1, 1);
    Get get = new Get(rowkey);
    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        fail("Replication wasn't disabled");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    // Test enable replication
    admin.enablePeer("2");
    // wait since the sleep interval would be long
    Thread.sleep(SLEEP_TIME * NB_RETRIES);
    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = htable2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME * NB_RETRIES);
      } else {
        assertArrayEquals(res.value(), row);
        return;
      }
    }
    fail("Waited too much time for put replication");
  }

  /**
   * Integration test for TestReplicationAdmin, removes and re-add a peer
   * cluster
   *
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testAddAndRemoveClusters() throws Exception {
    LOG.info("testAddAndRemoveClusters");
    admin.removePeer("2");
    Thread.sleep(SLEEP_TIME);
    byte[] rowKey = Bytes.toBytes("Won't be replicated");
    Put put = new Put(rowKey);
    put.add(famName, row, row);
    htable1.put(put);

    Get get = new Get(rowKey);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES-1) {
        break;
      }
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        fail("Not supposed to be replicated");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    admin.addPeer("2", utility2.getClusterKey());
    Thread.sleep(SLEEP_TIME);
    rowKey = Bytes.toBytes("do rep");
    put = new Put(rowKey);
    put.add(famName, row, row);
    LOG.info("Adding new row");
    htable1.put(put);

    get = new Get(rowKey);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME*i);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }
  }

  /**
   * Do a more intense version testSmallBatch, one  that will trigger
   * hlog rolling and other non-trivial code paths
   * @throws Exception
   */
  @Test(timeout=300000)
  public void loadTesting() throws Exception {
    htable1.setWriteBufferSize(1024);
    htable1.setAutoFlush(false);
    for (int i = 0; i < NB_ROWS_IN_BIG_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(famName, row, row);
      htable1.put(put);
    }
    htable1.flushCommits();

    Scan scan = new Scan();

    ResultScanner scanner = htable1.getScanner(scan);
    Result[] res = scanner.next(NB_ROWS_IN_BIG_BATCH);
    scanner.close();

    assertEquals(NB_ROWS_IN_BATCH *10, res.length);

    scan = new Scan();

    for (int i = 0; i < NB_RETRIES; i++) {

      scanner = htable2.getScanner(scan);
      res = scanner.next(NB_ROWS_IN_BIG_BATCH);
      scanner.close();
      if (res.length != NB_ROWS_IN_BIG_BATCH) {
        if (i == NB_RETRIES-1) {
          int lastRow = -1;
          for (Result result : res) {
            int currentRow = Bytes.toInt(result.getRow());
            for (int row = lastRow+1; row < currentRow; row++) {
              LOG.error("Row missing: " + row);
            }
            lastRow = currentRow;
          }
          LOG.error("Last row: " + lastRow);
          fail("Waited too much time for normal batch replication, "
              + res.length + " instead of " + NB_ROWS_IN_BIG_BATCH);
        } else {
          LOG.info("Only got " + res.length + " rows");
          Thread.sleep(SLEEP_TIME);
        }
      } else {
        break;
      }
    }
  }

  /**
   * Do a small loading into a table, make sure the data is really the same,
   * then run the VerifyReplication job to check the results. Do a second
   * comparison where all the cells are different.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testVerifyRepJob() throws Exception {
    // Populate the tables, at the same time it guarantees that the tables are
    // identical since it does the check
    testSmallBatch();

    String[] args = new String[] {"2", Bytes.toString(tableName)};
    Job job = VerifyReplication.createSubmittableJob(CONF_WITH_LOCALFS, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());

    Scan scan = new Scan();
    ResultScanner rs = htable2.getScanner(scan);
    Put put = null;
    for (Result result : rs) {
      put = new Put(result.getRow());
      KeyValue firstVal = result.raw()[0];
      put.add(firstVal.getFamily(),
          firstVal.getQualifier(), Bytes.toBytes("diff data"));
      htable2.put(put);
    }
    Delete delete = new Delete(put.getRow());
    htable2.delete(delete);
    job = VerifyReplication.createSubmittableJob(CONF_WITH_LOCALFS, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(0, job.getCounters().
            findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
        assertEquals(NB_ROWS_IN_BATCH, job.getCounters().
            findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
  }

  /**
   * Load up multiple tables over 2 region servers and kill a source during
   * the upload. The failover happens internally.
   *
   * WARNING this test sometimes fails because of HBASE-3515
   *
   * @throws Exception
   */
  @Test(timeout=300000)
  public void queueFailover() throws Exception {
    utility1.createMultiRegions(htable1, famName);

    // killing the RS with .META. can result into failed puts until we solve
    // IO fencing
    int rsToKill1 =
        utility1.getHBaseCluster().getServerWithMeta() == 0 ? 1 : 0;
    int rsToKill2 =
        utility2.getHBaseCluster().getServerWithMeta() == 0 ? 1 : 0;

    // Takes about 20 secs to run the full loading, kill around the middle
    Thread killer1 = killARegionServer(utility1, 7500, rsToKill1);
    Thread killer2 = killARegionServer(utility2, 10000, rsToKill2);

    LOG.info("Start loading table");
    int initialCount = utility1.loadTable(htable1, famName);
    LOG.info("Done loading table");
    killer1.join(5000);
    killer2.join(5000);
    LOG.info("Done waiting for threads");

    Result[] res;
    while (true) {
      try {
        Scan scan = new Scan();
        ResultScanner scanner = htable1.getScanner(scan);
        res = scanner.next(initialCount);
        scanner.close();
        break;
      } catch (UnknownScannerException ex) {
        LOG.info("Cluster wasn't ready yet, restarting scanner");
      }
    }
    // Test we actually have all the rows, we may miss some because we
    // don't have IO fencing.
    if (res.length != initialCount) {
      LOG.warn("We lost some rows on the master cluster!");
      // We don't really expect the other cluster to have more rows
      initialCount = res.length;
    }

    int lastCount = 0;

    final long start = System.currentTimeMillis();
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for queueFailover replication. " +
          "Waited "+(System.currentTimeMillis() - start)+"ms.");
      }
      Scan scan2 = new Scan();
      ResultScanner scanner2 = htable2.getScanner(scan2);
      Result[] res2 = scanner2.next(initialCount * 2);
      scanner2.close();
      if (res2.length < initialCount) {
        if (lastCount < res2.length) {
          i--; // Don't increment timeout if we make progress
        }
        lastCount = res2.length;
        LOG.info("Only got " + lastCount + " rows instead of " +
            initialCount + " current i=" + i);
        Thread.sleep(SLEEP_TIME*2);
      } else {
        break;
      }
    }
  }

  private static Thread killARegionServer(final HBaseTestingUtility utility,
                                   final long timeout, final int rs) {
    Thread killer = new Thread() {
      public void run() {
        try {
          Thread.sleep(timeout);
          utility.expireRegionServerSession(rs);
        } catch (Exception e) {
          LOG.error(e);
        }
      }
    };
    killer.setDaemon(true);
    killer.start();
    return killer;
  }

}

