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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * This class is only a base for other integration-level replication tests.
 * Do not add tests here.
 * TestReplicationSmallTests is where tests that don't require bring machines up/down should go
 * All other tests should have their own classes and extend this one
 */
public class TestReplicationBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationBase.class);
  private static Connection connection1;
  private static Connection connection2;
  protected static Configuration CONF_WITH_LOCALFS;

  protected static ReplicationAdmin admin;
  protected static Admin hbaseAdmin;

  protected static Table htable1;
  protected static Table htable2;

  protected static final HBaseTestingUtility UTIL1 = new HBaseTestingUtility();
  protected static final HBaseTestingUtility UTIL2 = new HBaseTestingUtility();
  protected static Configuration CONF1 = UTIL1.getConfiguration();
  protected static Configuration CONF2 = UTIL2.getConfiguration();

  protected static int NUM_SLAVES1 = 1;
  protected static int NUM_SLAVES2 = 1;
  protected static final int NB_ROWS_IN_BATCH = 100;
  protected static final int NB_ROWS_IN_BIG_BATCH =
      NB_ROWS_IN_BATCH * 10;
  protected static final long SLEEP_TIME = 500;
  protected static final int NB_RETRIES = 50;
  protected static AtomicInteger replicateCount = new AtomicInteger();
  protected static volatile List<WAL.Entry> replicatedEntries = Lists.newArrayList();

  protected static final TableName tableName = TableName.valueOf("test");
  protected static final byte[] famName = Bytes.toBytes("f");
  protected static final byte[] row = Bytes.toBytes("row");
  protected static final byte[] noRepfamName = Bytes.toBytes("norep");
  protected static final String PEER_ID2 = "2";

  protected boolean isSerialPeer() {
    return false;
  }

  protected final void cleanUp() throws IOException, InterruptedException {
    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for (JVMClusterUtil.RegionServerThread r : UTIL1.getHBaseCluster()
        .getRegionServerThreads()) {
      UTIL1.getAdmin().rollWALWriter(r.getRegionServer().getServerName());
    }
    int rowCount = UTIL1.countRows(tableName);
    UTIL1.deleteTableData(tableName);
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call deleteTableData on
    // utility2 since late writes could make it to the slave in some way.
    // Instead, we truncate the first table and wait for all the Deletes to
    // make it to the slave.
    Scan scan = new Scan();
    int lastCount = 0;
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for truncate");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(rowCount);
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

  protected static void waitForReplication(int expectedRows, int retries)
      throws IOException, InterruptedException {
    waitForReplication(htable2, expectedRows, retries);
  }

  protected static void waitForReplication(Table htable2, int expectedRows, int retries)
      throws IOException, InterruptedException {
    Scan scan;
    for (int i = 0; i < retries; i++) {
      scan = new Scan();
      if (i== retries -1) {
        fail("Waited too much time for normal batch replication");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(expectedRows);
      scanner.close();
      if (res.length != expectedRows) {
        LOG.info("Only got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  protected static void loadData(String prefix, byte[] row) throws IOException {
    loadData(prefix, row, famName);
  }

  protected static void loadData(String prefix, byte[] row, byte[] familyName) throws IOException {
    List<Put> puts = new ArrayList<>(NB_ROWS_IN_BATCH);
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(prefix + Integer.toString(i)));
      put.addColumn(familyName, row, row);
      puts.add(put);
    }
    htable1.put(puts);
  }

  protected static void setupConfig(HBaseTestingUtility util, String znodeParent) {
    Configuration conf = util.getConfiguration();
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodeParent);
    // We don't want too many edits per batch sent to the ReplicationEndpoint to trigger
    // sufficient number of events. But we don't want to go too low because
    // HBaseInterClusterReplicationEndpoint partitions entries into batches and we want
    // more than one batch sent to the peer cluster for better testing.
    conf.setInt("replication.source.size.capacity", 102400);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setLong("replication.sleep.before.failover", 2000);
    conf.setInt("replication.source.maxretriesmultiplier", 10);
    conf.setFloat("replication.source.ratio", 1.0f);
    conf.setBoolean("replication.source.eof.autorecovery", true);
    conf.setLong("hbase.serial.replication.waiting.ms", 100);
  }

  static void configureClusters(HBaseTestingUtility util1,
      HBaseTestingUtility util2) {
    setupConfig(util1, "/1");
    setupConfig(util2, "/2");

    Configuration conf2 = util2.getConfiguration();
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf2.setBoolean("hbase.tests.use.shortcircuit.reads", false);
  }

  static void restartSourceCluster(int numSlaves) throws Exception {
    Closeables.close(hbaseAdmin, true);
    Closeables.close(htable1, true);
    UTIL1.shutdownMiniHBaseCluster();
    UTIL1.restartHBaseCluster(numSlaves);
    // Invalidate the cached connection state.
    CONF1 = UTIL1.getConfiguration();
    hbaseAdmin = UTIL1.getAdmin();
    Connection connection1 = UTIL1.getConnection();
    htable1 = connection1.getTable(tableName);
  }

  static void restartTargetHBaseCluster(int numSlaves) throws Exception {
    Closeables.close(htable2, true);
    UTIL2.restartHBaseCluster(numSlaves);
    // Invalidate the cached connection state
    CONF2 = UTIL2.getConfiguration();
    htable2 = UTIL2.getConnection().getTable(tableName);
  }

  private static void startClusters() throws Exception {
    UTIL1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = UTIL1.getZkCluster();
    LOG.info("Setup first Zk");

    UTIL2.setZkCluster(miniZK);
    LOG.info("Setup second Zk");

    CONF_WITH_LOCALFS = HBaseConfiguration.create(CONF1);
    UTIL1.startMiniCluster(NUM_SLAVES1);
    // Have a bunch of slave servers, because inter-cluster shipping logic uses number of sinks
    // as a component in deciding maximum number of parallel batches to send to the peer cluster.
    UTIL2.startMiniCluster(NUM_SLAVES2);

    connection1 = ConnectionFactory.createConnection(CONF1);
    connection2 = ConnectionFactory.createConnection(CONF2);
    admin = new ReplicationAdmin(CONF1);
    hbaseAdmin = connection1.getAdmin();

    TableDescriptor table = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName).setMaxVersions(100)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();

    try (
      Admin admin1 = connection1.getAdmin();
      Admin admin2 = connection2.getAdmin()) {
      admin1.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      admin2.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      UTIL1.waitUntilAllRegionsAssigned(tableName);
      htable1 = connection1.getTable(tableName);
      UTIL2.waitUntilAllRegionsAssigned(tableName);
      htable2 = connection2.getTable(tableName);
    }

  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    configureClusters(UTIL1, UTIL2);
    startClusters();
  }

  private boolean peerExist(String peerId) throws IOException {
    return hbaseAdmin.listReplicationPeers().stream().anyMatch(p -> peerId.equals(p.getPeerId()));
  }

  @Before
  public void setUpBase() throws Exception {
    if (!peerExist(PEER_ID2)) {
      ReplicationPeerConfigBuilder builder = ReplicationPeerConfig.newBuilder()
        .setClusterKey(UTIL2.getClusterKey()).setSerial(isSerialPeer()).setReplicationEndpointImpl(
          ReplicationEndpointTest.class.getName());
      hbaseAdmin.addReplicationPeer(PEER_ID2, builder.build());
    }
  }

  @After
  public void tearDownBase() throws Exception {
    if (peerExist(PEER_ID2)) {
      hbaseAdmin.removeReplicationPeer(PEER_ID2);
    }
  }

  protected static void runSimplePutDeleteTest() throws IOException, InterruptedException {
    Put put = new Put(row);
    put.addColumn(famName, row, row);

    htable1 = UTIL1.getConnection().getTable(tableName);
    htable1.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.isEmpty()) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(row, res.value());
        break;
      }
    }

    Delete del = new Delete(row);
    htable1.delete(del);

    get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
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

  protected static void runSmallBatchTest() throws IOException, InterruptedException {
    // normal Batch tests
    loadData("", row);

    Scan scan = new Scan();

    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);

    waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (htable2 != null) {
      htable2.close();
    }
    if (htable1 != null) {
      htable1.close();
    }
    if (admin != null) {
      admin.close();
    }
    if (hbaseAdmin != null) {
      hbaseAdmin.close();
    }

    if (connection2 != null) {
      connection2.close();
    }
    if (connection1 != null) {
      connection1.close();
    }
    UTIL2.shutdownMiniCluster();
    UTIL1.shutdownMiniCluster();
  }

  /**
   * Custom replication endpoint to keep track of replication status for tests.
   */
  public static class ReplicationEndpointTest extends HBaseInterClusterReplicationEndpoint {
    public ReplicationEndpointTest() {
      replicateCount.set(0);
    }

    @Override public boolean replicate(ReplicateContext replicateContext) {
      replicateCount.incrementAndGet();
      replicatedEntries.addAll(replicateContext.getEntries());

      return super.replicate(replicateContext);
    }
  }
}
