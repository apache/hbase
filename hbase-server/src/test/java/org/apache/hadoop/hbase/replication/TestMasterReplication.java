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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.regionserver.TestSourceFSConfigurationProvider;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ReplicationTests.class, LargeTests.class})
public class TestMasterReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterReplication.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterReplication.class);

  private Configuration baseConfiguration;

  private HBaseTestingUtil[] utilities;
  private Configuration[] configurations;
  private MiniZooKeeperCluster miniZK;

  private static final long SLEEP_TIME = 1000;
  private static final int NB_RETRIES = 120;

  private static final TableName tableName = TableName.valueOf("test");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] famName1 = Bytes.toBytes("f1");
  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");
  private static final byte[] row3 = Bytes.toBytes("row3");
  private static final byte[] row4 = Bytes.toBytes("row4");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");

  private static final byte[] count = Bytes.toBytes("count");
  private static final byte[] put = Bytes.toBytes("put");
  private static final byte[] delete = Bytes.toBytes("delete");

  private TableDescriptor table;

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
    baseConfiguration.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    baseConfiguration.set("hbase.replication.source.fs.conf.provider",
      TestSourceFSConfigurationProvider.class.getCanonicalName());
    baseConfiguration.set(HConstants.REPLICATION_CLUSTER_ID, "12345");
    baseConfiguration.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    baseConfiguration.setStrings(
        CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        CoprocessorCounter.class.getName());
    table = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName1)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();
  }

  /**
   * It tests the replication scenario involving 0 -> 1 -> 0. It does it by
   * adding and deleting a row to a table in each cluster, checking if it's
   * replicated. It also tests that the puts and deletes are not replicated back
   * to the originating cluster.
   */
  @Test
  public void testCyclicReplication1() throws Exception {
    LOG.info("testSimplePutDelete");
    int numClusters = 2;
    Table[] htables = null;
    try {
      htables = setUpClusterTablesAndPeers(numClusters);

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
   * Tests the replication scenario 0 -> 0. By default
   * {@link org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint},
   * the replication peer should not be added.
   */
  @Test(expected = DoNotRetryIOException.class)
  public void testLoopedReplication()
    throws Exception {
    LOG.info("testLoopedReplication");
    startMiniClusters(1);
    createTableOnClusters(table);
    addPeer("1", 0, 0);
  }

  /**
   * It tests the replication scenario involving 0 -> 1 -> 0. It does it by bulk loading a set of
   * HFiles to a table in each cluster, checking if it's replicated.
   */
  @Test
  public void testHFileCyclicReplication() throws Exception {
    LOG.info("testHFileCyclicReplication");
    int numClusters = 2;
    Table[] htables = null;
    try {
      htables = setUpClusterTablesAndPeers(numClusters);

      // Load 100 rows for each hfile range in cluster '0' and validate whether its been replicated
      // to cluster '1'.
      byte[][][] hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][] { Bytes.toBytes("ddd"), Bytes.toBytes("fff") }, };
      int numOfRows = 100;
      int[] expectedCounts =
          new int[] { hfileRanges.length * numOfRows, hfileRanges.length * numOfRows };

      loadAndValidateHFileReplication("testHFileCyclicReplication_01", 0, new int[] { 1 }, row,
        famName, htables, hfileRanges, numOfRows, expectedCounts, true);

      // Load 200 rows for each hfile range in cluster '1' and validate whether its been replicated
      // to cluster '0'.
      hfileRanges = new byte[][][] { new byte[][] { Bytes.toBytes("gggg"), Bytes.toBytes("iiii") },
        new byte[][] { Bytes.toBytes("jjj"), Bytes.toBytes("lll") }, };
      numOfRows = 200;
      int[] newExpectedCounts = new int[] { hfileRanges.length * numOfRows + expectedCounts[0],
        hfileRanges.length * numOfRows + expectedCounts[1] };

      loadAndValidateHFileReplication("testHFileCyclicReplication_10", 1, new int[] { 0 }, row,
        famName, htables, hfileRanges, numOfRows, newExpectedCounts, true);

    } finally {
      close(htables);
      shutDownMiniClusters();
    }
  }

  private Table[] setUpClusterTablesAndPeers(int numClusters) throws Exception {
    Table[] htables;
    startMiniClusters(numClusters);
    createTableOnClusters(table);

    htables = getHTablesOnClusters(tableName);
    // Test the replication scenarios of 0 -> 1 -> 0
    addPeer("1", 0, 1);
    addPeer("1", 1, 0);
    return htables;
  }

  /**
   * Tests the cyclic replication scenario of 0 -> 1 -> 2 -> 0 by adding and deleting rows to a
   * table in each clusters and ensuring that the each of these clusters get the appropriate
   * mutations. It also tests the grouping scenario where a cluster needs to replicate the edits
   * originating from itself and also the edits that it received using replication from a different
   * cluster. The scenario is explained in HBASE-9158
   */
  @Test
  public void testCyclicReplication2() throws Exception {
    LOG.info("testCyclicReplication2");
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
      wait(row4, htables[0], false);
    } finally {
      close(htables);
      shutDownMiniClusters();
    }
  }

  /**
   * It tests the multi slave hfile replication scenario involving 0 -> 1, 2. It does it by bulk
   * loading a set of HFiles to a table in master cluster, checking if it's replicated in its peers.
   */
  @Test
  public void testHFileMultiSlaveReplication() throws Exception {
    LOG.info("testHFileMultiSlaveReplication");
    int numClusters = 3;
    Table[] htables = null;
    try {
      startMiniClusters(numClusters);
      createTableOnClusters(table);

      // Add a slave, 0 -> 1
      addPeer("1", 0, 1);

      htables = getHTablesOnClusters(tableName);

      // Load 100 rows for each hfile range in cluster '0' and validate whether its been replicated
      // to cluster '1'.
      byte[][][] hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes("mmmm"), Bytes.toBytes("oooo") },
          new byte[][] { Bytes.toBytes("ppp"), Bytes.toBytes("rrr") }, };
      int numOfRows = 100;

      int[] expectedCounts =
        new int[] { hfileRanges.length * numOfRows, hfileRanges.length * numOfRows };

      loadAndValidateHFileReplication("testHFileCyclicReplication_0", 0, new int[] { 1 }, row,
        famName, htables, hfileRanges, numOfRows, expectedCounts, true);

      // Validate data is not replicated to cluster '2'.
      assertEquals(0, utilities[2].countRows(htables[2]));

      rollWALAndWait(utilities[0], htables[0].getName(), row);

      // Add one more slave, 0 -> 2
      addPeer("2", 0, 2);

      // Load 200 rows for each hfile range in cluster '0' and validate whether its been replicated
      // to cluster '1' and '2'. Previous data should be replicated to cluster '2'.
      hfileRanges = new byte[][][] { new byte[][] { Bytes.toBytes("ssss"), Bytes.toBytes("uuuu") },
        new byte[][] { Bytes.toBytes("vvv"), Bytes.toBytes("xxx") }, };
      numOfRows = 200;

      int[] newExpectedCounts = new int[] { hfileRanges.length * numOfRows + expectedCounts[0],
        hfileRanges.length * numOfRows + expectedCounts[1], hfileRanges.length * numOfRows };

      loadAndValidateHFileReplication("testHFileCyclicReplication_1", 0, new int[] { 1, 2 }, row,
        famName, htables, hfileRanges, numOfRows, newExpectedCounts, true);

    } finally {
      close(htables);
      shutDownMiniClusters();
    }
  }

  /**
   * It tests the bulk loaded hfile replication scenario to only explicitly specified table column
   * families. It does it by bulk loading a set of HFiles belonging to both the CFs of table and set
   * only one CF data to replicate.
   */
  @Test
  public void testHFileReplicationForConfiguredTableCfs() throws Exception {
    LOG.info("testHFileReplicationForConfiguredTableCfs");
    int numClusters = 2;
    Table[] htables = null;
    try {
      startMiniClusters(numClusters);
      createTableOnClusters(table);

      htables = getHTablesOnClusters(tableName);
      // Test the replication scenarios only 'f' is configured for table data replication not 'f1'
      addPeer("1", 0, 1, tableName.getNameAsString() + ":" + Bytes.toString(famName));

      // Load 100 rows for each hfile range in cluster '0' for table CF 'f'
      byte[][][] hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][] { Bytes.toBytes("ddd"), Bytes.toBytes("fff") }, };
      int numOfRows = 100;
      int[] expectedCounts =
          new int[] { hfileRanges.length * numOfRows, hfileRanges.length * numOfRows };

      loadAndValidateHFileReplication("load_f", 0, new int[] { 1 }, row, famName, htables,
        hfileRanges, numOfRows, expectedCounts, true);

      // Load 100 rows for each hfile range in cluster '0' for table CF 'f1'
      hfileRanges = new byte[][][] { new byte[][] { Bytes.toBytes("gggg"), Bytes.toBytes("iiii") },
        new byte[][] { Bytes.toBytes("jjj"), Bytes.toBytes("lll") }, };
      numOfRows = 100;

      int[] newExpectedCounts =
        new int[] { hfileRanges.length * numOfRows + expectedCounts[0], expectedCounts[1] };

      loadAndValidateHFileReplication("load_f1", 0, new int[] { 1 }, row, famName1, htables,
        hfileRanges, numOfRows, newExpectedCounts, false);

      // Validate data replication for CF 'f1'

      // Source cluster table should contain data for the families
      wait(0, htables[0], hfileRanges.length * numOfRows + expectedCounts[0]);

      // Sleep for enough time so that the data is still not replicated for the CF which is not
      // configured for replication
      Thread.sleep((NB_RETRIES / 2) * SLEEP_TIME);
      // Peer cluster should have only configured CF data
      wait(1, htables[1], expectedCounts[1]);
    } finally {
      close(htables);
      shutDownMiniClusters();
    }
  }

  /**
   * Tests cyclic replication scenario of 0 -> 1 -> 2 -> 1.
   */
  @Test
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

  /**
   * Tests that base replication peer configs are applied on peer creation
   * and the configs are overriden if updated as part of updateReplicationPeerConfig()
   *
   */
  @Test
  public void testBasePeerConfigsForReplicationPeer()
    throws Exception {
    LOG.info("testBasePeerConfigsForPeerMutations");
    String firstCustomPeerConfigKey = "hbase.xxx.custom_config";
    String firstCustomPeerConfigValue = "test";
    String firstCustomPeerConfigUpdatedValue = "test_updated";

    String secondCustomPeerConfigKey = "hbase.xxx.custom_second_config";
    String secondCustomPeerConfigValue = "testSecond";
    String secondCustomPeerConfigUpdatedValue = "testSecondUpdated";
    try {
      baseConfiguration.set(ReplicationPeerConfigUtil.HBASE_REPLICATION_PEER_BASE_CONFIG,
        firstCustomPeerConfigKey.concat("=").concat(firstCustomPeerConfigValue));
      startMiniClusters(2);
      addPeer("1", 0, 1);
      addPeer("2", 0, 1);
      Admin admin = utilities[0].getAdmin();

      // Validates base configs 1 is present for both peer.
      Assert.assertEquals(firstCustomPeerConfigValue, admin.getReplicationPeerConfig("1").
        getConfiguration().get(firstCustomPeerConfigKey));
      Assert.assertEquals(firstCustomPeerConfigValue, admin.getReplicationPeerConfig("2").
        getConfiguration().get(firstCustomPeerConfigKey));

      // override value of configuration 1 for peer "1".
      ReplicationPeerConfig updatedReplicationConfigForPeer1 = ReplicationPeerConfig.
        newBuilder(admin.getReplicationPeerConfig("1")).
        putConfiguration(firstCustomPeerConfigKey, firstCustomPeerConfigUpdatedValue).build();

      // add configuration 2 for peer "2".
      ReplicationPeerConfig updatedReplicationConfigForPeer2 = ReplicationPeerConfig.
        newBuilder(admin.getReplicationPeerConfig("2")).
        putConfiguration(secondCustomPeerConfigKey, secondCustomPeerConfigUpdatedValue).build();

      admin.updateReplicationPeerConfig("1", updatedReplicationConfigForPeer1);
      admin.updateReplicationPeerConfig("2", updatedReplicationConfigForPeer2);

      // validates configuration is overridden by updateReplicationPeerConfig
      Assert.assertEquals(firstCustomPeerConfigUpdatedValue, admin.getReplicationPeerConfig("1").
        getConfiguration().get(firstCustomPeerConfigKey));
      Assert.assertEquals(secondCustomPeerConfigUpdatedValue, admin.getReplicationPeerConfig("2").
        getConfiguration().get(secondCustomPeerConfigKey));

      // Add second config to base config and perform restart.
      utilities[0].getConfiguration().set(ReplicationPeerConfigUtil.
        HBASE_REPLICATION_PEER_BASE_CONFIG, firstCustomPeerConfigKey.concat("=").
        concat(firstCustomPeerConfigValue).concat(";").concat(secondCustomPeerConfigKey)
        .concat("=").concat(secondCustomPeerConfigValue));

      utilities[0].shutdownMiniHBaseCluster();
      utilities[0].restartHBaseCluster(1);
      admin = utilities[0].getAdmin();

      // Configurations should be updated after restart again
      Assert.assertEquals(firstCustomPeerConfigValue, admin.getReplicationPeerConfig("1").
        getConfiguration().get(firstCustomPeerConfigKey));
      Assert.assertEquals(firstCustomPeerConfigValue, admin.getReplicationPeerConfig("2").
        getConfiguration().get(firstCustomPeerConfigKey));

      Assert.assertEquals(secondCustomPeerConfigValue, admin.getReplicationPeerConfig("1").
        getConfiguration().get(secondCustomPeerConfigKey));
      Assert.assertEquals(secondCustomPeerConfigValue, admin.getReplicationPeerConfig("2").
        getConfiguration().get(secondCustomPeerConfigKey));
    } finally {
      shutDownMiniClusters();
      baseConfiguration.unset(ReplicationPeerConfigUtil.HBASE_REPLICATION_PEER_BASE_CONFIG);
    }
  }

  @Test
  public void testBasePeerConfigsRemovalForReplicationPeer()
    throws Exception {
    LOG.info("testBasePeerConfigsForPeerMutations");
    String firstCustomPeerConfigKey = "hbase.xxx.custom_config";
    String firstCustomPeerConfigValue = "test";

    try {
      baseConfiguration.set(ReplicationPeerConfigUtil.HBASE_REPLICATION_PEER_BASE_CONFIG,
        firstCustomPeerConfigKey.concat("=").concat(firstCustomPeerConfigValue));
      startMiniClusters(2);
      addPeer("1", 0, 1);
      Admin admin = utilities[0].getAdmin();

      // Validates base configs 1 is present for both peer.
      Assert.assertEquals(firstCustomPeerConfigValue, admin.getReplicationPeerConfig("1").
        getConfiguration().get(firstCustomPeerConfigKey));

      utilities[0].getConfiguration().unset(ReplicationPeerConfigUtil.
        HBASE_REPLICATION_PEER_BASE_CONFIG);
      utilities[0].getConfiguration().set(ReplicationPeerConfigUtil.
        HBASE_REPLICATION_PEER_BASE_CONFIG, firstCustomPeerConfigKey.concat("=").concat(""));


      utilities[0].shutdownMiniHBaseCluster();
      utilities[0].restartHBaseCluster(1);
      admin = utilities[0].getAdmin();

      // Configurations should be removed after restart again
      Assert.assertNull(admin.getReplicationPeerConfig("1")
        .getConfiguration().get(firstCustomPeerConfigKey));
    } finally {
      shutDownMiniClusters();
      baseConfiguration.unset(ReplicationPeerConfigUtil.HBASE_REPLICATION_PEER_BASE_CONFIG);
    }
  }

  @Test
  public void testRemoveBasePeerConfigWithoutExistingConfigForReplicationPeer()
    throws Exception {
    LOG.info("testBasePeerConfigsForPeerMutations");
    String firstCustomPeerConfigKey = "hbase.xxx.custom_config";

    try {
      baseConfiguration.set(ReplicationPeerConfigUtil.HBASE_REPLICATION_PEER_BASE_CONFIG,
        firstCustomPeerConfigKey.concat("=").concat(""));
      startMiniClusters(2);
      addPeer("1", 0, 1);
      Admin admin = utilities[0].getAdmin();

      Assert.assertNull("Config should not be there", admin.getReplicationPeerConfig("1").
        getConfiguration().get(firstCustomPeerConfigKey));
    } finally {
      shutDownMiniClusters();
      baseConfiguration.unset(ReplicationPeerConfigUtil.HBASE_REPLICATION_PEER_BASE_CONFIG);
    }
  }

  @After
  public void tearDown() throws IOException {
    configurations = null;
    utilities = null;
  }

  @SuppressWarnings("resource")
  private void startMiniClusters(int numClusters) throws Exception {
    utilities = new HBaseTestingUtil[numClusters];
    configurations = new Configuration[numClusters];
    for (int i = 0; i < numClusters; i++) {
      Configuration conf = new Configuration(baseConfiguration);
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/" +
        i + ThreadLocalRandom.current().nextInt());
      HBaseTestingUtil utility = new HBaseTestingUtil(conf);
      if (i == 0) {
        utility.startMiniZKCluster();
        miniZK = utility.getZkCluster();
      } else {
        utility.setZkCluster(miniZK);
      }
      utility.startMiniCluster();
      utilities[i] = utility;
      configurations[i] = conf;
      new ZKWatcher(conf, "cluster" + i, null, true);
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

  private void createTableOnClusters(TableDescriptor table) throws Exception {
    for (HBaseTestingUtil utility : utilities) {
      utility.getAdmin().createTable(table);
    }
  }

  private void addPeer(String id, int masterClusterNumber,
      int slaveClusterNumber) throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configurations[masterClusterNumber]);
      Admin admin = conn.getAdmin()) {
      admin.addReplicationPeer(id,
        ReplicationPeerConfig.newBuilder().
          setClusterKey(utilities[slaveClusterNumber].getClusterKey()).build());
    }
  }

  private void addPeer(String id, int masterClusterNumber, int slaveClusterNumber, String tableCfs)
      throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configurations[masterClusterNumber]);
      Admin admin = conn.getAdmin()) {
      admin.addReplicationPeer(
        id,
        ReplicationPeerConfig.newBuilder()
          .setClusterKey(utilities[slaveClusterNumber].getClusterKey())
          .setReplicateAllUserTables(false)
          .setTableCFsMap(ReplicationPeerConfigUtil.parseTableCFsFromConfig(tableCfs)).build());
    }
  }

  private void disablePeer(String id, int masterClusterNumber) throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configurations[masterClusterNumber]);
      Admin admin = conn.getAdmin()) {
      admin.disableReplicationPeer(id);
    }
  }

  private void enablePeer(String id, int masterClusterNumber) throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configurations[masterClusterNumber]);
      Admin admin = conn.getAdmin()) {
      admin.enableReplicationPeer(id);
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
      LOG.warn("Exception occurred while closing the object:", e);
    }
  }

  @SuppressWarnings("resource")
  private Table[] getHTablesOnClusters(TableName tableName) throws Exception {
    int numClusters = utilities.length;
    Table[] htables = new Table[numClusters];
    for (int i = 0; i < numClusters; i++) {
      Table htable = ConnectionFactory.createConnection(configurations[i]).getTable(tableName);
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

  private void loadAndValidateHFileReplication(String testName, int masterNumber,
      int[] slaveNumbers, byte[] row, byte[] fam, Table[] tables, byte[][][] hfileRanges,
      int numOfRows, int[] expectedCounts, boolean toValidate) throws Exception {
    HBaseTestingUtil util = utilities[masterNumber];

    Path dir = util.getDataTestDirOnTestFS(testName);
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path familyDir = new Path(dir, Bytes.toString(fam));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      HFileTestUtil.createHFile(util.getConfiguration(), fs,
        new Path(familyDir, "hfile_" + hfileIdx++), fam, row, from, to, numOfRows);
    }

    Table source = tables[masterNumber];
    final TableName tableName = source.getName();
    BulkLoadHFiles.create(util.getConfiguration()).bulkLoad(tableName, dir);

    if (toValidate) {
      for (int slaveClusterNumber : slaveNumbers) {
        wait(slaveClusterNumber, tables[slaveClusterNumber], expectedCounts[slaveClusterNumber]);
      }
    }
  }

  private void wait(int slaveNumber, Table target, int expectedCount)
      throws IOException, InterruptedException {
    int count = 0;
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for bulkloaded data replication. Current count=" + count
            + ", expected count=" + expectedCount);
      }
      count = utilities[slaveNumber].countRows(target);
      if (count != expectedCount) {
        LOG.info("Waiting more time for bulkloaded data replication.");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  private void wait(byte[] row, Table target, boolean isDeleted) throws Exception {
    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for replication. Row:" + Bytes.toString(row)
            + ". IsDeleteReplication:" + isDeleted);
      }
      Result res = target.get(get);
      boolean sleep = isDeleted ? res.size() > 0 : res.isEmpty();
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

  private void rollWALAndWait(final HBaseTestingUtil utility, final TableName table,
      final byte[] row) throws IOException {
    final Admin admin = utility.getAdmin();
    final SingleProcessHBaseCluster cluster = utility.getMiniHBaseCluster();

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
    final WALActionsListener listener = new WALActionsListener() {
      @Override
      public void postLogRoll(final Path oldPath, final Path newPath) throws IOException {
        latch.countDown();
      }
    };
    region.getWAL().registerWALActionsListener(listener);

    // request a roll
    admin.rollWALWriter(cluster.getServerHoldingRegion(region.getTableDescriptor().getTableName(),
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

  /**
   * Use a coprocessor to count puts and deletes. as KVs would be replicated back with the same
   * timestamp there is otherwise no way to count them.
   */
  public static class CoprocessorCounter implements RegionCoprocessor, RegionObserver {
    private int nCount = 0;
    private int nDelete = 0;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

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
