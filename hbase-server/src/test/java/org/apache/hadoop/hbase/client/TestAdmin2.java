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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Class to test HBaseAdmin.
 * Spins up the minicluster once at test start and then takes it down afterward.
 * Add any testing of HBaseAdmin functionality here.
 */
@Category({LargeTests.class, ClientTests.class})
public class TestAdmin2 extends TestAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAdmin2.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAdmin2.class);

  @Test
  public void testCreateBadTables() throws IOException {
    String msg = null;
    try {
      ADMIN.createTable(new HTableDescriptor(TableName.META_TABLE_NAME));
    } catch(TableExistsException e) {
      msg = e.toString();
    }
    assertTrue("Unexcepted exception message " + msg, msg != null &&
      msg.startsWith(TableExistsException.class.getName()) &&
      msg.contains(TableName.META_TABLE_NAME.getNameAsString()));

    // Now try and do concurrent creation with a bunch of threads.
    final HTableDescriptor threadDesc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    threadDesc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    int count = 10;
    Thread [] threads = new Thread [count];
    final AtomicInteger successes = new AtomicInteger(0);
    final AtomicInteger failures = new AtomicInteger(0);
    final Admin localAdmin = ADMIN;
    for (int i = 0; i < count; i++) {
      threads[i] = new Thread(Integer.toString(i)) {
        @Override
        public void run() {
          try {
            localAdmin.createTable(threadDesc);
            successes.incrementAndGet();
          } catch (TableExistsException e) {
            failures.incrementAndGet();
          } catch (IOException e) {
            throw new RuntimeException("Failed threaded create" + getName(), e);
          }
        }
      };
    }
    for (int i = 0; i < count; i++) {
      threads[i].start();
    }
    for (int i = 0; i < count; i++) {
      while(threads[i].isAlive()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
    // All threads are now dead.  Count up how many tables were created and
    // how many failed w/ appropriate exception.
    assertEquals(1, successes.get());
    assertEquals(count - 1, failures.get());
  }

  /**
   * Test for hadoop-1581 'HBASE: Unopenable tablename bug'.
   * @throws Exception
   */
  @Test
  public void testTableNameClash() throws Exception {
    final String name = this.name.getMethodName();
    HTableDescriptor htd1 = new HTableDescriptor(TableName.valueOf(name + "SOMEUPPERCASE"));
    HTableDescriptor htd2 = new HTableDescriptor(TableName.valueOf(name));
    htd1.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    htd2.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    ADMIN.createTable(htd1);
    ADMIN.createTable(htd2);
    // Before fix, below would fail throwing a NoServerForRegionException.
    TEST_UTIL.getConnection().getTable(htd2.getTableName()).close();
  }

  /***
   * HMaster.createTable used to be kind of synchronous call
   * Thus creating of table with lots of regions can cause RPC timeout
   * After the fix to make createTable truly async, RPC timeout shouldn't be an
   * issue anymore
   * @throws Exception
   */
  @Test
  public void testCreateTableRPCTimeOut() throws Exception {
    final String name = this.name.getMethodName();
    int oldTimeout = TEST_UTIL.getConfiguration().
      getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 1500);
    try {
      int expectedRegions = 100;
      // Use 80 bit numbers to make sure we aren't limited
      byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
      byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };
      Admin hbaseadmin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
      htd.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
      hbaseadmin.createTable(htd, startKey, endKey, expectedRegions);
    } finally {
      TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, oldTimeout);
    }
  }

  /**
   * Test read only tables
   * @throws Exception
   */
  @Test
  public void testReadOnlyTable() throws Exception {
    final TableName name = TableName.valueOf(this.name.getMethodName());
    Table table = TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    byte[] value = Bytes.toBytes("somedata");
    // This used to use an empty row... That must have been a bug
    Put put = new Put(value);
    put.addColumn(HConstants.CATALOG_FAMILY, HConstants.CATALOG_FAMILY, value);
    table.put(put);
    table.close();
  }

  /**
   * Test that user table names can contain '-' and '.' so long as they do not
   * start with same. HBASE-771
   * @throws IOException
   */
  @Test
  public void testTableNames() throws IOException {
    byte[][] illegalNames = new byte[][] {
        Bytes.toBytes("-bad"),
        Bytes.toBytes(".bad")
    };
    for (byte[] illegalName : illegalNames) {
      try {
        new HTableDescriptor(TableName.valueOf(illegalName));
        throw new IOException("Did not detect '" +
            Bytes.toString(illegalName) + "' as an illegal user table name");
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
    byte[] legalName = Bytes.toBytes("g-oo.d");
    try {
      new HTableDescriptor(TableName.valueOf(legalName));
    } catch (IllegalArgumentException e) {
      throw new IOException("Legal user table name: '" +
        Bytes.toString(legalName) + "' caused IllegalArgumentException: " +
        e.getMessage());
    }
  }

  /**
   * For HADOOP-2579
   * @throws IOException
   */
  @Test (expected=TableExistsException.class)
  public void testTableExistsExceptionWithATable() throws IOException {
    final TableName name = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY).close();
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
  }

  /**
   * Can't disable a table if the table isn't in enabled state
   * @throws IOException
   */
  @Test (expected=TableNotEnabledException.class)
  public void testTableNotEnabledExceptionWithATable() throws IOException {
    final TableName name = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY).close();
    ADMIN.disableTable(name);
    ADMIN.disableTable(name);
  }

  /**
   * Can't enable a table if the table isn't in disabled state
   */
  @Test(expected = TableNotDisabledException.class)
  public void testTableNotDisabledExceptionWithATable() throws IOException {
    final TableName name = TableName.valueOf(this.name.getMethodName());
    Table t = TEST_UTIL.createTable(name, HConstants.CATALOG_FAMILY);
    try {
      ADMIN.enableTable(name);
    } finally {
      t.close();
    }
  }

  /**
   * For HADOOP-2579
   */
  @Test(expected = TableNotFoundException.class)
  public void testTableNotFoundExceptionWithoutAnyTables() throws IOException {
    TableName tableName = TableName.valueOf("testTableNotFoundExceptionWithoutAnyTables");
    Table ht = TEST_UTIL.getConnection().getTable(tableName);
    ht.get(new Get(Bytes.toBytes("e")));
  }

  @Test
  public void testShouldUnassignTheRegion() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    createTableWithDefaultConf(tableName);

    RegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(tableName);
    List<RegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (RegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.getTable().isSystemTable()) {
        info = regionInfo;
        ADMIN.unassign(regionInfo.getRegionName(), true);
      }
    }
    boolean isInList = ProtobufUtil.getOnlineRegions(
      rs.getRSRpcServices()).contains(info);
    long timeout = System.currentTimeMillis() + 10000;
    while ((System.currentTimeMillis() < timeout) && (isInList)) {
      Thread.sleep(100);
      isInList = ProtobufUtil.getOnlineRegions(
        rs.getRSRpcServices()).contains(info);
    }

    assertFalse("The region should not be present in online regions list.",
      isInList);
  }

  @Test
  public void testCloseRegionIfInvalidRegionNameIsPassed() throws Exception {
    final String name = this.name.getMethodName();
    byte[] tableName = Bytes.toBytes(name);
    createTableWithDefaultConf(tableName);

    RegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(tableName));
    List<RegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (RegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaRegion()) {
        if (regionInfo.getRegionNameAsString().contains(name)) {
          info = regionInfo;
          try {
            ADMIN.unassign(Bytes.toBytes("sample"), true);
          } catch (UnknownRegionException nsre) {
            // expected, ignore it
          }
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
        onlineRegions.contains(info));
  }

  @Test
  public void testCloseRegionThatFetchesTheHRIFromMeta() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    createTableWithDefaultConf(tableName);

    RegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(tableName);
    List<RegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (RegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaRegion()) {
        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion2")) {
          info = regionInfo;
          ADMIN.unassign(regionInfo.getRegionName(), true);
        }
      }
    }

    boolean isInList = ProtobufUtil.getOnlineRegions(
      rs.getRSRpcServices()).contains(info);
    long timeout = System.currentTimeMillis() + 10000;
    while ((System.currentTimeMillis() < timeout) && (isInList)) {
      Thread.sleep(100);
      isInList = ProtobufUtil.getOnlineRegions(
        rs.getRSRpcServices()).contains(info);
    }

    assertFalse("The region should not be present in online regions list.",
      isInList);
  }

  private HBaseAdmin createTable(TableName tableName) throws IOException {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();

    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("value");

    htd.addFamily(hcd);
    admin.createTable(htd, null);
    return admin;
  }

  private void createTableWithDefaultConf(byte[] TABLENAME) throws IOException {
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));
  }

  private void createTableWithDefaultConf(TableName TABLENAME) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    ADMIN.createTable(htd, null);
  }

  /**
   * For HBASE-2556
   */
  @Test
  public void testGetTableRegions() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    int expectedRegions = 10;

    // Use 80 bit numbers to make sure we aren't limited
    byte [] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte [] endKey =   { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };


    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    ADMIN.createTable(desc, startKey, endKey, expectedRegions);

    List<RegionInfo> RegionInfos = ADMIN.getRegions(tableName);

    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + RegionInfos.size(),
        expectedRegions, RegionInfos.size());
 }

  @Test
  public void testMoveToPreviouslyAssignedRS() throws IOException, InterruptedException {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Admin localAdmin = createTable(tableName);
    List<RegionInfo> tableRegions = localAdmin.getRegions(tableName);
    RegionInfo hri = tableRegions.get(0);
    AssignmentManager am = master.getAssignmentManager();
    ServerName server = am.getRegionStates().getRegionServerOfRegion(hri);
    localAdmin.move(hri.getEncodedNameAsBytes(), server);
    assertEquals("Current region server and region server before move should be same.", server,
      am.getRegionStates().getRegionServerOfRegion(hri));
  }

  @Test
  public void testWALRollWriting() throws Exception {
    setUpforLogRolling();
    String className = this.getClass().getName();
    StringBuilder v = new StringBuilder(className);
    while (v.length() < 1000) {
      v.append(className);
    }
    byte[] value = Bytes.toBytes(v.toString());
    HRegionServer regionServer = startAndWriteData(TableName.valueOf(name.getMethodName()), value);
    LOG.info("after writing there are "
        + AbstractFSWALProvider.getNumRolledLogFiles(regionServer.getWAL(null)) + " log files");

    // flush all regions
    for (HRegion r : regionServer.getOnlineRegionsLocalContext()) {
      r.flush(true);
    }
    ADMIN.rollWALWriter(regionServer.getServerName());
    int count = AbstractFSWALProvider.getNumRolledLogFiles(regionServer.getWAL(null));
    LOG.info("after flushing all regions and rolling logs there are " +
        count + " log files");
    assertTrue(("actual count: " + count), count <= 2);
  }

  private void setUpforLogRolling() {
    // Force a region split after every 768KB
    TEST_UTIL.getConfiguration().setLong(HConstants.HREGION_MAX_FILESIZE,
        768L * 1024L);

    // We roll the log after every 32 writes
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.maxlogentries", 32);

    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.logroll.errors.tolerated", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 10 * 1000);

    // For less frequently updated regions flush after every 2 flushes
    TEST_UTIL.getConfiguration().setInt(
        "hbase.hregion.memstore.optionalflushcount", 2);

    // We flush the cache after every 8192 bytes
    TEST_UTIL.getConfiguration().setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
        8192);

    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 10 * 1000);

    // Reduce thread wake frequency so that other threads can get
    // a chance to run.
    TEST_UTIL.getConfiguration().setInt(HConstants.THREAD_WAKE_FREQUENCY,
        2 * 1000);

    /**** configuration for testLogRollOnDatanodeDeath ****/
    // lower the namenode & datanode heartbeat so the namenode
    // quickly detects datanode failures
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    // the namenode might still try to choose the recently-dead datanode
    // for a pipeline, so try to a new pipeline multiple times
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 30);
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.hlog.tolerable.lowreplication", 2);
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.hlog.lowreplication.rolllimit", 3);
  }

  private HRegionServer startAndWriteData(TableName tableName, byte[] value)
  throws IOException, InterruptedException {
    // When the hbase:meta table can be opened, the region servers are running
    TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME).close();

    // Create the test table and open it
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    ADMIN.createTable(desc);
    Table table = TEST_UTIL.getConnection().getTable(tableName);

    HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(tableName);
    for (int i = 1; i <= 256; i++) { // 256 writes should cause 8 log rolls
      Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
      put.addColumn(HConstants.CATALOG_FAMILY, null, value);
      table.put(put);
      if (i % 32 == 0) {
        // After every 32 writes sleep to let the log roller run
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }

    table.close();
    return regionServer;
  }

  /**
   * Check that we have an exception if the cluster is not there.
   */
  @Test
  public void testCheckHBaseAvailableWithoutCluster() {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());

    // Change the ZK address to go to something not used.
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT,
      conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 9999)+10);

    long start = System.currentTimeMillis();
    try {
      HBaseAdmin.available(conf);
      assertTrue(false);
    } catch (ZooKeeperConnectionException ignored) {
    } catch (IOException ignored) {
    }
    long end = System.currentTimeMillis();

    LOG.info("It took "+(end-start)+" ms to find out that" +
      " HBase was not available");
  }

  @Test
  public void testDisableCatalogTable() throws Exception {
    try {
      ADMIN.disableTable(TableName.META_TABLE_NAME);
      fail("Expected to throw ConstraintException");
    } catch (ConstraintException e) {
    }
    // Before the fix for HBASE-6146, the below table creation was failing as the hbase:meta table
    // actually getting disabled by the disableTable() call.
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf(Bytes.toBytes(name.getMethodName())));
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("cf1"));
    htd.addFamily(hcd);
    TEST_UTIL.getHBaseAdmin().createTable(htd);
  }

  @Test
  public void testIsEnabledOrDisabledOnUnknownTable() throws Exception {
    try {
      ADMIN.isTableEnabled(TableName.valueOf(name.getMethodName()));
      fail("Test should fail if isTableEnabled called on unknown table.");
    } catch (IOException e) {
    }

    try {
      ADMIN.isTableDisabled(TableName.valueOf(name.getMethodName()));
      fail("Test should fail if isTableDisabled called on unknown table.");
    } catch (IOException e) {
    }
  }

  @Test
  public void testGetRegion() throws Exception {
    // We use actual HBaseAdmin instance instead of going via Admin interface in
    // here because makes use of an internal HBA method (TODO: Fix.).
    HBaseAdmin rawAdmin = TEST_UTIL.getHBaseAdmin();

    final TableName tableName = TableName.valueOf(name.getMethodName());
    LOG.info("Started " + tableName);
    Table t = TEST_UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY);

    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      HRegionLocation regionLocation = locator.getRegionLocation(Bytes.toBytes("mmm"));
      RegionInfo region = regionLocation.getRegionInfo();
      byte[] regionName = region.getRegionName();
      Pair<RegionInfo, ServerName> pair = rawAdmin.getRegion(regionName);
      assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
      pair = rawAdmin.getRegion(region.getEncodedNameAsBytes());
      assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
    }
  }

  @Test
  public void testBalancer() throws Exception {
    boolean initialState = ADMIN.isBalancerEnabled();

    // Start the balancer, wait for it.
    boolean prevState = ADMIN.setBalancerRunning(!initialState, true);

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, ADMIN.isBalancerEnabled());

    // Reset it back to what it was
    prevState = ADMIN.setBalancerRunning(initialState, true);

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);
    // Current state should be the original state again
    assertEquals(initialState, ADMIN.isBalancerEnabled());
  }

  @Test
  public void testRegionNormalizer() throws Exception {
    boolean initialState = ADMIN.isNormalizerEnabled();

    // flip state
    boolean prevState = ADMIN.setNormalizerRunning(!initialState);

    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);

    // Current state should be opposite of the original
    assertEquals(!initialState, ADMIN.isNormalizerEnabled());

    // Reset it back to what it was
    prevState = ADMIN.setNormalizerRunning(initialState);

    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);
    // Current state should be the original state again
    assertEquals(initialState, ADMIN.isNormalizerEnabled());
  }

  @Test
  public void testAbortProcedureFail() throws Exception {
    Random randomGenerator = new Random();
    long procId = randomGenerator.nextLong();

    boolean abortResult = ADMIN.abortProcedure(procId, true);
    assertFalse(abortResult);
  }

  @Test
  public void testGetProcedures() throws Exception {
    String procList = ADMIN.getProcedures();
    assertTrue(procList.startsWith("["));
  }

  @Test
  public void testGetLocks() throws Exception {
    String lockList = ADMIN.getLocks();
    assertTrue(lockList.startsWith("["));
  }

  @Test
  public void testDecommissionRegionServers() throws Exception {
    List<ServerName> decommissionedRegionServers = ADMIN.listDecommissionedRegionServers();
    assertTrue(decommissionedRegionServers.isEmpty());

    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 6);

    ArrayList<ServerName> clusterRegionServers =
        new ArrayList<>(ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
          .getLiveServerMetrics().keySet());

    assertEquals(3, clusterRegionServers.size());

    HashMap<ServerName, List<RegionInfo>> serversToDecommssion = new HashMap<>();
    // Get a server that has meta online. We will decommission two of the servers,
    // leaving one online.
    int i;
    for (i = 0; i < clusterRegionServers.size(); i++) {
      List<RegionInfo> regionsOnServer = ADMIN.getRegions(clusterRegionServers.get(i));
      if (ADMIN.getRegions(clusterRegionServers.get(i)).stream().anyMatch(p -> p.isMetaRegion())) {
        serversToDecommssion.put(clusterRegionServers.get(i), regionsOnServer);
        break;
      }
    }

    clusterRegionServers.remove(i);
    // Get another server to decommission.
    serversToDecommssion.put(clusterRegionServers.get(0),
      ADMIN.getRegions(clusterRegionServers.get(0)));

    ServerName remainingServer = clusterRegionServers.get(1);

    // Decommission
    ADMIN.decommissionRegionServers(new ArrayList<ServerName>(serversToDecommssion.keySet()), true);
    assertEquals(2, ADMIN.listDecommissionedRegionServers().size());

    // Verify the regions have been off the decommissioned servers, all on the one
    // remaining server.
    for (ServerName server : serversToDecommssion.keySet()) {
      for (RegionInfo region : serversToDecommssion.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, remainingServer, 10000);
      }
    }

    // Recommission and load the regions.
    for (ServerName server : serversToDecommssion.keySet()) {
      List<byte[]> encodedRegionNames = serversToDecommssion.get(server).stream()
          .map(region -> region.getEncodedNameAsBytes()).collect(Collectors.toList());
      ADMIN.recommissionRegionServer(server, encodedRegionNames);
    }
    assertTrue(ADMIN.listDecommissionedRegionServers().isEmpty());
    // Verify the regions have been moved to the recommissioned servers
    for (ServerName server : serversToDecommssion.keySet()) {
      for (RegionInfo region : serversToDecommssion.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, server, 10000);
      }
    }
  }

  /**
   * TestCase for HBASE-21355
   */
  @Test
  public void testGetRegionInfo() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table table = TEST_UTIL.createTable(tableName, Bytes.toBytes("f"));
    for (int i = 0; i < 100; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"),
        Bytes.toBytes(i)));
    }
    ADMIN.flush(tableName);

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
    List<HRegion> regions = rs.getRegions(tableName);
    Assert.assertEquals(1, regions.size());

    HRegion region = regions.get(0);
    byte[] regionName = region.getRegionInfo().getRegionName();
    HStore store = region.getStore(Bytes.toBytes("f"));
    long expectedStoreFilesSize = store.getStorefilesSize();
    Assert.assertNotNull(store);
    Assert.assertEquals(expectedStoreFilesSize, store.getSize());

    ClusterConnection conn = ((ClusterConnection) ADMIN.getConnection());
    HBaseRpcController controller = conn.getRpcControllerFactory().newController();
    for (int i = 0; i < 10; i++) {
      RegionInfo ri =
          ProtobufUtil.getRegionInfo(controller, conn.getAdmin(rs.getServerName()), regionName);
      Assert.assertEquals(region.getRegionInfo(), ri);

      // Make sure that the store size is still the actual file system's store size.
      Assert.assertEquals(expectedStoreFilesSize, store.getSize());
    }
  }

  @Test
  public void testTableSplitFollowedByModify() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, Bytes.toBytes("f"));

    // get the original table region count
    List<RegionInfo> regions = ADMIN.getRegions(tableName);
    int originalCount = regions.size();
    assertEquals(1, originalCount);

    // split the table and wait until region count increases
    ADMIN.split(tableName, Bytes.toBytes(3));
    TEST_UTIL.waitFor(30000, new Predicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return ADMIN.getRegions(tableName).size() > originalCount;
      }
    });

    // do some table modification
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(ADMIN.getDescriptor(tableName))
        .setMaxFileSize(11111111)
        .build();
    ADMIN.modifyTable(tableDesc);
    assertEquals(11111111, ADMIN.getDescriptor(tableName).getMaxFileSize());
  }

  @Test
  public void testTableMergeFollowedByModify() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, new byte[][] { Bytes.toBytes("f") },
      new byte[][] { Bytes.toBytes(3) });

    // assert we have at least 2 regions in the table
    List<RegionInfo> regions = ADMIN.getRegions(tableName);
    int originalCount = regions.size();
    assertTrue(originalCount >= 2);

    byte[] nameOfRegionA = regions.get(0).getEncodedNameAsBytes();
    byte[] nameOfRegionB = regions.get(1).getEncodedNameAsBytes();

    // merge the table regions and wait until region count decreases
    ADMIN.mergeRegionsAsync(nameOfRegionA, nameOfRegionB, true);
    TEST_UTIL.waitFor(30000, new Predicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return ADMIN.getRegions(tableName).size() < originalCount;
      }
    });

    // do some table modification
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(ADMIN.getDescriptor(tableName))
        .setMaxFileSize(11111111)
        .build();
    ADMIN.modifyTable(tableDesc);
    assertEquals(11111111, ADMIN.getDescriptor(tableName).getMaxFileSize());
  }

  @Test
  public void testSnapshotCleanupAsync() throws Exception {
    testSnapshotCleanup(false);
  }

  @Test
  public void testSnapshotCleanupSync() throws Exception {
    testSnapshotCleanup(true);
  }

  private void testSnapshotCleanup(final boolean synchronous) throws IOException {
    final boolean initialState = ADMIN.isSnapshotCleanupEnabled();
    // Switch the snapshot auto cleanup state to opposite to initial state
    boolean prevState = ADMIN.snapshotCleanupSwitch(!initialState, synchronous);
    // The previous state should be the original state we observed
    assertEquals(initialState, prevState);
    // Current state should be opposite of the initial state
    assertEquals(!initialState, ADMIN.isSnapshotCleanupEnabled());
    // Reset the state back to what it was initially
    prevState = ADMIN.snapshotCleanupSwitch(initialState, synchronous);
    // The previous state should be the opposite of the initial state
    assertEquals(!initialState, prevState);
    // Current state should be the original state again
    assertEquals(initialState, ADMIN.isSnapshotCleanupEnabled());
  }

}
