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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.filter.FilterAllFilter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestClientClusterMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClientClusterMetrics.class);

  private static HBaseTestingUtility UTIL;
  private static Admin ADMIN;
  private final static int SLAVES = 5;
  private final static int MASTERS = 3;
  private static MiniHBaseCluster CLUSTER;
  private static HRegionServer DEAD;
  private static final TableName TABLE_NAME = TableName.valueOf("test");
  private static final byte[] CF = Bytes.toBytes("cf");


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, MyObserver.class.getName());
    UTIL = new HBaseTestingUtility(conf);
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(MASTERS).numRegionServers(SLAVES).numDataNodes(SLAVES).build();
    UTIL.startMiniCluster(option);
    CLUSTER = UTIL.getHBaseCluster();
    CLUSTER.waitForActiveAndReadyMaster();
    ADMIN = UTIL.getAdmin();
    // Kill one region server
    List<RegionServerThread> rsts = CLUSTER.getLiveRegionServerThreads();
    RegionServerThread rst = rsts.get(rsts.size() - 1);
    DEAD = rst.getRegionServer();
    DEAD.stop("Test dead servers metrics");
    while (rst.isAlive()) {
      Thread.sleep(500);
    }
  }

  @Test
  public void testDefaults() throws Exception {
    ClusterMetrics origin = ADMIN.getClusterMetrics();
    ClusterMetrics defaults = ADMIN.getClusterMetrics(EnumSet.allOf(Option.class));
    Assert.assertEquals(origin.getHBaseVersion(), defaults.getHBaseVersion());
    Assert.assertEquals(origin.getClusterId(), defaults.getClusterId());
    Assert.assertEquals(origin.getAverageLoad(), defaults.getAverageLoad(), 0);
    Assert.assertEquals(origin.getBackupMasterNames().size(),
        defaults.getBackupMasterNames().size());
    Assert.assertEquals(origin.getDeadServerNames().size(), defaults.getDeadServerNames().size());
    Assert.assertEquals(origin.getRegionCount(), defaults.getRegionCount());
    Assert.assertEquals(origin.getLiveServerMetrics().size(),
        defaults.getLiveServerMetrics().size());
    Assert.assertEquals(origin.getMasterInfoPort(), defaults.getMasterInfoPort());
    Assert.assertEquals(origin.getServersName().size(), defaults.getServersName().size());
    Assert.assertEquals(ADMIN.getRegionServers().size(), defaults.getServersName().size());
  }

  @Test
  public void testAsyncClient() throws Exception {
    try (AsyncConnection asyncConnect = ConnectionFactory.createAsyncConnection(
      UTIL.getConfiguration()).get()) {
      AsyncAdmin asyncAdmin = asyncConnect.getAdmin();
      CompletableFuture<ClusterMetrics> originFuture =
        asyncAdmin.getClusterMetrics();
      CompletableFuture<ClusterMetrics> defaultsFuture =
        asyncAdmin.getClusterMetrics(EnumSet.allOf(Option.class));
      ClusterMetrics origin = originFuture.get();
      ClusterMetrics defaults = defaultsFuture.get();
      Assert.assertEquals(origin.getHBaseVersion(), defaults.getHBaseVersion());
      Assert.assertEquals(origin.getClusterId(), defaults.getClusterId());
      Assert.assertEquals(origin.getHBaseVersion(), defaults.getHBaseVersion());
      Assert.assertEquals(origin.getClusterId(), defaults.getClusterId());
      Assert.assertEquals(origin.getAverageLoad(), defaults.getAverageLoad(), 0);
      Assert.assertEquals(origin.getBackupMasterNames().size(),
        defaults.getBackupMasterNames().size());
      Assert.assertEquals(origin.getDeadServerNames().size(), defaults.getDeadServerNames().size());
      Assert.assertEquals(origin.getRegionCount(), defaults.getRegionCount());
      Assert.assertEquals(origin.getLiveServerMetrics().size(),
        defaults.getLiveServerMetrics().size());
      Assert.assertEquals(origin.getMasterInfoPort(), defaults.getMasterInfoPort());
      Assert.assertEquals(origin.getServersName().size(), defaults.getServersName().size());
      origin.getTableRegionStatesCount().forEach(((tableName, regionStatesCount) -> {
        RegionStatesCount defaultRegionStatesCount = defaults.getTableRegionStatesCount()
          .get(tableName);
        Assert.assertEquals(defaultRegionStatesCount, regionStatesCount);
      }));
    }
  }

  @Test
  public void testLiveAndDeadServersStatus() throws Exception {
    // Count the number of live regionservers
    List<RegionServerThread> regionserverThreads = CLUSTER.getLiveRegionServerThreads();
    int numRs = 0;
    int len = regionserverThreads.size();
    for (int i = 0; i < len; i++) {
      if (regionserverThreads.get(i).isAlive()) {
        numRs++;
      }
    }
    // Depending on the (random) order of unit execution we may run this unit before the
    // minicluster is fully up and recovered from the RS shutdown done during test init.
    Waiter.waitFor(CLUSTER.getConfiguration(), 10 * 1000, 100, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        ClusterMetrics metrics = ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
        Assert.assertNotNull(metrics);
        return metrics.getRegionCount() > 0;
      }
    });
    // Retrieve live servers and dead servers info.
    EnumSet<Option> options =
        EnumSet.of(Option.LIVE_SERVERS, Option.DEAD_SERVERS, Option.SERVERS_NAME);
    ClusterMetrics metrics = ADMIN.getClusterMetrics(options);
    Assert.assertNotNull(metrics);
    // exclude a dead region server
    Assert.assertEquals(SLAVES -1, numRs);
    // live servers = nums of regionservers
    // By default, HMaster don't carry any regions so it won't report its load.
    // Hence, it won't be in the server list.
    Assert.assertEquals(numRs, metrics.getLiveServerMetrics().size());
    Assert.assertTrue(metrics.getRegionCount() > 0);
    Assert.assertNotNull(metrics.getDeadServerNames());
    Assert.assertEquals(1, metrics.getDeadServerNames().size());
    ServerName deadServerName = metrics.getDeadServerNames().iterator().next();
    Assert.assertEquals(DEAD.getServerName(), deadServerName);
    Assert.assertNotNull(metrics.getServersName());
    Assert.assertEquals(numRs, metrics.getServersName().size());
  }

  @Test
  public void testRegionStatesCount() throws Exception {
    Table table = UTIL.createTable(TABLE_NAME, CF);
    table.put(new Put(Bytes.toBytes("k1"))
      .addColumn(CF, Bytes.toBytes("q1"), Bytes.toBytes("v1")));
    table.put(new Put(Bytes.toBytes("k2"))
      .addColumn(CF, Bytes.toBytes("q2"), Bytes.toBytes("v2")));
    table.put(new Put(Bytes.toBytes("k3"))
      .addColumn(CF, Bytes.toBytes("q3"), Bytes.toBytes("v3")));

    ClusterMetrics metrics = ADMIN.getClusterMetrics();
    Assert.assertEquals(metrics.getTableRegionStatesCount().size(), 2);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getRegionsInTransition(), 0);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getOpenRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getTotalRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getClosedRegions(), 0);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getSplitRegions(), 0);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getRegionsInTransition(), 0);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getOpenRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getTotalRegions(), 1);

    UTIL.deleteTable(TABLE_NAME);
  }

  @Test
  public void testRegionStatesWithSplit() throws Exception {
    int startRowNum = 20;
    int rowCount = 80;
    Table table = UTIL.createTable(TABLE_NAME, CF);
    table.put(new Put(Bytes.toBytes("k1"))
      .addColumn(CF, Bytes.toBytes("q1"), Bytes.toBytes("v1")));
    table.put(new Put(Bytes.toBytes("k2"))
      .addColumn(CF, Bytes.toBytes("q2"), Bytes.toBytes("v2")));

    insertData(TABLE_NAME, startRowNum, rowCount);

    ClusterMetrics metrics = ADMIN.getClusterMetrics();
    Assert.assertEquals(metrics.getTableRegionStatesCount().size(), 2);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getRegionsInTransition(), 0);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getOpenRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getTotalRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getRegionsInTransition(), 0);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getOpenRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getTotalRegions(), 1);

    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    // Split region of the table
    ADMIN.split(TABLE_NAME, splitKey);

    metrics = ADMIN.getClusterMetrics();
    Assert.assertEquals(metrics.getTableRegionStatesCount().size(), 2);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getRegionsInTransition(), 0);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getOpenRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TableName.META_TABLE_NAME)
      .getTotalRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getRegionsInTransition(), 0);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getOpenRegions(), 2);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getTotalRegions(), 3);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getSplitRegions(), 1);
    Assert.assertEquals(metrics.getTableRegionStatesCount().get(TABLE_NAME)
      .getClosedRegions(), 0);

    UTIL.deleteTable(TABLE_NAME);
  }

  @Test public void testMasterAndBackupMastersStatus() throws Exception {
    // get all the master threads
    List<MasterThread> masterThreads = CLUSTER.getMasterThreads();
    int numActive = 0;
    int activeIndex = 0;
    ServerName activeName = null;
    HMaster active = null;
    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i).getMaster().isActiveMaster()) {
        numActive++;
        activeIndex = i;
        active = masterThreads.get(activeIndex).getMaster();
        activeName = active.getServerName();
      }
    }
    Assert.assertNotNull(active);
    Assert.assertEquals(1, numActive);
    Assert.assertEquals(MASTERS, masterThreads.size());
    // Retrieve master and backup masters infos only.
    EnumSet<Option> options = EnumSet.of(Option.MASTER, Option.BACKUP_MASTERS);
    ClusterMetrics metrics = ADMIN.getClusterMetrics(options);
    Assert.assertTrue(metrics.getMasterName().equals(activeName));
    Assert.assertEquals(MASTERS - 1, metrics.getBackupMasterNames().size());
  }

  @Test public void testUserMetrics() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    User userFoo = User.createUserForTesting(conf, "FOO_USER_METRIC_TEST", new String[0]);
    User userBar = User.createUserForTesting(conf, "BAR_USER_METRIC_TEST", new String[0]);
    User userTest = User.createUserForTesting(conf, "TEST_USER_METRIC_TEST", new String[0]);
    UTIL.createTable(TABLE_NAME, CF);
    waitForUsersMetrics(0);
    long writeMetaMetricBeforeNextuser = getMetaMetrics().getWriteRequestCount();
    userFoo.runAs(new PrivilegedAction<Void>() {
      @Override public Void run() {
        try {
          doPut();
        } catch (IOException e) {
          Assert.fail("Exception:" + e.getMessage());
        }
        return null;
      }
    });
    waitForUsersMetrics(1);
    long writeMetaMetricForUserFoo =
        getMetaMetrics().getWriteRequestCount() - writeMetaMetricBeforeNextuser;
    long readMetaMetricBeforeNextuser = getMetaMetrics().getReadRequestCount();
    userBar.runAs(new PrivilegedAction<Void>() {
      @Override public Void run() {
        try {
          doGet();
        } catch (IOException e) {
          Assert.fail("Exception:" + e.getMessage());
        }
        return null;
      }
    });
    waitForUsersMetrics(2);
    long readMetaMetricForUserBar =
        getMetaMetrics().getReadRequestCount() - readMetaMetricBeforeNextuser;
    long filteredMetaReqeust = getMetaMetrics().getFilteredReadRequestCount();
    userTest.runAs(new PrivilegedAction<Void>() {
      @Override public Void run() {
        try {
          Table table = createConnection(UTIL.getConfiguration()).getTable(TABLE_NAME);
          for (Result result : table.getScanner(new Scan().setFilter(new FilterAllFilter()))) {
            Assert.fail("Should have filtered all rows");
          }
        } catch (IOException e) {
          Assert.fail("Exception:" + e.getMessage());
        }
        return null;
      }
    });
    waitForUsersMetrics(3);
    long filteredMetaReqeustForTestUser =
        getMetaMetrics().getFilteredReadRequestCount() - filteredMetaReqeust;
    Map<byte[], UserMetrics> userMap =
        ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().values()
            .iterator().next().getUserMetrics();
    for (byte[] user : userMap.keySet()) {
      switch (Bytes.toString(user)) {
        case "FOO_USER_METRIC_TEST":
          Assert.assertEquals(1,
              userMap.get(user).getWriteRequestCount() - writeMetaMetricForUserFoo);
          break;
        case "BAR_USER_METRIC_TEST":
          Assert
              .assertEquals(1, userMap.get(user).getReadRequestCount() - readMetaMetricForUserBar);
          Assert.assertEquals(0, userMap.get(user).getWriteRequestCount());
          break;
        case "TEST_USER_METRIC_TEST":
          Assert.assertEquals(1,
              userMap.get(user).getFilteredReadRequests() - filteredMetaReqeustForTestUser);
          Assert.assertEquals(0, userMap.get(user).getWriteRequestCount());
          break;
        default:
          //current user
          Assert.assertEquals(UserProvider.instantiate(conf).getCurrent().getName(),
              Bytes.toString(user));
          //Read/write count because of Meta operations
          Assert.assertTrue(userMap.get(user).getReadRequestCount() > 1);
          break;
      }
    }
    UTIL.deleteTable(TABLE_NAME);
  }

  private RegionMetrics getMetaMetrics() throws IOException {
    for (ServerMetrics serverMetrics : ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .getLiveServerMetrics().values()) {
      for (RegionMetrics metrics : serverMetrics.getRegionMetrics().values()) {
        if (CatalogFamilyFormat.parseRegionInfoFromRegionName(metrics.getRegionName())
          .isMetaRegion()) {
          return metrics;
        }
      }
    }
    Assert.fail("Should have find meta metrics");
    return null;
  }

  private void waitForUsersMetrics(int noOfUsers) throws Exception {
    //Sleep for metrics to get updated on master
    Thread.sleep(5000);
    Waiter.waitFor(CLUSTER.getConfiguration(), 10 * 1000, 100, new Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        Map<byte[], UserMetrics> metrics =
            ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().values()
                .iterator().next().getUserMetrics();
        Assert.assertNotNull(metrics);
        //including current user + noOfUsers
        return metrics.keySet().size() > noOfUsers;
      }
    });
  }

  private void doPut() throws IOException {
    Table table = createConnection(UTIL.getConfiguration()).getTable(TABLE_NAME);
    table.put(new Put(Bytes.toBytes("a")).addColumn(CF, Bytes.toBytes("col1"), Bytes.toBytes("1")));

  }

  private void doGet() throws IOException {
    Table table = createConnection(UTIL.getConfiguration()).getTable(TABLE_NAME);
    table.get(new Get(Bytes.toBytes("a")).addColumn(CF, Bytes.toBytes("col1")));

  }

  private Connection createConnection(Configuration conf) throws IOException {
    User user = UserProvider.instantiate(conf).getCurrent();
    return ClusterConnectionFactory.createAsyncClusterConnection(conf, null, user).toConnection();
  }

  @Test
  public void testOtherStatusInfos() throws Exception {
    EnumSet<Option> options =
        EnumSet.of(Option.MASTER_COPROCESSORS, Option.HBASE_VERSION,
                   Option.CLUSTER_ID, Option.BALANCER_ON);
    ClusterMetrics metrics = ADMIN.getClusterMetrics(options);
    Assert.assertEquals(1, metrics.getMasterCoprocessorNames().size());
    Assert.assertNotNull(metrics.getHBaseVersion());
    Assert.assertNotNull(metrics.getClusterId());
    Assert.assertTrue(metrics.getAverageLoad() == 0.0);
    Assert.assertNotNull(metrics.getBalancerOn());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (ADMIN != null) {
      ADMIN.close();
    }
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testObserver() throws IOException {
    int preCount = MyObserver.PRE_COUNT.get();
    int postCount = MyObserver.POST_COUNT.get();
    Assert.assertTrue(ADMIN.getClusterMetrics().getMasterCoprocessorNames().stream()
        .anyMatch(s -> s.equals(MyObserver.class.getSimpleName())));
    Assert.assertEquals(preCount + 1, MyObserver.PRE_COUNT.get());
    Assert.assertEquals(postCount + 1, MyObserver.POST_COUNT.get());
  }

  private static void insertData(final TableName tableName, int startRow, int rowCount)
      throws IOException {
    Table t = UTIL.getConnection().getTable(tableName);
    Put p;
    for (int i = 0; i < rowCount; i++) {
      p = new Put(Bytes.toBytes("" + (startRow + i)));
      p.addColumn(CF, Bytes.toBytes("val1"), Bytes.toBytes(i));
      t.put(p);
    }
  }

  public static class MyObserver implements MasterCoprocessor, MasterObserver {
    private static final AtomicInteger PRE_COUNT = new AtomicInteger(0);
    private static final AtomicInteger POST_COUNT = new AtomicInteger(0);

    @Override public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override public void preGetClusterMetrics(ObserverContext<MasterCoprocessorEnvironment> ctx)
        throws IOException {
      PRE_COUNT.incrementAndGet();
    }

    @Override public void postGetClusterMetrics(ObserverContext<MasterCoprocessorEnvironment> ctx,
        ClusterMetrics metrics) throws IOException {
      POST_COUNT.incrementAndGet();
    }
  }
}
