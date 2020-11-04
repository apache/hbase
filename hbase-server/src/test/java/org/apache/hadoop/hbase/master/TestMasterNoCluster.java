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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_QUORUM;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaMockingUtil;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;

/**
 * Standup the master and fake it to test various aspects of master function.
 * Does NOT spin up a mini hbase nor mini dfs cluster testing master (it does
 * put up a zk cluster but this is usually pretty fast compared).  Also, should
 * be possible to inject faults at points difficult to get at in cluster context.
 * TODO: Speed up the zk connection by Master.  It pauses 5 seconds establishing
 * session.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestMasterNoCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterNoCluster.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterNoCluster.class);
  private static final HBaseTestingUtility TESTUTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TESTUTIL.getConfiguration();
    // We use local filesystem.  Set it so it writes into the testdir.
    CommonFSUtils.setRootDir(c, TESTUTIL.getDataTestDir());
    DefaultMetricsSystem.setMiniClusterMode(true);
    // Startup a mini zk cluster.
    TESTUTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TESTUTIL.shutdownMiniZKCluster();
  }

  @After
  public void tearDown()
  throws KeeperException, ZooKeeperConnectionException, IOException {
    // Make sure zk is clean before we run the next test.
    ZKWatcher zkw = new ZKWatcher(TESTUTIL.getConfiguration(),
        "@Before", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    });
    // Before fails sometimes so retry.
    try {
      TESTUTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          try {
            ZKUtil.deleteNodeRecursively(zkw, zkw.getZNodePaths().baseZNode);
            return true;
          } catch (KeeperException.NotEmptyException e) {
            LOG.info("Failed delete, retrying", e);
          }
          return false;
        }
      });
    } catch (Exception e) {
      LOG.info("Failed zk clear", e);
    }
    zkw.close();
  }

  /**
   * Test starting master then stopping it before its fully up.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Test
  public void testStopDuringStart()
  throws IOException, KeeperException, InterruptedException {
    HMaster master = new HMaster(TESTUTIL.getConfiguration());
    master.start();
    // Immediately have it stop.  We used hang in assigning meta.
    master.stopMaster();
    master.join();
  }

  /**
   * Test master failover.
   * Start up three fake regionservers and a master.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws org.apache.hbase.thirdparty.com.google.protobuf.ServiceException
   */
  @Ignore @Test // Disabled since HBASE-18511. Reenable when master can carry regions.
  public void testFailover() throws Exception {
    final long now = System.currentTimeMillis();
    // Names for our three servers.  Make the port numbers match hostname.
    // Will come in use down in the server when we need to figure how to respond.
    final ServerName sn0 = ServerName.valueOf("0.example.org", 0, now);
    final ServerName sn1 = ServerName.valueOf("1.example.org", 1, now);
    final ServerName sn2 = ServerName.valueOf("2.example.org", 2, now);
    final ServerName [] sns = new ServerName [] {sn0, sn1, sn2};
    // Put up the mock servers
    final Configuration conf = TESTUTIL.getConfiguration();
    final MockRegionServer rs0 = new MockRegionServer(conf, sn0);
    final MockRegionServer rs1 = new MockRegionServer(conf, sn1);
    final MockRegionServer rs2 = new MockRegionServer(conf, sn2);
    // Put some data into the servers.  Make it look like sn0 has the metaH
    // Put data into sn2 so it looks like it has a few regions for a table named 't'.
    MetaTableLocator.setMetaLocation(rs0.getZooKeeper(),
      rs0.getServerName(), RegionState.State.OPEN);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Result [] results = new Result [] {
      MetaMockingUtil.getMetaTableRowResult(
        new HRegionInfo(tableName, HConstants.EMPTY_START_ROW, HBaseTestingUtility.KEYS[1]),
        rs2.getServerName()),
      MetaMockingUtil.getMetaTableRowResult(
        new HRegionInfo(tableName, HBaseTestingUtility.KEYS[1], HBaseTestingUtility.KEYS[2]),
        rs2.getServerName()),
      MetaMockingUtil.getMetaTableRowResult(new HRegionInfo(tableName, HBaseTestingUtility.KEYS[2],
          HConstants.EMPTY_END_ROW),
        rs2.getServerName())
    };
    rs1.setNextResults(HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), results);

    // Create master.  Subclass to override a few methods so we can insert mocks
    // and get notification on transitions.  We need to fake out any rpcs the
    // master does opening/closing regions.  Also need to fake out the address
    // of the 'remote' mocked up regionservers.
    // Insert a mock for the connection, use TESTUTIL.getConfiguration rather than
    // the conf from the master; the conf will already have an ClusterConnection
    // associate so the below mocking of a connection will fail.
    final ClusterConnection mockedConnection = HConnectionTestingUtility.getMockedConnectionAndDecorate(
        TESTUTIL.getConfiguration(), rs0, rs0, rs0.getServerName(),
        HRegionInfo.FIRST_META_REGIONINFO);
    HMaster master = new HMaster(conf) {
      @Override
      InetAddress getRemoteInetAddress(final int port, final long serverStartCode)
      throws UnknownHostException {
        // Return different address dependent on port passed.
        if (port > sns.length) {
          return super.getRemoteInetAddress(port, serverStartCode);
        }
        ServerName sn = sns[port];
        return InetAddress.getByAddress(sn.getHostname(),
          new byte [] {10, 0, 0, (byte)sn.getPort()});
      }

      @Override
      protected void initClusterSchemaService() throws IOException, InterruptedException {}

      @Override
      protected ServerManager createServerManager(MasterServices master) throws IOException {
        ServerManager sm = super.createServerManager(master);
        // Spy on the created servermanager
        ServerManager spy = Mockito.spy(sm);
        return spy;
      }

      @Override
      public ClusterConnection getConnection() {
        return mockedConnection;
      }

      @Override
      public ClusterConnection getClusterConnection() {
        return mockedConnection;
      }
    };
    master.start();

    try {
      // Wait till master is up ready for RPCs.
      while (!master.serviceStarted) Threads.sleep(10);
      // Fake master that there are regionservers out there.  Report in.
      for (int i = 0; i < sns.length; i++) {
        RegionServerReportRequest.Builder request = RegionServerReportRequest.newBuilder();;
        ServerName sn = ServerName.parseVersionedServerName(sns[i].getVersionedBytes());
        request.setServer(ProtobufUtil.toServerName(sn));
        request.setLoad(ServerMetricsBuilder.toServerLoad(ServerMetricsBuilder.of(sn)));
        master.getMasterRpcServices().regionServerReport(null, request.build());
      }
       // Master should now come up.
      while (!master.isInitialized()) {
        Threads.sleep(100);
      }
      assertTrue(master.isInitialized());
    } finally {
      rs0.stop("Test is done");
      rs1.stop("Test is done");
      rs2.stop("Test is done");
      master.stopMaster();
      master.join();
    }
  }

  @Test
  public void testMasterInitWithSameClientServerZKQuorum() throws Exception {
    Configuration conf = new Configuration(TESTUTIL.getConfiguration());
    conf.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, conf.get(ZOOKEEPER_QUORUM));
    conf.setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, TESTUTIL.getZkCluster().getClientPort());
    HMaster master = new HMaster(conf);
    master.start();
    // the master will abort due to IllegalArgumentException so we should finish within 60 seconds
    master.join();
  }

  @Test
  public void testMasterInitWithObserverModeClientZKQuorum() throws Exception {
    Configuration conf = new Configuration(TESTUTIL.getConfiguration());
    Assert.assertFalse(Boolean.getBoolean(HConstants.CLIENT_ZOOKEEPER_OBSERVER_MODE));
    // set client ZK to some non-existing address and make sure server won't access client ZK
    // (server start should not be affected)
    conf.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
    conf.setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT,
      TESTUTIL.getZkCluster().getClientPort() + 1);
    // settings to allow us not to start additional RS
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    conf.setBoolean(LoadBalancer.TABLES_ON_MASTER, true);
    // main setting for this test case
    conf.setBoolean(HConstants.CLIENT_ZOOKEEPER_OBSERVER_MODE, true);
    HMaster master = new HMaster(conf);
    master.start();
    while (!master.isInitialized()) {
      Threads.sleep(200);
    }
    Assert.assertNull(master.getMetaLocationSyncer());
    Assert.assertNull(master.masterAddressSyncer);
    master.stopMaster();
    master.join();
  }
}
