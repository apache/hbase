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


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaMockingUtil;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import com.google.protobuf.ServiceException;

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
  private static final Log LOG = LogFactory.getLog(TestMasterNoCluster.class);
  private static final HBaseTestingUtility TESTUTIL = new HBaseTestingUtility();
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).withLookingForStuckThread(true).build();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TESTUTIL.getConfiguration();
    // We use local filesystem.  Set it so it writes into the testdir.
    FSUtils.setRootDir(c, TESTUTIL.getDataTestDir());
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
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TESTUTIL.getConfiguration(),
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
    ZKUtil.deleteNodeRecursively(zkw, zkw.baseZNode);
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
    CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(
      TESTUTIL.getConfiguration());
    HMaster master = new HMaster(TESTUTIL.getConfiguration(), cp);
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
   */
  @Test
  public void testFailover()
  throws IOException, KeeperException, InterruptedException, ServiceException {
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
    final TableName tableName = TableName.valueOf("t");
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
    CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(
      TESTUTIL.getConfiguration());
    // Insert a mock for the connection, use TESTUTIL.getConfiguration rather than
    // the conf from the master; the conf will already have an HConnection
    // associate so the below mocking of a connection will fail.
    final ClusterConnection mockedConnection = HConnectionTestingUtility.getMockedConnectionAndDecorate(
        TESTUTIL.getConfiguration(), rs0, rs0, rs0.getServerName(),
        HRegionInfo.FIRST_META_REGIONINFO);
    HMaster master = new HMaster(conf, cp) {
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
      void initClusterSchemaService() throws IOException, InterruptedException {}

      @Override
      ServerManager createServerManager(Server master, MasterServices services)
      throws IOException {
        ServerManager sm = super.createServerManager(master, services);
        // Spy on the created servermanager
        ServerManager spy = Mockito.spy(sm);
        // Fake a successful close.
        Mockito.doReturn(true).when(spy).
          sendRegionClose((ServerName)Mockito.any(), (HRegionInfo)Mockito.any(),
            (ServerName)Mockito.any());
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
        request.setLoad(ServerLoad.EMPTY_SERVERLOAD.obtainServerLoadPB());
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
  public void testNotPullingDeadRegionServerFromZK()
      throws IOException, KeeperException, InterruptedException {
    final Configuration conf = TESTUTIL.getConfiguration();
    final ServerName newServer = ServerName.valueOf("test.sample", 1, 101);
    final ServerName deadServer = ServerName.valueOf("test.sample", 1, 100);
    final MockRegionServer rs0 = new MockRegionServer(conf, newServer);

    CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(
      TESTUTIL.getConfiguration());
    HMaster master = new HMaster(conf, cp) {
      @Override
      void assignMeta(MonitoredTask status, Set<ServerName> previouslyFailedMeatRSs, int replicaId)
      { }

      @Override
      void initClusterSchemaService() throws IOException, InterruptedException {}

      @Override
      void initializeZKBasedSystemTrackers() throws IOException,
      InterruptedException, KeeperException, CoordinatedStateException {
        super.initializeZKBasedSystemTrackers();
        // Record a newer server in server manager at first
        getServerManager().recordNewServerWithLock(newServer, ServerLoad.EMPTY_SERVERLOAD);

        List<ServerName> onlineServers = new ArrayList<ServerName>();
        onlineServers.add(deadServer);
        onlineServers.add(newServer);
        // Mock the region server tracker to pull the dead server from zk
        regionServerTracker = Mockito.spy(regionServerTracker);
        Mockito.doReturn(onlineServers).when(
          regionServerTracker).getOnlineServers();
      }

      @Override
      public ClusterConnection getConnection() {
        // Insert a mock for the connection, use TESTUTIL.getConfiguration rather than
        // the conf from the master; the conf will already have an HConnection
        // associate so the below mocking of a connection will fail.
        try {
          return HConnectionTestingUtility.getMockedConnectionAndDecorate(
            TESTUTIL.getConfiguration(), rs0, rs0, rs0.getServerName(),
            HRegionInfo.FIRST_META_REGIONINFO);
        } catch (IOException e) {
          return null;
        }
      }
    };
    master.start();

    try {
      // Wait till master is initialized.
      while (!master.isInitialized()) Threads.sleep(10);
      LOG.info("Master is initialized");

      assertFalse("The dead server should not be pulled in",
        master.getServerManager().isServerOnline(deadServer));
    } finally {
      master.stopMaster();
      master.join();
    }
  }
}
