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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaMockingUtil;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.zookeeper.KeeperException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import com.google.protobuf.ServiceException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standup the master and fake it to test various aspects of master function.
 * Does NOT spin up a mini hbase nor mini dfs cluster testing master (it does
 * put up a zk cluster but this is usually pretty fast compared).  Also, should
 * be possible to inject faults at points difficult to get at in cluster context.
 * TODO: Speed up the zk connection by Master.  It pauses 5 seconds establishing
 * session.
 */
@Category(MediumTests.class)
public class TestMasterNoCluster {
  private static Logger LOG = LoggerFactory.getLogger(TestMasterNoCluster.class);
  private static final HBaseTestingUtility TESTUTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TESTUTIL.getConfiguration();
    // We use local filesystem.  Set it so it writes into the testdir.
    c.set(HConstants.HBASE_DIR, TESTUTIL.getDataTestDir().toString());
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
  @Test (timeout=30000)
  public void testStopDuringStart()
  throws IOException, KeeperException, InterruptedException {
    HMaster master = new HMaster(TESTUTIL.getConfiguration());
    master.start();
    // Immediately have it stop.  We used hang in assigning root.
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
    final ServerName sn0 = new ServerName("0.example.org", 0, now);
    final ServerName sn1 = new ServerName("1.example.org", 1, now);
    final ServerName sn2 = new ServerName("2.example.org", 2, now);
    final ServerName [] sns = new ServerName [] {sn0, sn1, sn2};
    // Put up the mock servers
    final Configuration conf = TESTUTIL.getConfiguration();
    final MockRegionServer rs0 = new MockRegionServer(conf, sn0);
    final MockRegionServer rs1 = new MockRegionServer(conf, sn1);
    final MockRegionServer rs2 = new MockRegionServer(conf, sn2);
    // Put some data into the servers.  Make it look like sn0 has the root
    // w/ an entry that points to sn1 as the host of .META.  Put data into sn2
    // so it looks like it has a few regions for a table named 't'.
    RootRegionTracker.setRootLocation(rs0.getZooKeeper(), rs0.getServerName());
    byte [] rootregion = Bytes.toBytes("-ROOT-,,0");
    rs0.setGetResult(rootregion, HRegionInfo.FIRST_META_REGIONINFO.getRegionName(),
      MetaMockingUtil.getMetaTableRowResult(HRegionInfo.FIRST_META_REGIONINFO,
        rs1.getServerName()));
    final byte [] tableName = Bytes.toBytes("t");
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
    HMaster master = new HMaster(conf) {
      InetAddress getRemoteInetAddress(final int port, final long serverStartCode)
      throws UnknownHostException {
        // Return different address dependent on port passed.
        ServerName sn = sns[port];
        return InetAddress.getByAddress(sn.getHostname(),
          new byte [] {10, 0, 0, (byte)sn.getPort()});
      }

      @Override
      ServerManager createServerManager(Server master, MasterServices services)
      throws IOException {
        ServerManager sm = super.createServerManager(master, services);
        // Spy on the created servermanager
        ServerManager spy = Mockito.spy(sm);
        // Fake a successful open.
        Mockito.doReturn(RegionOpeningState.OPENED).when(spy).
          sendRegionOpen((ServerName)Mockito.any(), (HRegionInfo)Mockito.any(),
            Mockito.anyInt());
        return spy;
      }

      @Override
      CatalogTracker createCatalogTracker(ZooKeeperWatcher zk,
          Configuration conf, Abortable abortable, int defaultTimeout)
      throws IOException {
        // Insert a mock for the connection used by the CatalogTracker.  Any
        // regionserver should do.  Use TESTUTIL.getConfiguration rather than
        // the conf from the master; the conf will already have an HConnection
        // associate so the below mocking of a connection will fail.
        HConnection connection =
          HConnectionTestingUtility.getMockedConnectionAndDecorate(TESTUTIL.getConfiguration(),
            rs0, rs0, rs0.getServerName(), HRegionInfo.ROOT_REGIONINFO);
        return new CatalogTracker(zk, conf, connection, abortable, defaultTimeout);
      }
    };
    master.start();

    try {
      // Wait till master is up ready for RPCs.
      while (!master.isRpcServerOpen()) Threads.sleep(10);
      // Fake master that there are regionservers out there.  Report in.
      for (int i = 0; i < sns.length; i++) {
        RegionServerReportRequest.Builder request = RegionServerReportRequest.newBuilder();;
        ServerName sn = ServerName.parseVersionedServerName(sns[i].getVersionedBytes());
        request.setServer(ProtobufUtil.toServerName(sn));
        request.setLoad(ServerLoad.EMPTY_SERVERLOAD.obtainServerLoadPB());
        master.regionServerReport(null, request.build());
      }
      // Master should now come up.
      while (!master.isInitialized()) {Threads.sleep(10);}
      assertTrue(master.isInitialized());
    } finally {
      rs0.stop("Test is done");
      rs1.stop("Test is done");
      rs2.stop("Test is done");
      master.stopMaster();
      master.join();
    }
  }

  /**
   * Test starting master getting it up post initialized state using mocks.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws DeserializationException 
   * @throws ServiceException
   */
  @Test
  public void testCatalogDeploys()
  throws IOException, KeeperException, InterruptedException, DeserializationException, ServiceException {
    final Configuration conf = TESTUTIL.getConfiguration();
    conf.setInt("hbase.master.wait.on.regionservers.mintostart", 1);
    conf.setInt("hbase.master.wait.on.regionservers.maxtostart", 1);

    final long now = System.currentTimeMillis();
    // Name for our single mocked up regionserver.
    final ServerName sn = new ServerName("0.example.org", 0, now);
    // Here is our mocked up regionserver.  Create it now.  Need it setting up
    // master next.
    final MockRegionServer rs0 = new MockRegionServer(conf, sn);

    // Create master.  Subclass to override a few methods so we can insert mocks
    // and get notification on transitions.  We need to fake out any rpcs the
    // master does opening/closing regions.  Also need to fake out the address
    // of the 'remote' mocked up regionservers.
    HMaster master = new HMaster(conf) {
      InetAddress getRemoteInetAddress(final int port, final long serverStartCode)
      throws UnknownHostException {
        // Interject an unchecked, nonsense InetAddress; i.e. no resolve.
        return InetAddress.getByAddress(rs0.getServerName().getHostname(),
          new byte [] {10, 0, 0, 0});
      }

      @Override
      ServerManager createServerManager(Server master, MasterServices services)
      throws IOException {
        ServerManager sm = super.createServerManager(master, services);
        // Spy on the created servermanager
        ServerManager spy = Mockito.spy(sm);
        // Fake a successful open.
        Mockito.doReturn(RegionOpeningState.OPENED).when(spy).
          sendRegionOpen((ServerName)Mockito.any(), (HRegionInfo)Mockito.any(),
            Mockito.anyInt());
        return spy;
      }

      @Override
      CatalogTracker createCatalogTracker(ZooKeeperWatcher zk,
          Configuration conf, Abortable abortable, int defaultTimeout)
      throws IOException {
        // Insert a mock for the connection used by the CatalogTracker.   Use
        // TESTUTIL.getConfiguration rather than the conf from the master; the
        // conf will already have an HConnection associate so the below mocking
        // of a connection will fail.
        HConnection connection =
          HConnectionTestingUtility.getMockedConnectionAndDecorate(TESTUTIL.getConfiguration(),
            rs0, rs0, rs0.getServerName(), HRegionInfo.ROOT_REGIONINFO);
        return new CatalogTracker(zk, conf, connection, abortable, defaultTimeout);
      }
    };
    master.start();
    LOG.info("Master has started");

    try {
      // Wait till master is up ready for RPCs.
      while (!master.isRpcServerOpen()) Threads.sleep(10);
      LOG.info("RpcServerOpen has started");

      // Fake master that there is a regionserver out there.  Report in.
      RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
      request.setPort(rs0.getServerName().getPort());
      request.setServerStartCode(rs0.getServerName().getStartcode());
      request.setServerCurrentTime(now);
      RegionServerStartupResponse result =
        master.regionServerStartup(null, request.build());
      String rshostname = new String();
      for (NameStringPair e : result.getMapEntriesList()) {
        if (e.getName().toString().equals(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
          rshostname = e.getValue();
        }
      }
      // Assert hostname is as expected.
      assertEquals(rs0.getServerName().getHostname(), rshostname);
      // Now master knows there is at least one regionserver checked in and so
      // it'll wait a while to see if more and when none, will assign root and
      // meta to this single server.  Will do an rpc open but we've
      // mocked it above in our master override to return 'success'.  As part of
      // region open, master will have set an unassigned znode for the region up
      // into zk for the regionserver to transition.  Lets do that now to
      // complete fake of a successful open.
      Mocking.fakeRegionServerRegionOpenInZK(master, rs0.getZooKeeper(),
        rs0.getServerName(), HRegionInfo.ROOT_REGIONINFO);
      LOG.info("fakeRegionServerRegionOpenInZK has started");


      // Need to set root location as r1.  Usually the regionserver does this
      // when its figured it just opened the root region by setting the root
      // location up into zk.  Since we're mocking regionserver, need to do this
      // ourselves.
      RootRegionTracker.setRootLocation(rs0.getZooKeeper(), rs0.getServerName());
      // Do same transitions for .META. (presuming master has by now assigned
      // .META. to rs1).
      Mocking.fakeRegionServerRegionOpenInZK(master, rs0.getZooKeeper(),
        rs0.getServerName(), HRegionInfo.FIRST_META_REGIONINFO);
      // Now trigger our mock regionserver to start returning a row when we
      // go to get .META. entry in -ROOT-.  We do it by setting into
      // our MockRegionServer some data to be returned when there is a get on
      // -ROOT- table (up to this its been returning null making master think
      // nothing assigned, not even .META.). The region for -ROOT- table we
      // hardcode below.  Its always the same, at least in tests.  We need to do
      // this because CatalogTracker runs inside in Master initialization to
      // confirm .META. has a server.
      byte [] rootregion = Bytes.toBytes("-ROOT-,,0");
      rs0.setGetResult(rootregion, HRegionInfo.FIRST_META_REGIONINFO.getRegionName(),
        MetaMockingUtil.getMetaTableRowResult(HRegionInfo.FIRST_META_REGIONINFO,
          rs0.getServerName()));
      // Master should now come up.
      while (!master.isInitialized()) {Threads.sleep(10);}
      assertTrue(master.isInitialized());
    } finally {
      rs0.stop("Test is done");
      master.stopMaster();
      master.join();
    }
  }

}
