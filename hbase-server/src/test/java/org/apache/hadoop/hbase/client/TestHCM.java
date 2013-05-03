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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.exceptions.RegionServerStoppedException;
import org.apache.hadoop.hbase.exceptions.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.master.ClusterStatusPublisher;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * This class is for testing HCM features
 */
@Category(MediumTests.class)
public class TestHCM {
  private static final Log LOG = LogFactory.getLog(TestHCM.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TABLE_NAME = Bytes.toBytes("test");
  private static final byte[] TABLE_NAME1 = Bytes.toBytes("test1");
  private static final byte[] TABLE_NAME2 = Bytes.toBytes("test2");
  private static final byte[] TABLE_NAME3 = Bytes.toBytes("test3");
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbb");
  private static final byte[] ROW_X = Bytes.toBytes("xxx");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setClass(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS,
        ClusterStatusPublisher.MulticastPublisher.class, ClusterStatusPublisher.Publisher.class);
    TEST_UTIL.getConfiguration().setClass(ClusterStatusListener.STATUS_LISTENER_CLASS,
        ClusterStatusListener.MultiCastListener.class, ClusterStatusListener.Listener.class);
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static Random _randy = new Random();

  public static void createNewConfigurations() throws SecurityException,
  IllegalArgumentException, NoSuchFieldException,
  IllegalAccessException, InterruptedException, ZooKeeperConnectionException, IOException {
    HConnection last = null;
    for (int i = 0; i <= (HConnectionManager.MAX_CACHED_CONNECTION_INSTANCES * 2); i++) {
      // set random key to differentiate the connection from previous ones
      Configuration configuration = HBaseConfiguration.create();
      configuration.set("somekey", String.valueOf(_randy.nextInt()));
      System.out.println("Hash Code: " + configuration.hashCode());
      HConnection connection = HConnectionManager.getConnection(configuration);
      if (last != null) {
        if (last == connection) {
          System.out.println("!! Got same connection for once !!");
        }
      }
      // change the configuration once, and the cached connection is lost forever:
      //      the hashtable holding the cache won't be able to find its own keys
      //      to remove them, so the LRU strategy does not work.
      configuration.set("someotherkey", String.valueOf(_randy.nextInt()));
      last = connection;
      LOG.info("Cache Size: " + getHConnectionManagerCacheSize());
      Thread.sleep(100);
    }
    Assert.assertEquals(1,
      getHConnectionManagerCacheSize());
  }

  private static int getHConnectionManagerCacheSize(){
    return HConnectionTestingUtility.getConnectionCount();
  }

  @Test(expected = RegionServerStoppedException.class)
  public void testClusterStatus() throws Exception {
    byte[] tn = "testClusterStatus".getBytes();
    byte[] cf = "cf".getBytes();
    byte[] rk = "rk1".getBytes();

    JVMClusterUtil.RegionServerThread rs = TEST_UTIL.getHBaseCluster().startRegionServer();
    rs.waitForServerOnline();
    final ServerName sn = rs.getRegionServer().getServerName();

    HTable t = TEST_UTIL.createTable(tn, cf);
    TEST_UTIL.waitTableAvailable(tn);

    while(TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().
        getRegionStates().isRegionsInTransition()){
      Thread.sleep(1);
    }
    final HConnectionImplementation hci =  (HConnectionImplementation)t.getConnection();
    while (t.getRegionLocation(rk).getPort() != sn.getPort()){
      TEST_UTIL.getHBaseAdmin().move(t.getRegionLocation(rk).getRegionInfo().
          getEncodedNameAsBytes(), sn.getVersionedBytes());
      while(TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().
          getRegionStates().isRegionsInTransition()){
        Thread.sleep(1);
      }
      hci.clearRegionCache(tn);
    }
    Assert.assertNotNull(hci.clusterStatusListener);
    TEST_UTIL.assertRegionOnServer(t.getRegionLocation(rk).getRegionInfo(), sn, 20000);

    Put p1 = new Put(rk);
    p1.add(cf, "qual".getBytes(), "val".getBytes());
    t.put(p1);

    rs.getRegionServer().abort("I'm dead");

    // We want the status to be updated. That's a least 10 second
    TEST_UTIL.waitFor(40000, 1000, true, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getHBaseCluster().getMaster().getServerManager().
            getDeadServers().isDeadServer(sn);
      }
    });

    TEST_UTIL.waitFor(40000, 1000, true, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return hci.clusterStatusListener.isDeadServer(sn);
      }
    });

    hci.getClient(sn);  // will throw an exception: RegionServerStoppedException
  }

  @Test
  public void abortingHConnectionRemovesItselfFromHCM() throws Exception {
    // Save off current HConnections
    Map<HConnectionKey, HConnectionImplementation> oldHBaseInstances =
        new HashMap<HConnectionKey, HConnectionImplementation>();
    oldHBaseInstances.putAll(HConnectionManager.CONNECTION_INSTANCES);

    HConnectionManager.CONNECTION_INSTANCES.clear();

    try {
      HConnection connection = HConnectionManager.getConnection(TEST_UTIL.getConfiguration());
      connection.abort("test abortingHConnectionRemovesItselfFromHCM", new Exception(
          "test abortingHConnectionRemovesItselfFromHCM"));
      Assert.assertNotSame(connection,
        HConnectionManager.getConnection(TEST_UTIL.getConfiguration()));
    } finally {
      // Put original HConnections back
      HConnectionManager.CONNECTION_INSTANCES.clear();
      HConnectionManager.CONNECTION_INSTANCES.putAll(oldHBaseInstances);
    }
  }

  /**
   * Test that when we delete a location using the first row of a region
   * that we really delete it.
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testRegionCaching() throws Exception{
    HTable table = TEST_UTIL.createTable(TABLE_NAME, FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    HConnectionManager.HConnectionImplementation conn =
      (HConnectionManager.HConnectionImplementation)table.getConnection();

    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    assertNotNull(conn.getCachedLocation(TABLE_NAME.clone(), ROW.clone()));
    assertNotNull(conn.getCachedLocation(
      Bytes.toString(TABLE_NAME).getBytes() , Bytes.toString(ROW).getBytes()));

    final int nextPort = conn.getCachedLocation(TABLE_NAME, ROW).getPort() + 1;
    HRegionLocation loc = conn.getCachedLocation(TABLE_NAME, ROW);
    conn.updateCachedLocation(loc.getRegionInfo(), loc, new ServerName("127.0.0.1", nextPort,
      HConstants.LATEST_TIMESTAMP), HConstants.LATEST_TIMESTAMP);
    Assert.assertEquals(conn.getCachedLocation(TABLE_NAME, ROW).getPort(), nextPort);

    conn.forceDeleteCachedLocation(TABLE_NAME.clone(), ROW.clone());
    HRegionLocation rl = conn.getCachedLocation(TABLE_NAME, ROW);
    assertNull("What is this location?? " + rl, rl);

    // We're now going to move the region and check that it works for the client
    // First a new put to add the location in the cache
    conn.clearRegionCache(TABLE_NAME);
    Assert.assertEquals(0, conn.getNumberOfCachedRegionLocations(TABLE_NAME));
    Put put2 = new Put(ROW);
    put2.add(FAM_NAM, ROW, ROW);
    table.put(put2);
    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));

    TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, false);
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();

    // We can wait for all regions to be online, that makes log reading easier when debugging
    while (master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
      Thread.sleep(1);
    }

    // Now moving the region to the second server
    HRegionLocation toMove = conn.getCachedLocation(TABLE_NAME, ROW);
    byte[] regionName = toMove.getRegionInfo().getRegionName();
    byte[] encodedRegionNameBytes = toMove.getRegionInfo().getEncodedNameAsBytes();

    // Choose the other server.
    int curServerId = TEST_UTIL.getHBaseCluster().getServerWith(regionName);
    int destServerId = (curServerId == 0 ? 1 : 0);

    HRegionServer curServer = TEST_UTIL.getHBaseCluster().getRegionServer(curServerId);
    HRegionServer destServer = TEST_UTIL.getHBaseCluster().getRegionServer(destServerId);

    ServerName destServerName = destServer.getServerName();

    // Check that we are in the expected state
    Assert.assertTrue(curServer != destServer);
    Assert.assertFalse(curServer.getServerName().equals(destServer.getServerName()));
    Assert.assertFalse( toMove.getPort() == destServerName.getPort());
    Assert.assertNotNull(curServer.getOnlineRegion(regionName));
    Assert.assertNull(destServer.getOnlineRegion(regionName));
    Assert.assertFalse(TEST_UTIL.getMiniHBaseCluster().getMaster().
        getAssignmentManager().getRegionStates().isRegionsInTransition());

    // Moving. It's possible that we don't have all the regions online at this point, so
    //  the test must depends only on the region we're looking at.
    LOG.info("Move starting region="+toMove.getRegionInfo().getRegionNameAsString());
    TEST_UTIL.getHBaseAdmin().move(
      toMove.getRegionInfo().getEncodedNameAsBytes(),
      destServerName.getServerName().getBytes()
    );

    while (destServer.getOnlineRegion(regionName) == null ||
        destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
      // wait for the move to be finished
      Thread.sleep(1);
    }

    LOG.info("Move finished for region="+toMove.getRegionInfo().getRegionNameAsString());

    // Check our new state.
    Assert.assertNull(curServer.getOnlineRegion(regionName));
    Assert.assertNotNull(destServer.getOnlineRegion(regionName));
    Assert.assertFalse(destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));
    Assert.assertFalse(curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));


    // Cache was NOT updated and points to the wrong server
    Assert.assertFalse(
        conn.getCachedLocation(TABLE_NAME, ROW).getPort() == destServerName.getPort());

    // Hijack the number of retry to fail immediately instead of retrying: there will be no new
    //  connection to the master
    Field numTries = conn.getClass().getDeclaredField("numTries");
    numTries.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(numTries, numTries.getModifiers() & ~Modifier.FINAL);
    final int prevNumRetriesVal = (Integer)numTries.get(conn);
    numTries.set(conn, 1);

    // We do a put and expect the cache to be updated, even if we don't retry
    LOG.info("Put starting");
    Put put3 = new Put(ROW);
    put3.add(FAM_NAM, ROW, ROW);
    try {
      table.put(put3);
      Assert.assertFalse("Unreachable point", true);
    }catch (Throwable e){
      LOG.info("Put done, exception caught: "+e.getClass());
      // Now check that we have the exception we wanted
      Assert.assertTrue(e instanceof RetriesExhaustedWithDetailsException);
      RetriesExhaustedWithDetailsException re = (RetriesExhaustedWithDetailsException)e;
      Assert.assertTrue(re.getNumExceptions() == 1);
      Assert.assertTrue(Arrays.equals(re.getRow(0).getRow(), ROW));
    }
    Assert.assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    Assert.assertEquals(
      "Previous server was "+curServer.getServerName().getHostAndPort(),
      destServerName.getPort(), conn.getCachedLocation(TABLE_NAME, ROW).getPort());

    Assert.assertFalse(destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));
    Assert.assertFalse(curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));

    // We move it back to do another test with a scan
    LOG.info("Move starting region=" + toMove.getRegionInfo().getRegionNameAsString());
    TEST_UTIL.getHBaseAdmin().move(
      toMove.getRegionInfo().getEncodedNameAsBytes(),
      curServer.getServerName().getServerName().getBytes()
    );

    while (curServer.getOnlineRegion(regionName) == null ||
        destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
      // wait for the move to be finished
      Thread.sleep(1);
    }

    // Check our new state.
    Assert.assertNotNull(curServer.getOnlineRegion(regionName));
    Assert.assertNull(destServer.getOnlineRegion(regionName));
    LOG.info("Move finished for region=" + toMove.getRegionInfo().getRegionNameAsString());

    // Cache was NOT updated and points to the wrong server
    Assert.assertFalse(conn.getCachedLocation(TABLE_NAME, ROW).getPort() ==
      curServer.getServerName().getPort());


    Scan sc = new Scan();
    sc.setStopRow(ROW);
    sc.setStopRow(ROW);

    try {
      ResultScanner rs = table.getScanner(sc);
      while (rs.next() != null) {
      }
      Assert.assertFalse("Unreachable point", true);
    } catch (Throwable e) {
      LOG.info("Scan done, expected exception caught: " + e.getClass());
    }

    // Cache is updated with the right value.
    Assert.assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    Assert.assertEquals(
      "Previous server was "+destServer.getServerName().getHostAndPort(),
      curServer.getServerName().getPort(), conn.getCachedLocation(TABLE_NAME, ROW).getPort());

    numTries.set(conn, prevNumRetriesVal);
    table.close();
  }

  /**
   * Test that Connection or Pool are not closed when managed externally
   * @throws Exception
   */
  @Test
  public void testConnectionManagement() throws Exception{
    TEST_UTIL.createTable(TABLE_NAME1, FAM_NAM);
    HConnection conn = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 10,
        60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        Threads.newDaemonThreadFactory("test-hcm"));

    HTable table = new HTable(TABLE_NAME1, conn, pool);
    table.close();
    assertFalse(conn.isClosed());
    assertFalse(pool.isShutdown());
    table = new HTable(TEST_UTIL.getConfiguration(), TABLE_NAME1, pool);
    table.close();
    assertFalse(pool.isShutdown());
    conn.close();
    pool.shutdownNow();
  }

  /**
   * Test that stale cache updates don't override newer cached values.
   */
  @Test(timeout = 60000)
  public void testCacheSeqNums() throws Exception{
    HTable table = TEST_UTIL.createTable(TABLE_NAME2, FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    HConnectionManager.HConnectionImplementation conn =
      (HConnectionManager.HConnectionImplementation)table.getConnection();

    HRegionLocation location = conn.getCachedLocation(TABLE_NAME2, ROW);
    assertNotNull(location);

    HRegionLocation anySource = new HRegionLocation(location.getRegionInfo(), new ServerName(
        location.getHostname(), location.getPort() - 1, 0L));

    // Same server as already in cache reporting - overwrites any value despite seqNum.
    int nextPort = location.getPort() + 1;
    conn.updateCachedLocation(location.getRegionInfo(), location,
        new ServerName("127.0.0.1", nextPort, 0), location.getSeqNum() - 1);
    location = conn.getCachedLocation(TABLE_NAME2, ROW);
    Assert.assertEquals(nextPort, location.getPort());

    // No source specified - same.
    nextPort = location.getPort() + 1;
    conn.updateCachedLocation(location.getRegionInfo(), location,
        new ServerName("127.0.0.1", nextPort, 0), location.getSeqNum() - 1);
    location = conn.getCachedLocation(TABLE_NAME2, ROW);
    Assert.assertEquals(nextPort, location.getPort());

    // Higher seqNum - overwrites lower seqNum.
    nextPort = location.getPort() + 1;
    conn.updateCachedLocation(location.getRegionInfo(), anySource,
        new ServerName("127.0.0.1", nextPort, 0), location.getSeqNum() + 1);
    location = conn.getCachedLocation(TABLE_NAME2, ROW);
    Assert.assertEquals(nextPort, location.getPort());

    // Lower seqNum - does not overwrite higher seqNum.
    nextPort = location.getPort() + 1;
    conn.updateCachedLocation(location.getRegionInfo(), anySource,
        new ServerName("127.0.0.1", nextPort, 0), location.getSeqNum() - 1);
    location = conn.getCachedLocation(TABLE_NAME2, ROW);
    Assert.assertEquals(nextPort - 1, location.getPort());
  }

  /**
   * Make sure that {@link Configuration} instances that are essentially the
   * same map to the same {@link HConnection} instance.
   */
  @Test
  public void testConnectionSameness() throws Exception {
    HConnection previousConnection = null;
    for (int i = 0; i < 2; i++) {
      // set random key to differentiate the connection from previous ones
      Configuration configuration = TEST_UTIL.getConfiguration();
      configuration.set("some_key", String.valueOf(_randy.nextInt()));
      LOG.info("The hash code of the current configuration is: "
          + configuration.hashCode());
      HConnection currentConnection = HConnectionManager
          .getConnection(configuration);
      if (previousConnection != null) {
        assertTrue(
            "Did not get the same connection even though its key didn't change",
            previousConnection == currentConnection);
      }
      previousConnection = currentConnection;
      // change the configuration, so that it is no longer reachable from the
      // client's perspective. However, since its part of the LRU doubly linked
      // list, it will eventually get thrown out, at which time it should also
      // close the corresponding {@link HConnection}.
      configuration.set("other_key", String.valueOf(_randy.nextInt()));
    }
  }

  /**
   * Makes sure that there is no leaking of
   * {@link HConnectionManager.HConnectionImplementation} in the {@link HConnectionManager}
   * class.
   */
  @Test
  public void testConnectionUniqueness() throws Exception {
    int zkmaxconnections = TEST_UTIL.getConfiguration().
      getInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS,
        HConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS);
    // Test up to a max that is < the maximum number of zk connections.  If we
    // go above zk connections, we just fall into cycle where we are failing
    // to set up a session and test runs for a long time.
    int maxConnections = Math.min(zkmaxconnections - 1, 20);
    List<HConnection> connections = new ArrayList<HConnection>(maxConnections);
    HConnection previousConnection = null;
    try {
      for (int i = 0; i < maxConnections; i++) {
        // set random key to differentiate the connection from previous ones
        Configuration configuration = new Configuration(TEST_UTIL.getConfiguration());
        configuration.set("some_key", String.valueOf(_randy.nextInt()));
        configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
            String.valueOf(_randy.nextInt()));
        LOG.info("The hash code of the current configuration is: "
            + configuration.hashCode());
        HConnection currentConnection =
          HConnectionManager.getConnection(configuration);
        if (previousConnection != null) {
          assertTrue("Got the same connection even though its key changed!",
              previousConnection != currentConnection);
        }
        // change the configuration, so that it is no longer reachable from the
        // client's perspective. However, since its part of the LRU doubly linked
        // list, it will eventually get thrown out, at which time it should also
        // close the corresponding {@link HConnection}.
        configuration.set("other_key", String.valueOf(_randy.nextInt()));

        previousConnection = currentConnection;
        LOG.info("The current HConnectionManager#HBASE_INSTANCES cache size is: "
            + getHConnectionManagerCacheSize());
        Thread.sleep(50);
        connections.add(currentConnection);
      }
    } finally {
      for (HConnection c: connections) {
        // Clean up connections made so we don't interfere w/ subsequent tests.
        HConnectionManager.deleteConnection(c.getConfiguration());
      }
    }
  }

  @Test
  public void testClosing() throws Exception {
    Configuration configuration =
      new Configuration(TEST_UTIL.getConfiguration());
    configuration.set(HConstants.HBASE_CLIENT_INSTANCE_ID,
        String.valueOf(_randy.nextInt()));

    HConnection c1 = HConnectionManager.createConnection(configuration);
    // We create two connections with the same key.
    HConnection c2 = HConnectionManager.createConnection(configuration);

    HConnection c3 = HConnectionManager.getConnection(configuration);
    HConnection c4 = HConnectionManager.getConnection(configuration);
    assertTrue(c3 == c4);

    c1.close();
    assertTrue(c1.isClosed());
    assertFalse(c2.isClosed());
    assertFalse(c3.isClosed());

    c3.close();
    // still a reference left
    assertFalse(c3.isClosed());
    c3.close();
    assertTrue(c3.isClosed());
    // c3 was removed from the cache
    HConnection c5 = HConnectionManager.getConnection(configuration);
    assertTrue(c5 != c3);

    assertFalse(c2.isClosed());
    c2.close();
    assertTrue(c2.isClosed());
    c5.close();
    assertTrue(c5.isClosed());
  }

  /**
   * Trivial test to verify that nobody messes with
   * {@link HConnectionManager#createConnection(Configuration)}
   */
  @Test
  public void testCreateConnection() throws Exception {
    Configuration configuration = TEST_UTIL.getConfiguration();
    HConnection c1 = HConnectionManager.createConnection(configuration);
    HConnection c2 = HConnectionManager.createConnection(configuration);
    // created from the same configuration, yet they are different
    assertTrue(c1 != c2);
    assertTrue(c1.getConfiguration() == c2.getConfiguration());
    // make sure these were not cached
    HConnection c3 = HConnectionManager.getConnection(configuration);
    assertTrue(c1 != c3);
    assertTrue(c2 != c3);
  }


  /**
   * This test checks that one can connect to the cluster with only the
   *  ZooKeeper quorum set. Other stuff like master address will be read
   *  from ZK by the client.
   */
  @Test(timeout = 60000)
  public void testConnection() throws Exception{
    // We create an empty config and add the ZK address.
    Configuration c = new Configuration();
    c.set(HConstants.ZOOKEEPER_QUORUM,
      TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));
    c.set(HConstants.ZOOKEEPER_CLIENT_PORT ,
      TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT));

    // This should be enough to connect
    HConnection conn = HConnectionManager.getConnection(c);
    assertTrue( conn.isMasterRunning() );
    conn.close();
  }

  @Test
  public void testMulti() throws Exception {
    HTable table = TEST_UTIL.createTable(TABLE_NAME3, FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);
    HConnectionManager.HConnectionImplementation conn =
      (HConnectionManager.HConnectionImplementation)
        HConnectionManager.getConnection(TEST_UTIL.getConfiguration());

    // We're now going to move the region and check that it works for the client
    // First a new put to add the location in the cache
    conn.clearRegionCache(TABLE_NAME3);
    Assert.assertEquals(0, conn.getNumberOfCachedRegionLocations(TABLE_NAME3));

    TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, false);
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();

    // We can wait for all regions to be online, that makes log reading easier when debugging
    while (master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
      Thread.sleep(1);
    }

    Put put = new Put(ROW_X);
    put.add(FAM_NAM, ROW_X, ROW_X);
    table.put(put);

    // Now moving the region to the second server
    HRegionLocation toMove = conn.getCachedLocation(TABLE_NAME3, ROW_X);
    byte[] regionName = toMove.getRegionInfo().getRegionName();
    byte[] encodedRegionNameBytes = toMove.getRegionInfo().getEncodedNameAsBytes();

    // Choose the other server.
    int curServerId = TEST_UTIL.getHBaseCluster().getServerWith(regionName);
    int destServerId = (curServerId == 0 ? 1 : 0);

    HRegionServer curServer = TEST_UTIL.getHBaseCluster().getRegionServer(curServerId);
    HRegionServer destServer = TEST_UTIL.getHBaseCluster().getRegionServer(destServerId);

    ServerName destServerName = destServer.getServerName();

    //find another row in the cur server that is less than ROW_X
    List<HRegion> regions = curServer.getOnlineRegions(TABLE_NAME3);
    byte[] otherRow = null;
    for (HRegion region : regions) {
      if (!region.getRegionInfo().getEncodedName().equals(toMove.getRegionInfo().getEncodedName())
          && Bytes.BYTES_COMPARATOR.compare(region.getRegionInfo().getStartKey(), ROW_X) < 0) {
        otherRow = region.getRegionInfo().getStartKey();
        break;
      }
    }
    assertNotNull(otherRow);
    // If empty row, set it to first row.-f
    if (otherRow.length <= 0) otherRow = Bytes.toBytes("aaa");
    Put put2 = new Put(otherRow);
    put2.add(FAM_NAM, otherRow, otherRow);
    table.put(put2); //cache put2's location

    // Check that we are in the expected state
    Assert.assertTrue(curServer != destServer);
    Assert.assertNotEquals(curServer.getServerName(), destServer.getServerName());
    Assert.assertNotEquals(toMove.getPort(), destServerName.getPort());
    Assert.assertNotNull(curServer.getOnlineRegion(regionName));
    Assert.assertNull(destServer.getOnlineRegion(regionName));
    Assert.assertFalse(TEST_UTIL.getMiniHBaseCluster().getMaster().
        getAssignmentManager().getRegionStates().isRegionsInTransition());

    // Moving. It's possible that we don't have all the regions online at this point, so
    //  the test must depends only on the region we're looking at.
    LOG.info("Move starting region="+toMove.getRegionInfo().getRegionNameAsString());
    TEST_UTIL.getHBaseAdmin().move(
      toMove.getRegionInfo().getEncodedNameAsBytes(),
      destServerName.getServerName().getBytes()
    );

    while (destServer.getOnlineRegion(regionName) == null ||
        destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes) ||
        master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
      // wait for the move to be finished
      Thread.sleep(1);
    }

    LOG.info("Move finished for region="+toMove.getRegionInfo().getRegionNameAsString());

    // Check our new state.
    Assert.assertNull(curServer.getOnlineRegion(regionName));
    Assert.assertNotNull(destServer.getOnlineRegion(regionName));
    Assert.assertFalse(destServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));
    Assert.assertFalse(curServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameBytes));


    // Cache was NOT updated and points to the wrong server
    Assert.assertFalse(
        conn.getCachedLocation(TABLE_NAME3, ROW_X).getPort() == destServerName.getPort());

    // Hijack the number of retry to fail after 2 tries
    Field numTries = conn.getClass().getDeclaredField("numTries");
    numTries.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(numTries, numTries.getModifiers() & ~Modifier.FINAL);
    final int prevNumRetriesVal = (Integer)numTries.get(conn);
    numTries.set(conn, 2);

    Put put3 = new Put(ROW_X);
    put3.add(FAM_NAM, ROW_X, ROW_X);
    Put put4 = new Put(otherRow);
    put4.add(FAM_NAM, otherRow, otherRow);

    // do multi
    table.batch(Lists.newArrayList(put4, put3)); // first should be a valid row,
                                                 // second we get RegionMovedException.

    numTries.set(conn, prevNumRetriesVal);
    table.close();
    conn.close();
  }

  @Test
  public void testErrorBackoffTimeCalculation() throws Exception {
    final long ANY_PAUSE = 1000;
    HRegionInfo ri = new HRegionInfo(TABLE_NAME);
    HRegionLocation location = new HRegionLocation(ri, new ServerName("127.0.0.1", 1, 0));
    HRegionLocation diffLocation = new HRegionLocation(ri, new ServerName("127.0.0.1", 2, 0));

    ManualEnvironmentEdge timeMachine = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(timeMachine);
    try {
      long timeBase = timeMachine.currentTimeMillis();
      long largeAmountOfTime = ANY_PAUSE * 1000;
      HConnectionImplementation.ServerErrorTracker tracker =
          new HConnectionImplementation.ServerErrorTracker(largeAmountOfTime);

      // The default backoff is 0.
      assertEquals(0, tracker.calculateBackoffTime(location, ANY_PAUSE));

      // Check some backoff values from HConstants sequence.
      tracker.reportServerError(location);
      assertEqualsWithJitter(ANY_PAUSE, tracker.calculateBackoffTime(location, ANY_PAUSE));
      tracker.reportServerError(location);
      tracker.reportServerError(location);
      tracker.reportServerError(location);
      assertEqualsWithJitter(ANY_PAUSE * 2, tracker.calculateBackoffTime(location, ANY_PAUSE));

      // All of this shouldn't affect backoff for different location.

      assertEquals(0, tracker.calculateBackoffTime(diffLocation, ANY_PAUSE));
      tracker.reportServerError(diffLocation);
      assertEqualsWithJitter(ANY_PAUSE, tracker.calculateBackoffTime(diffLocation, ANY_PAUSE));

      // But should still work for a different region in the same location.
      HRegionInfo ri2 = new HRegionInfo(TABLE_NAME2);
      HRegionLocation diffRegion = new HRegionLocation(ri2, location.getServerName());
      assertEqualsWithJitter(ANY_PAUSE * 2, tracker.calculateBackoffTime(diffRegion, ANY_PAUSE));

      // Check with different base.
      assertEqualsWithJitter(ANY_PAUSE * 4,
          tracker.calculateBackoffTime(location, ANY_PAUSE * 2));

      // See that time from last error is taken into account. Time shift is applied after jitter,
      // so pass the original expected backoff as the base for jitter.
      long timeShift = (long)(ANY_PAUSE * 0.5);
      timeMachine.setValue(timeBase + timeShift);
      assertEqualsWithJitter(ANY_PAUSE * 2 - timeShift,
        tracker.calculateBackoffTime(location, ANY_PAUSE), ANY_PAUSE * 2);

      // However we should not go into negative.
      timeMachine.setValue(timeBase + ANY_PAUSE * 100);
      assertEquals(0, tracker.calculateBackoffTime(location, ANY_PAUSE));

      // We also should not go over the boundary; last retry would be on it.
      long timeLeft = (long)(ANY_PAUSE * 0.5);
      timeMachine.setValue(timeBase + largeAmountOfTime - timeLeft);
      assertTrue(tracker.canRetryMore());
      tracker.reportServerError(location);
      assertEquals(timeLeft, tracker.calculateBackoffTime(location, ANY_PAUSE));
      timeMachine.setValue(timeBase + largeAmountOfTime);
      assertFalse(tracker.canRetryMore());
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  private static void assertEqualsWithJitter(long expected, long actual) {
    assertEqualsWithJitter(expected, actual, expected);
  }

  private static void assertEqualsWithJitter(long expected, long actual, long jitterBase) {
    assertTrue("Value not within jitter: " + expected + " vs " + actual,
        Math.abs(actual - expected) <= (0.01f * jitterBase));
  }
}

