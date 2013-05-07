/*
 * Copyright 2010 The Apache Software Foundation
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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.AssignmentPlan;
import org.apache.hadoop.hbase.master.ServerManager;

import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class is for testing HCM features
 */
public class TestHCM {
  private static final Log LOG = LogFactory.getLog(TestHCM.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TABLE_NAME = Bytes.toBytes("test");
  private static final byte[] FAM_NAM = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("bbd");

  private static final int REGION_SERVERS = 5;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 3);
   TEST_UTIL.getConfiguration().set("hbase.loadbalancer.impl",
        "org.apache.hadoop.hbase.master.RegionManager$AssignmentLoadBalancer");
    TEST_UTIL.getConfiguration().setBoolean("hbase.client.record.context", true);
    TEST_UTIL.startMiniCluster(REGION_SERVERS);
    TEST_UTIL.getConfiguration().setInt(
        HConstants.HBASE_REGION_ASSIGNMENT_LOADBALANCER_WAITTIME_MS, 60000);
  }

  @Test
  public void testBlacklistRegionServerWithoutTimeout() throws Exception {

    byte[] tableName = Bytes.toBytes("testBlacklistRegionServerWithoutTimeout");

    HTable table = TEST_UTIL.createTable(tableName, FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);

    List<RegionServerThread> servers =
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads();

    // Update to Assignment Plan to balance the region across regionservers
    // equally
    AssignmentPlan ap = TEST_UTIL.getHBaseCluster().
        getMaster().regionPlacement.getNewAssignmentPlan();
    TEST_UTIL.getHBaseCluster().getMaster().regionPlacement.updateAssignmentPlan(ap);

    // Wait for rebalance to to complete
    Thread.sleep(60000);

    HRegionServer blacklistedServer = null;
    int blacklistedServerId = 0;

    // Lets select a server with does not have META/ROOT
    for (int i = 0; i < servers.size(); i++) {
      blacklistedServer = servers.get(i).getRegionServer();
      blacklistedServerId = i;
      for (HRegion region : blacklistedServer.getOnlineRegions()) {
        if (region.getRegionInfo().isMetaRegion() ||
            region.getRegionInfo().isRootRegion() ||
            region.getRegionNameAsString().contains(",,")) {
          blacklistedServer = null;
          break;
        }
      }
      if (blacklistedServer != null) {
        break;
      }
    }

    TEST_UTIL.getHBaseCluster().getMaster().addServerToBlacklist(
        blacklistedServer.getHServerInfo().getHostnamePort());

    LOG.debug(blacklistedServer.getServerInfo().getHostnamePort() + " blacklisted");

    drainRegionServer(ap, blacklistedServer);

    LOG.debug("No more regions on black listed server " +
        blacklistedServer.getHServerInfo().getHostnamePort());

    TEST_UTIL.getHBaseCluster().abortRegionServer(
        (blacklistedServerId + 1) % servers.size());

    Thread.sleep(60000);

    int numberOfNonMetaRegions = 0;
    for (HRegion r : blacklistedServer.getOnlineRegions()) {
      LOG.debug("Region opened on " + r.getRegionNameAsString());
      if (!r.getRegionInfo().isMetaRegion() &&
          !r.getRegionInfo().isRootRegion()) {
        numberOfNonMetaRegions++;
      }
    }

    assertTrue(numberOfNonMetaRegions == 0);

    LOG.debug("Removing blacklisted Region Server");

    // Now lets remove it from the black list. The load balancer will kick in
    // and start assigning regions to this region server
    TEST_UTIL.getHBaseCluster().getMaster().clearBlacklistedServer(
        blacklistedServer.getHServerInfo().getHostnamePort());

    while (blacklistedServer.getOnlineRegions().size() == 0) {
      LOG.debug("No regions assigned yet.");
      Thread.sleep(10000);
    }

    LOG.debug("Region Server has been atleast assigned " +
        blacklistedServer.getOnlineRegions().size() +
        " regions.");
  }

  
  /**
   * Test that when we delete a location using the first row of a region
   * that we really delete it.
   * @throws Exception
   */
  @Test
  public void testRegionCaching() throws Exception{

    HTable table = TEST_UTIL.createTable(TABLE_NAME, FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);
    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);
    HConnectionManager.TableServers conn =
        (HConnectionManager.TableServers) table.getConnectionAndResetOperationContext();
    assertNotNull(conn.getCachedLocation(TABLE_NAME, ROW));
    conn.deleteCachedLocation(TABLE_NAME, ROW, null);
    HRegionLocation rl = conn.getCachedLocation(TABLE_NAME, ROW);
    assertNull("What is this location?? " + rl, rl);
  }

  /**
   * Test that on failures, HBase client reports the errors correctly.
   * @throws Exception
   */
  @Test
  public void testClientFailureReporting() throws Exception {

    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testClientFailureReporting"), FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);

    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    setupFailure(table,
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size());
    try {
      table.put(put);
      assertNull("Put should not succeed");
    } catch (IOException e) {
      verifyFailure(table, e);

    } finally {
      ServerManager.clearRSBlacklist();
      waitForRegionsToGetReAssigned();
    }
  }

  private void waitForRegionsToGetReAssigned() {
    while(
        TEST_UTIL.getHBaseCluster().getActiveMaster().
          getRegionManager().areAllMetaRegionsOnline() == false) {}
  }


  /**
   * Test that on failures, HBase client reports the errors correctly.
   * @throws Exception
   */
  @Test
  public void testClientFailureReportingGet() throws Exception {
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testClientFailureReportingGet"), FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);


    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);

    setupFailure(table,
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size());
    try {
      Get get = new Get(ROW);
      table.get(get);
      assertNull("Get should not succeed");
    } catch (IOException e) {
      verifyFailure(table, e);
    } finally {
      ServerManager.clearRSBlacklist();
      waitForRegionsToGetReAssigned();
    }
  }

  /**
   * Test that on failures, HBase client reports the errors correctly.
   * @throws Exception
   */
  @Test
  public void testClientFailureReportingDelete() throws Exception {

    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testClientFailureReportingDelete"), FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);

    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);

    setupFailure(table,
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size());
    try {
      Delete delete = new Delete(ROW);
      table.delete(delete);
      assertNull("Delete should not succeed");
    } catch (IOException e) {
      verifyFailure(table, e);
    } finally {
      ServerManager.clearRSBlacklist();
      waitForRegionsToGetReAssigned();
    }

  }

  /**
   * Simulates a case where the get operation succeeds after a retry
   * @throws Exception
   */
  @Test
  public void testClientGetSuccess() throws Exception {

    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testClientSuccess"), FAM_NAM);
    TEST_UTIL.createMultiRegions(table, FAM_NAM);

    Put put = new Put(ROW);
    put.add(FAM_NAM, ROW, ROW);
    table.put(put);

    setupFailure(table, 0);
    try {
      Get g = new Get(ROW);
      table.get(g);
      List<OperationContext> op = table.getAndResetOperationContext();
      assertTrue(op.size() > 0);
      for (OperationContext p : op) {
        assertTrue(p.getError() != null);
        assertTrue(p.getLocation() != null);
      }
    } catch (IOException e) {

    } finally {
      ServerManager.clearRSBlacklist();
      waitForRegionsToGetReAssigned();
    }
  }

  
  private int drainRegionServer(AssignmentPlan ap,
      HRegionServer blacklistedServer) throws IOException, InterruptedException {

    while (true) {
      Collection<HRegion> regions = blacklistedServer.getOnlineRegions();

      final Set<HRegionInfo> pendingRegionsForServer = TEST_UTIL.getHBaseCluster().getMaster().
          getRegionManager().getAssignmentManager().
          getTransientAssignments(blacklistedServer.getServerInfo().getServerAddress());

      // Loop until the server has no regions and there are no more assignments left
      // for this region server.
      if (regions.size() == 0 && pendingRegionsForServer ==  null) {
        break;
      }

      for (HRegion region : regions) {
        HRegionInterface destRS =
            getDestinationServer(ap, blacklistedServer.getHServerInfo().getServerAddress(),
            region.getRegionInfo());
        if (destRS == null) {
          LOG.debug("No preferred server found for " + region.getRegionNameAsString() +
              ". Skipping");
        }
        LOG.debug("Moving region " + region.getRegionNameAsString());
        try {
          TEST_UTIL.getHBaseAdmin().moveRegion(
              region.getRegionInfo().getRegionName(),
              destRS.getHServerInfo().getHostnamePort());
        } catch (IOException e) {
          LOG.info("Cannot move " + region.getRegionNameAsString());
          continue;
        }

        while (true) {
          try {
            HRegionInfo r = destRS.getRegionInfo(region.getRegionName());
            if (r != null) {
              break;
            }
          } catch (Exception e) {
            LOG.info("Waiting for region to come online on destination region server");
            // region not yet moved; continue
          }
          Thread.sleep(500);
        }
      }
      Thread.sleep(1000);
    }

    return 0;
  }

 final HRegionInterface getDestinationServer(
     AssignmentPlan plan, HServerAddress serverAddr,
     final HRegionInfo region) {

    List<HServerAddress> serversForRegion = plan.getAssignment(region);

    // Get the preferred region server from the Assignment Plan
    for (HServerAddress server : serversForRegion) {
      if (!server.equals(serverAddr)) {
        try {
          HRegionInterface candidate = TEST_UTIL.getHBaseAdmin().getConnection().getHRegionConnection(server);
          if (!TEST_UTIL.getHBaseAdmin().getConnection().getHRegionConnection(server).isStopped()) {
            return candidate;
          }
        } catch (IOException e) {
          // server not online/reachable skip
        }
      }
    }

    // if none found we should return a random server. For now return null
    return null;
  }

  /**
   * Simulates a case where the RegionServer throws exception because
   * a put operation failed.
   * @throws Exception
   */
  @Test
  public void testRemoteServerFailure() throws Exception {

    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRemoteServerFailure"), FAM_NAM);
    table.setAutoFlush(true);

    TEST_UTIL.createMultiRegions(table, FAM_NAM);

    try {

      for (int i = 0; i < REGION_SERVERS; i++) {
        TEST_UTIL.getHBaseCluster().getRegionServer(i).
        getConfiguration().set(HConstants.REGION_IMPL,
          TestHRegion.class.getName());
      }

      TEST_UTIL.closeRegionByRow(ROW, table);
      // Enough time for region to close and reopen with the new TestHRegionClass
      Thread.sleep(10000);

      Put put = new Put(ROW);
      put.add(FAM_NAM, ROW, ROW);

      table.put(put);

      assertNull("Put should not succeed");
    } catch (RetriesExhaustedException e) {
      assertTrue(e.wasOperationAttemptedByServer());
    }
  }

  static public class TestHRegion extends HRegion {
    public TestHRegion(Path basedir, HLog log, FileSystem fs,
        Configuration conf, HRegionInfo regionInfo, FlushRequester flushListener) {
          super(basedir, log, fs, conf, regionInfo, flushListener);
    }

    @Override
    public OperationStatusCode[] batchMutateWithLocks(Pair<Mutation, Integer>[] putsAndLocks,
        String methodName) throws IOException {
      throw new IOException("Test induced failure");
    }
  }

  /**
   * Assign noOfRS to the dead maps list, so that no regions are assigned to
   * them and then kill the current region server.
   * @param table
   * @param noOfRS
   * @throws IOException
   */
  private void setupFailure(HTable table, int noOfRS) throws IOException {
    HRegionLocation regLoc = table.getRegionLocation(ROW);
    List<RegionServerThread> regionServers =
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads();

    // Add all region servers to the black list to prevent fail overs.
    for(int i = 0; i < noOfRS; i++) {
      ServerManager.blacklistRSHostPort(
          regionServers.get(i).getRegionServer().getHServerInfo().getHostnamePort());
    }

    HRegionInterface rsConnection =
        TEST_UTIL.getHBaseAdmin().connection.getHRegionConnection(regLoc.getServerAddress());
    rsConnection.closeRegion(regLoc.getRegionInfo(), false);

  }

  private void verifyFailure(HTable table, Exception e) {

    List<OperationContext> context = table.getAndResetOperationContext();
    assertTrue(context.size() != 0);
    for (OperationContext c : context) {
      assertTrue(c.getError() != null);
      assertTrue(c.getLocation() != null);
    }
    assertTrue(e instanceof RetriesExhaustedException);
    RetriesExhaustedException exp = (RetriesExhaustedException)(e);
    assertTrue(exp.getRegionNames() != null);
    assertTrue(!exp.wasOperationAttemptedByServer());
    LOG.info(exp.getFailureInfo().values());
  }
}
