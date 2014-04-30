/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.HBaseEventHandler;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventHandlerListener;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.master.handler.MasterCloseRegionHandler;
import org.apache.hadoop.hbase.master.handler.MasterOpenRegionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.TestTag;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TagRunner.class)
public class TestRegionStateOnMasterFailure extends MultiMasterTest {

  private static final Log LOG =
      LogFactory.getLog(TestRegionStateOnMasterFailure.class);

  private static void assertHostPort(String rsName) {
    assertTrue(rsName + " is not a valid host:port pair",
        HServerInfo.isValidHostAndPort(rsName));
  }

  private static final byte[] TABLE_NAME = Bytes.toBytes("TestRegionState");
  private static final byte[] CF1 = Bytes.toBytes("MyCF1");
  private static final byte[] CF2 = Bytes.toBytes("MyCF2");
  private static final byte[][] FAMILIES = new byte[][] { CF1, CF2 };

  private static final String REGION_EVENT_MSG =
      "REGION EVENT LISTENER: ";

  private static final int NUM_MASTERS = 2;
  private static final int NUM_RS = 4;

  private static final long TEST_TIMEOUT_MS = 360 * 1000;

  private static final Pattern META_AND_ROOT_RE = Pattern.compile(
      (Bytes.toStringBinary(HConstants.META_TABLE_NAME) + "|" +
      Bytes.toStringBinary(HConstants.ROOT_TABLE_NAME)).replace(".", "\\."));

  private List<HBaseEventHandlerListener> toUnregister =
      new ArrayList<HBaseEventHandlerListener>();

  @Override
  @After
  public void tearDown() throws IOException {
    for (HBaseEventHandlerListener listener : toUnregister) {
      HBaseEventHandler.unregisterListener(listener);
    }
    toUnregister.clear();
    super.tearDown();
  }

  private interface WayToCloseRegion {
    void closeRegion(HRegion region) throws IOException;
  }

  private class CloseRegionThroughAdmin implements WayToCloseRegion {
    @Override
    public void closeRegion(HRegion region) throws IOException {
      header("Closing region " + region.getRegionNameAsString());
      testUtil.closeRegion(region.getRegionName());
    }
  };

  private class KillRegionServerWithRegion implements WayToCloseRegion {
    @Override
    public void closeRegion(HRegion region) throws IOException {
      header("Aborting the region server with the region " +
          region.getRegionNameAsString());
      region.getRegionServer().abort("Killing region server holding " +
          "region " + region);
    }
  }

  /** Kills -ROOT- and .META. regionservers */
  private class KillRootAndMetaRS implements WayToCloseRegion {
    @Override
    public void closeRegion(HRegion ignored) throws IOException {
      // Copy the list of region server threads because it will be modified as we kill
      // -ROOT-/.META. regionservers.
      for (RegionServerThread rst : new ArrayList<>(
          miniCluster().getRegionServerThreads())) {
        HRegionServer rs = rst.getRegionServer();
        for (HRegionInfo hri : rs.getRegionsAssignment()) {
          if (hri.isRootRegion() || hri.isMetaRegion()) {
            rs.abort("Killing region server holding region " + hri.getRegionNameAsString());
            break;
          }
        }
      }
    }
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    ServerManager.clearRSBlacklist();
    startMiniCluster(NUM_MASTERS, NUM_RS);
    fillTable();
    shortSleep();
  }

  @Test(timeout=TEST_TIMEOUT_MS)
  public void testCloseUserRegionKillMasterOnClosed() throws IOException,
      InterruptedException, KeeperException {
    closeRegionAndKillMaster(TABLE_NAME, new CloseRegionThroughAdmin(),
        HBaseEventType.RS2ZK_REGION_CLOSED);
  }

  @Test(timeout=TEST_TIMEOUT_MS)
  public void testCloseUserRegionKillMasterOnOpened() throws IOException,
      InterruptedException, KeeperException {
    closeRegionAndKillMaster(TABLE_NAME, new CloseRegionThroughAdmin(),
        HBaseEventType.RS2ZK_REGION_OPENED);
  }

  @Test(timeout=TEST_TIMEOUT_MS)
  public void testCloseRootKillMasterOnClosed() throws Exception {
    closeRegionAndKillMaster(HConstants.ROOT_TABLE_NAME, new CloseRegionThroughAdmin(),
        HBaseEventType.RS2ZK_REGION_CLOSED);
  }

  @Test(timeout=TEST_TIMEOUT_MS)
  public void testCloseRootKillMasterOnOpened() throws Exception {
    closeRegionAndKillMaster(HConstants.ROOT_TABLE_NAME, new CloseRegionThroughAdmin(),
        HBaseEventType.RS2ZK_REGION_OPENED);
  }

  @Test(timeout=TEST_TIMEOUT_MS)
  public void testCloseMetaKillMasterOnClosed() throws Exception {
    closeRegionAndKillMaster(HConstants.META_TABLE_NAME, new CloseRegionThroughAdmin(),
        HBaseEventType.RS2ZK_REGION_CLOSED);
  }

  @Test(timeout=TEST_TIMEOUT_MS)
  public void testCloseMetaKillMasterOnOpened() throws Exception {
    closeRegionAndKillMaster(HConstants.META_TABLE_NAME, new CloseRegionThroughAdmin(),
        HBaseEventType.RS2ZK_REGION_OPENED);
  }

  // Marked as unstable and recorded in #3920920
  @TestTag({ "unstable" })
  @Test(timeout=TEST_TIMEOUT_MS)
  public void testKillRSWithUserRegion() throws IOException,
      InterruptedException, KeeperException {
    closeRegionAndKillMaster(TABLE_NAME, new KillRegionServerWithRegion(),
        HBaseEventType.RS2ZK_REGION_OPENED);
  }

  @Test(timeout=TEST_TIMEOUT_MS)
  public void testKillRootRS() throws Exception {
    closeRegionAndKillMaster(HConstants.ROOT_TABLE_NAME, new KillRegionServerWithRegion(),
        HBaseEventType.RS2ZK_REGION_OPENED);
  }

  @Test(timeout=TEST_TIMEOUT_MS)
  public void testKillMetaRS() throws Exception {
    closeRegionAndKillMaster(HConstants.META_TABLE_NAME, new KillRegionServerWithRegion(),
        HBaseEventType.RS2ZK_REGION_OPENED);
  }

  // Marked as unstable and recorded in #4212779
  @TestTag({ "unstable" })
  @Test(timeout=TEST_TIMEOUT_MS)
  public void testKillRootMetaRS() throws Exception {
    closeRegionAndKillMaster(HConstants.META_TABLE_NAME, new KillRootAndMetaRS(),
        HBaseEventType.RS2ZK_REGION_OPENED);
  }

  public void closeRegionAndKillMaster(byte[] tableName,
      WayToCloseRegion howToClose, HBaseEventType eventToWatch)
      throws IOException, InterruptedException, KeeperException {
    HBaseTestingUtility.setThreadNameFromCallerMethod();
    final List<HRegion> regions = miniCluster().getRegions(tableName);
    assertEquals(1, regions.size());
    final HRegion region = regions.get(0);
    final String originalRS =
        region.getRegionServer().getServerInfo().getHostnamePort();

    header("Original RS holding region " + region.getRegionNameAsString() +
        ": " + originalRS);

    // Blacklist this regionserver from being assigned any more regions.
    ServerManager.blacklistRSHostPort(originalRS);

    final String targetRegionName = region.getRegionNameAsString();
    MasterKillerListener listener = new MasterKillerListener(
        targetRegionName, eventToWatch);

    HBaseEventHandler.registerListener(listener);
    toUnregister.add(listener);
    howToClose.closeRegion(region);

    shortSleep();

    header("Waiting until all regions are assigned");
    testUtil.waitUntilAllRegionsAssigned(1);

    if (listener.failed()) {
      fail("Fatal error in the event listener -- please check the logs.");
    }

    final Map<String, Set<String>> regionToServerActual =
        getAssignmentsFromRSMemory();
    final Map<String, String> regionToServerInMeta =
        getAssignmentsFromMeta();

    checkForMultipleAssignments(regionToServerActual);
    checkMetaConsistency(regionToServerActual, regionToServerInMeta);
  }

  /**
   * Checks that the region to server mapping obtained from region server
   * in-memory state is the same as the state obtained from meta.
   * @param regionToServerActual the region to server assignment from RS memory
   * @param regionToServerInMeta the region to server assignment from .META.
   */
  private void checkMetaConsistency(
      final Map<String, Set<String>> regionToServerActual,
      final Map<String, String> regionToServerInMeta) {
    // Check that every region in .META. exists in RS memory and the assignment
    // is the same.
    for (Map.Entry<String, String> regionAndServer :
         regionToServerInMeta.entrySet()) {
      final String regionName = regionAndServer.getKey();
      final String hostPortFromMeta = regionAndServer.getValue();

      assertTrue("Region " + regionName + " is set as assigned to "
          + hostPortFromMeta
          + "in .META. but not in regionserver in-memory state",
          regionToServerActual.containsKey(regionName));

      // We should have already checked for multiple assignment, so no detailed
      // error message this time.
      final Set<String> actualServers = regionToServerActual.get(regionName);
      assertEquals(1, actualServers.size());

      // Check that the assignment is the same.
      final String actualHostPort = actualServers.iterator().next();
      assertHostPort(actualHostPort);
      assertTrue("Inconsistent region assignment for " + regionName + ": " +
          hostPortFromMeta + " in META but actually assigned to " + actualHostPort,
          actualHostPort.equals(hostPortFromMeta));
    }

    // Now check that every region in actual assignment exists in .META.
    // Given that, the above guarantees that the assignment is the same.
    for (Map.Entry<String, Set<String>> actualAssignment :
        regionToServerActual.entrySet()) {
      final String regionName = actualAssignment.getKey();
      assertTrue("Region " + regionName + " is not present in META but is " +
          "actually assigned to " + actualAssignment.getValue(),
          regionToServerInMeta.containsKey(regionName));
    }

    // Another sanity-check: the number of regions.
    assertEquals(regionToServerActual.size(), regionToServerInMeta.size());
  }

  private Map<String, String> getAssignmentsFromMeta() throws IOException {
    header("Resulting region assignments in .META.:");
    final HTable metaTable = new HTable(testUtil.getConfiguration(),
        HConstants.META_TABLE_NAME);
    final Scan scan = new Scan().addFamily(HConstants.CATALOG_FAMILY);
    final ResultScanner scanner = metaTable.getScanner(scan);
    Result result;
    final Map<String, String> regionToServerInMeta =
        new TreeMap<String, String>();
    while ((result = scanner.next()) != null) {
      final String regionName = Bytes.toStringBinary(result.getRow());
      final String hostPort = Bytes.toStringBinary(
          result.getFamilyMap(HConstants.CATALOG_FAMILY).get(
              HConstants.SERVER_QUALIFIER));
      assertHostPort(hostPort);
      regionToServerInMeta.put(regionName, hostPort);
      LOG.debug(regionName + " => " + hostPort);
    }
    LOG.info("Full region-to-server map obtained from META: " +
        regionToServerInMeta);
    return regionToServerInMeta;
  }

  private void checkForMultipleAssignments(
      final Map<String, Set<String>> regionToServerActual) {
    for (Map.Entry<String, Set<String>> regionAndServers :
        regionToServerActual.entrySet()) {
      assertTrue("Multiple assignment for region " + regionAndServers.getKey()
          + " : " + regionAndServers.getValue(),
          regionAndServers.getValue().size() == 1);
    }
  }

  private Map<String, Set<String>> getAssignmentsFromRSMemory()
      throws IOException {
    header("Resulting region assignments in regionserver memory:");
    final Map<String, Set<String>> regionToServerActual =
        new TreeMap<String, Set<String>>();
    for (int i = 0; i < NUM_RS; ++i) {
      final HRegionServer regionServer = miniCluster().getRegionServer(i);
      final HRegionInfo[] assignments = regionServer.getRegionsAssignment();
      final String hostPort = regionServer.getServerInfo().getHostnamePort();
      assertHostPort(hostPort);
      for (HRegionInfo hri : assignments) {
        final String regionName = hri.getRegionNameAsString();
        final String[] components = splitRegionName(regionName);
        final String tableName = components[0];

        // Skip root and meta tables.
        if (META_AND_ROOT_RE.matcher(tableName).matches()) {
          continue;
        }

        LOG.debug(regionName + " => " + hostPort);

        Set<String> servers = regionToServerActual.get(regionName);
        if (servers == null) {
          servers = new TreeSet<String>();
          regionToServerActual.put(regionName, servers);
        }
        servers.add(hostPort);
      }
    }
    LOG.info("Full region-to-server map obtained from regionservers: " +
        regionToServerActual);
    return regionToServerActual;
  }

  private static final String[] splitRegionName(String regionName) {
    String[] components = regionName.split(",");
    assertEquals("Invalid region name: " + regionName,
        3, components.length);
    return components;
  }

  /**
   * A listener that kills the master before it can process the "region opened" or "region closed"
   * events.
   */
  private class MasterKillerListener implements HBaseEventHandlerListener {

    private final String targetRegionName;
    private HBaseEventType eventToWatch;

    private volatile boolean failed;

    public MasterKillerListener(String targetRegionName, HBaseEventType eventToWatch) {
      this.targetRegionName = targetRegionName;
      this.eventToWatch = eventToWatch;
    }

    public boolean failed() {
      return failed;
    }

    /**
     * Displays an emphasized log message marked as coming from the event
     * listener.
     */
    private void logMsg(String msg) {
      header(REGION_EVENT_MSG + msg);
    }

    @Override
    public void beforeProcess(HBaseEventHandler event) {
      final HBaseEventType eventType = event.getHBEvent();
      LOG.info(REGION_EVENT_MSG + "Event: " + eventType + ", handler: " +
          event.getClass().getSimpleName());

      if (eventType != eventToWatch ||
         !(event instanceof MasterOpenRegionHandler ||
           event instanceof MasterCloseRegionHandler)) {
        LOG.info(REGION_EVENT_MSG + "Unrecognized event type/class: " + eventType + ", " +
           event.getClass().getSimpleName() + ", ignoring");
        return;
      }

      final HBaseEventHandler regionEvent = (HBaseEventHandler) event;

      boolean terminateEventThread = false;
      try {
        final String regionName = regionEvent.getRegionName();
        logMsg("Event: " + eventType + ", region: " + regionName + ", region server name: "
            + regionEvent.getRegionServerName() + ", target region: " + targetRegionName);

        // E.g. user table: regionName=01f5858c7919232822dd2525b7748aaf,
        //     targetRegionName=TestRegionState,,1343862006169.01f5858c7919232822dd2525b7748aaf.
        // Meta table: regionName=1028785192, targetRegionName=.META.,,1.1028785192
        if (targetRegionName.endsWith("." + regionName + ".") ||  // user table
            targetRegionName.endsWith("." + regionName)) {        // meta table
          // Blacklist the new regionserver from being assigned any
          // regions when the new master comes up. Then the master will
          // have to assign to the third regionserver.
          final String[] rsNameSplit =
              splitRegionName(regionEvent.getRegionServerName());
          final String newRSHostPort = rsNameSplit[0] + ":" +
              rsNameSplit[1];
          assertHostPort(newRSHostPort);
          ServerManager.blacklistRSHostPort(newRSHostPort);
          logMsg("Killing master right before it can process the event "
              + eventType + " for region " + regionName);
          HBaseEventHandler.unregisterListener(this);
          miniCluster().killActiveMaster();
          terminateEventThread = true;
        } else {
          logMsg("Skipping event for region " + regionName
              + " (does not match " + targetRegionName + ")");
        }
      } catch (Throwable t) {
        LOG.error(REGION_EVENT_MSG + "Unhandled exception", t);
        failed = true;
      }

      if (terminateEventThread) {
        throw new RuntimeException("Terminating the event processing " +
            "thread");
      }
    }

    @Override
    public void afterProcess(HBaseEventHandler event) {
    }
  }

  /**
   * Writes some data to the table.
   * @throws InterruptedException
   */
  private void fillTable() throws IOException, InterruptedException {
    Random rand = new Random(19387129L);
    HTable table = testUtil.createTable(TABLE_NAME, FAMILIES);
    for (int iStoreFile = 0; iStoreFile < 4; ++iStoreFile) {
      for (int iRow = 0; iRow < 100; ++iRow) {
        final byte[] row = Bytes.toBytes("row" + iRow);
        Put put = new Put(row);
        Delete del = new Delete(row);
        for (int iCol = 0; iCol < 10; ++iCol) {
          final byte[] cf = rand.nextBoolean() ? CF1 : CF2;
          final long ts = rand.nextInt();
          final byte[] qual = Bytes.toBytes("col" + iCol);
          if (rand.nextBoolean()) {
            final byte[] value = Bytes.toBytes("value_for_row_" + iRow +
                "_cf_" + Bytes.toStringBinary(cf) + "_col_" + iCol + "_ts_" +
                ts + "_random_" + rand.nextLong());
            put.add(cf, qual, ts, value);
          } else if (rand.nextDouble() < 0.8) {
            del.deleteColumn(cf, qual, ts);
          } else {
            del.deleteColumns(cf, qual, ts);
          }
        }
        table.put(put);
        table.delete(del);
        table.flushCommits();
      }
    }
    testUtil.waitUntilAllRegionsAssigned(1);
  }

}
