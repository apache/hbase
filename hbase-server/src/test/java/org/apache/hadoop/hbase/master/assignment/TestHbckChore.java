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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MetricsAssignmentManager;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.hbck.HbckChore;
import org.apache.hadoop.hbase.master.hbck.HbckReport;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestHbckChore extends TestAssignmentManagerBase {

  private HbckChore hbckChore;

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    hbckChore = new HbckChore(master);
  }

  @Test
  public void testForMeta() {
    byte[] metaRegionNameAsBytes = RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName();
    String metaRegionName = RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionNameAsString();
    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    assertEquals(NSERVERS, serverNames.size());

    hbckChore.choreForTesting();
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
      hbckChore.getLastReport().getInconsistentRegions();

    // Test for case1: Master thought this region opened, but no regionserver reported it.
    assertTrue(inconsistentRegions.containsKey(metaRegionName));
    Pair<ServerName, List<ServerName>> pair = inconsistentRegions.get(metaRegionName);
    ServerName locationInMeta = pair.getFirst();
    List<ServerName> reportedRegionServers = pair.getSecond();
    assertTrue(serverNames.contains(locationInMeta));
    assertEquals(0, reportedRegionServers.size());

    // Reported right region location. Then not in problematic regions.
    am.reportOnlineRegions(locationInMeta, Collections.singleton(metaRegionNameAsBytes));
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getLastReport().getInconsistentRegions();
    assertFalse(inconsistentRegions.containsKey(metaRegionName));
  }

  @Test
  public void testForUserTable() throws Exception {
    TableName tableName = TableName.valueOf("testForUserTable");
    RegionInfo hri = createRegionInfo(tableName, 1);
    String regionName = hri.getRegionNameAsString();
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    Future<byte[]> future = submitProcedure(createAssignProcedure(hri));
    waitOnFuture(future);

    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    assertEquals(NSERVERS, serverNames.size());

    // Test for case1: Master thought this region opened, but no regionserver reported it.
    hbckChore.choreForTesting();
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
      hbckChore.getLastReport().getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
    Pair<ServerName, List<ServerName>> pair = inconsistentRegions.get(regionName);
    ServerName locationInMeta = pair.getFirst();
    List<ServerName> reportedRegionServers = pair.getSecond();
    assertTrue(serverNames.contains(locationInMeta));
    assertEquals(0, reportedRegionServers.size());

    // Test for case2: Master thought this region opened on Server1, but regionserver reported
    // Server2
    final ServerName tempLocationInMeta = locationInMeta;
    final ServerName anotherServer =
      serverNames.stream().filter(s -> !s.equals(tempLocationInMeta)).findFirst().get();
    am.reportOnlineRegions(anotherServer, Collections.singleton(hri.getRegionName()));
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getLastReport().getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
    pair = inconsistentRegions.get(regionName);
    locationInMeta = pair.getFirst();
    reportedRegionServers = pair.getSecond();
    assertEquals(1, reportedRegionServers.size());
    assertFalse(reportedRegionServers.contains(locationInMeta));
    assertTrue(reportedRegionServers.contains(anotherServer));

    // Test for case3: More than one regionservers reported opened this region.
    am.reportOnlineRegions(locationInMeta, Collections.singleton(hri.getRegionName()));
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getLastReport().getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
    pair = inconsistentRegions.get(regionName);
    locationInMeta = pair.getFirst();
    reportedRegionServers = pair.getSecond();
    assertEquals(2, reportedRegionServers.size());
    assertTrue(reportedRegionServers.contains(locationInMeta));
    assertTrue(reportedRegionServers.contains(anotherServer));

    // Reported right region location, then not in inconsistent regions.
    am.reportOnlineRegions(anotherServer, Collections.emptySet());
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getLastReport().getInconsistentRegions();
    assertFalse(inconsistentRegions.containsKey(regionName));

    // Test for case4: No region location for a previously reported region. Probably due to
    // TRSP bug or bypass.
    am.offlineRegion(hri);
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getLastReport().getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
  }

  @Test
  public void testForDisabledTable() throws Exception {
    TableName tableName = TableName.valueOf("testForDisabledTable");
    RegionInfo hri = createRegionInfo(tableName, 1);
    String regionName = hri.getRegionNameAsString();
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    Future<byte[]> future = submitProcedure(createAssignProcedure(hri));
    waitOnFuture(future);

    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    assertEquals(NSERVERS, serverNames.size());

    hbckChore.choreForTesting();
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
      hbckChore.getLastReport().getInconsistentRegions();
    assertTrue(inconsistentRegions.containsKey(regionName));
    Pair<ServerName, List<ServerName>> pair = inconsistentRegions.get(regionName);
    ServerName locationInMeta = pair.getFirst();
    List<ServerName> reportedRegionServers = pair.getSecond();
    assertTrue(serverNames.contains(locationInMeta));
    assertEquals(0, reportedRegionServers.size());

    // Set table state to disabled, then not in inconsistent regions.
    TableStateManager tableStateManager = master.getTableStateManager();
    Mockito.when(tableStateManager.isTableState(tableName, TableState.State.DISABLED))
      .thenReturn(true);
    hbckChore.choreForTesting();
    inconsistentRegions = hbckChore.getLastReport().getInconsistentRegions();
    assertFalse(inconsistentRegions.containsKey(regionName));
  }

  @Test
  public void testForSplitParent() throws Exception {
    TableName tableName = TableName.valueOf("testForSplitParent");
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes(0))
      .setEndKey(Bytes.toBytes(1)).setSplit(true).setOffline(true).setRegionId(0).build();
    String regionName = hri.getEncodedName();
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    Future<byte[]> future = submitProcedure(createAssignProcedure(hri));
    waitOnFuture(future);

    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    assertEquals(NSERVERS, serverNames.size());

    hbckChore.choreForTesting();
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
      hbckChore.getLastReport().getInconsistentRegions();
    assertFalse(inconsistentRegions.containsKey(regionName));
  }

  @Test
  public void testOrphanRegionsOnFS() throws Exception {
    TableName tableName = TableName.valueOf("testOrphanRegionsOnFS");
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    Configuration conf = util.getConfiguration();

    hbckChore.choreForTesting();
    assertEquals(0, hbckChore.getLastReport().getOrphanRegionsOnFS().size());

    HRegion.createRegionDir(conf, regionInfo, CommonFSUtils.getRootDir(conf));
    hbckChore.choreForTesting();
    assertEquals(1, hbckChore.getLastReport().getOrphanRegionsOnFS().size());
    assertTrue(
      hbckChore.getLastReport().getOrphanRegionsOnFS().containsKey(regionInfo.getEncodedName()));

    FSUtils.deleteRegionDir(conf, regionInfo);
    hbckChore.choreForTesting();
    assertEquals(0, hbckChore.getLastReport().getOrphanRegionsOnFS().size());
  }

  @Test
  public void testChoreDisable() {
    // The way to disable to chore is to set hbase.master.hbck.chore.interval <= 0
    // When the interval is > 0, the chore should run.
    Instant lastRunTime = Optional.ofNullable(hbckChore.getLastReport())
      .map(HbckReport::getCheckingEndTimestamp).orElse(null);
    hbckChore.choreForTesting();
    Instant thisRunTime = Optional.ofNullable(hbckChore.getLastReport())
      .map(HbckReport::getCheckingEndTimestamp).orElse(null);
    assertNotNull(thisRunTime);
    assertNotEquals(lastRunTime, thisRunTime);

    // When the interval <= 0, the chore shouldn't run
    master.getConfiguration().setInt("hbase.master.hbck.chore.interval", 0);
    HbckChore hbckChoreWithChangedConf = new HbckChore(master);
    hbckChoreWithChangedConf.choreForTesting();
    assertNull(hbckChoreWithChangedConf.getLastReport());
  }

  @Test
  public void testConcurrentTriggersAreRejected() throws Exception {
    final CountDownLatch scanStarted = new CountDownLatch(1);
    final CountDownLatch resumeScan = new CountDownLatch(1);
    final MockMasterServices blockingMaster = Mockito.spy(master);
    Mockito.doAnswer(invocation -> {
      scanStarted.countDown();
      assertTrue(resumeScan.await(30, TimeUnit.SECONDS), "Timed out waiting to resume the scan");
      return am;
    }).when(blockingMaster).getAssignmentManager();
    final HbckChore chore = new HbckChore(blockingMaster);
    final ExecutorService concurrentExecutor = Executors.newFixedThreadPool(2);
    try {
      // Wait until the first scan is running and paused.
      final Future<Boolean> first = concurrentExecutor.submit(chore::runChore);
      assertTrue(scanStarted.await(10, TimeUnit.SECONDS), "The first scan did not start");

      // The second invocation is rejected before the first scan finishes; no report is published.
      final Future<Boolean> second = concurrentExecutor.submit(chore::runChore);
      assertFalse(second.get(10, TimeUnit.SECONDS));
      assertNull(chore.getLastReport());

      // Resume the first scan and verify that it completes and publishes a report.
      resumeScan.countDown();
      assertTrue(first.get(10, TimeUnit.SECONDS));
      assertNotNull(chore.getLastReport());
    } finally {
      resumeScan.countDown();
      concurrentExecutor.shutdownNow();
      concurrentExecutor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testConcurrentTriggersBeforeRunningStateIsSet() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final MockMasterServices blockingMaster = Mockito.spy(master);
    Mockito.doAnswer(invocation -> {
      latch.await();
      return am;
    }).when(blockingMaster).getAssignmentManager();
    final HbckChore chore = new HbckChore(blockingMaster);
    final List<Thread> callers = new CopyOnWriteArrayList<>();
    final ExecutorService concurrentExecutor = Executors.newFixedThreadPool(2, task -> {
      final Thread thread = new Thread(task);
      callers.add(thread);
      return thread;
    });
    try {
      final Future<Boolean> first;
      final Future<Boolean> second;
      synchronized (chore) {
        first = concurrentExecutor.submit(chore::runChore);
        second = concurrentExecutor.submit(chore::runChore);
        // Wait until both legacy callers pass the running check and wait for this monitor.
        // With an atomic guard, the rejected invocation completes instead.
        util.waitFor(10000, () -> first.isDone() || second.isDone() || (callers.size() == 2
          && callers.stream().allMatch(thread -> isBlockedOnChore(thread, chore))));
      }

      latch.countDown();
      // One invocation must return true and the other false, regardless of which starts first.
      assertNotEquals(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS),
        "Exactly one concurrent invocation must execute");
      assertNotNull(chore.getLastReport());
    } finally {
      latch.countDown();
      concurrentExecutor.shutdownNow();
      concurrentExecutor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Checks whether the worker is waiting for the chore monitor held by the current test thread.
   */
  private static boolean isBlockedOnChore(Thread thread, HbckChore chore) {
    final ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
    return info != null && info.getThreadState() == Thread.State.BLOCKED
      && info.getLockOwnerId() == Thread.currentThread().getId() && info.getLockInfo() != null
      && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(chore);
  }

  @Test
  public void testRunningStateIsClearedAfterMetricsFailure() {
    final MockMasterServices failingMaster = Mockito.spy(master);
    final AssignmentManager assignmentManager = Mockito.spy(am);
    final MetricsAssignmentManager metrics = Mockito.spy(am.getAssignmentManagerMetrics());
    Mockito.doReturn(assignmentManager).when(failingMaster).getAssignmentManager();
    Mockito.doReturn(metrics).when(assignmentManager).getAssignmentManagerMetrics();
    final HbckChore chore = new HbckChore(failingMaster);

    // Fail the metrics update on the first run.
    final IllegalStateException failure = new IllegalStateException("Metrics update failed");
    Mockito.doThrow(failure).when(metrics).updateOrphanRegionsOnRs(Mockito.anyInt());
    assertSame(failure, assertThrows(IllegalStateException.class, chore::runChore));

    // Replace the throwing stub so a second run can verify that the running state was cleared.
    Mockito.doNothing().when(metrics).updateOrphanRegionsOnRs(Mockito.anyInt());
    assertTrue(chore.runChore());
  }

  @Test
  public void testChoreSkipsForeignMetaTables() throws Exception {
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path rootDir = master.getMasterFileSystem().getRootDir();
    String[] metaTables = { "meta_replica1", "meta" };
    Path hbaseNamespaceDir = new Path(rootDir, HConstants.BASE_NAMESPACE_DIR + "/hbase");
    fs.mkdirs(hbaseNamespaceDir);

    for (String metaTable : metaTables) {
      TableName tableName = TableName.valueOf("hbase", metaTable);
      Path metaTableDir = new Path(hbaseNamespaceDir, metaTable);
      fs.mkdirs(metaTableDir);
      fs.mkdirs(new Path(metaTableDir, FSTableDescriptors.TABLEINFO_DIR));
      fs.mkdirs(new Path(metaTableDir, "abcdef0123456789"));

      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(HConstants.CATALOG_FAMILY)
          .setMaxVersions(HConstants.DEFAULT_HBASE_META_VERSIONS).setInMemory(true)
          .setBlocksize(HConstants.DEFAULT_HBASE_META_BLOCK_SIZE)
          .setBloomFilterType(BloomType.ROWCOL).build())
        .build();

      Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
      FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, tableDescriptor,
        false);
    }

    assertTrue(hbckChore.runChore(), "HbckChore should run successfully");
    HbckReport report = hbckChore.getLastReport();
    assertNotNull(report, "HbckReport should not be null");
    boolean hasForeignMetaOrphan = report.getOrphanRegionsOnFS().values().stream()
      .anyMatch(path -> path.toString().contains("meta_replica1"));
    assertFalse(hasForeignMetaOrphan, "HbckChore should not report foreign meta tables as orphans");
  }
}
