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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * End-to-end reproduction of HBASE-29364 on a real mini cluster.
 * Narrative (mirrors the incident):
 * A region is open and serving.
 * Its RegionServer is killed; ServerCrashProcedure reassigns the region (TRSP).
 * The reassignment open fails (FAILED_OPEN); the OpenRegionProcedure
 * records REPORT_SUCCEED + transitionCode=FAILED_OPEN in the procedure store.
 * The active Master "dies" before the procedure can be wrapped up, and a backup Master takes
 * over (real masterFailover).
 * The new Master replays the procedure during meta-load and -- because of the bug --
 * marks the region OPEN instead of retrying it.
 * The only non-organic part is the freeze: to deterministically keep the procedure in
 * REPORT_SUCCEED across the failover, a coprocessor on hbase:meta fails the
 * persistToMeta write that runs immediately after the FAILED_OPEN (this is exactly
 * the JIRA condition, where hbase:meta was unavailable at that moment).
 * This test asserts the buggy behaviour, so it PASSES against the unfixed code. After the
 * fix lands it must be inverted (the region must NOT come back OPEN).
 */
@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestOpenRegionProcedureRestoreFailedOpen {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestOpenRegionProcedureRestoreFailedOpen.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("t2-failed-open");
  private static final byte[] CF = Bytes.toBytes("cf");

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      FailOpenAndBlockMetaCP.class.getName());
    // Do not give up on the open; we want it stuck retrying, not failed-for-good.
    UTIL.getConfiguration().setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 1000);
    // We kill an RS mid-test; the failover master must not block its init waiting for the full
    // RegionServer count to come back.
    UTIL.getConfiguration().setInt("hbase.master.wait.on.regionservers.mintostart", 1);
    StartTestingClusterOption option =
      StartTestingClusterOption.builder().numMasters(2).numRegionServers(3).build();
    UTIL.startMiniCluster(option);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    // Stop injecting failures so the cluster can shut down cleanly.
    FailOpenAndBlockMetaCP.armed = false;
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFailedOpenReplayedAsOpen() throws Exception {
    SingleProcessHBaseCluster cluster = UTIL.getMiniHBaseCluster();

    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME);
    RegionInfo hri = UTIL.getAdmin().getRegions(TABLE_NAME).get(0);

    // Arm the injection: from now on, opens of this region FAIL, and the 2nd OPENING write to this
    // region's hbase:meta row (the post-FAILED_OPEN persistToMeta) is blocked.
    FailOpenAndBlockMetaCP.targetRegionMetaRow = hri.getRegionName();
    FailOpenAndBlockMetaCP.failOpenTable = TABLE_NAME;
    FailOpenAndBlockMetaCP.armed = true;

    // Kill the RegionServer hosting the region -> ServerCrashProcedure -> reassignment (TRSP).
    ServerName rsWithRegion = findRegionServer(cluster, hri);
    LOG.info("Killing RS hosting {}: {}", hri.getEncodedName(), rsWithRegion);
    cluster.killRegionServer(rsWithRegion);
    cluster.waitForRegionServerToStop(rsWithRegion, 60_000);

    // Wait until the reassignment open has failed and the procedure is frozen in REPORT_SUCCEED
    // (its post-FAILED_OPEN persistToMeta is blocked in the meta coprocessor).
    LOG.info("Waiting for the FAILED_OPEN procedure to freeze in REPORT_SUCCEED...");
    assertTrue(
      FailOpenAndBlockMetaCP.reachedBlock.await(120, TimeUnit.SECONDS),
      "Timed out waiting for the FAILED_OPEN persistToMeta to be reached");

    // The region must NOT be OPEN at this point - the open failed.
    State beforeFailover = regionState(cluster.getMaster(), hri);
    assertEquals(State.OPENING, regionState(cluster.getMaster(), hri));
    LOG.info("State before failover (active master): {}", beforeFailover);

    // The active Master "dies"; a backup Master takes over and replays procedures during meta-load.
    LOG.info("Triggering real master failover...");
    MasterProcedureTestingUtility.masterFailover(UTIL);
    HMaster newMaster = cluster.getMaster();
    LOG.info("New active master: {}", newMaster.getServerName());

    UTIL.waitFor(60_000, 500, () -> regionState(newMaster, hri) == State.OPENING);
    assertEquals(State.OPENING, regionState(newMaster, hri));
    assertNotEquals(State.OPEN, regionState(newMaster, hri));
  }

  private static State regionState(HMaster master, RegionInfo hri) {
    RegionStateNode node =
      master.getAssignmentManager().getRegionStates().getRegionStateNode(hri);
    return node == null ? null : node.getState();
  }

  private static ServerName findRegionServer(SingleProcessHBaseCluster cluster, RegionInfo hri)
    throws IOException {
    for (HRegionServer rs : cluster.getRegionServerThreads().stream().map(t -> t.getRegionServer())
      .collect(java.util.stream.Collectors.toList())) {
      if (!rs.getRegions(hri.getTable()).isEmpty()) {
        return rs.getServerName();
      }
    }
    throw new IOException("No RS hosts " + hri.getEncodedName());
  }

  /**
   * Region coprocessor used to (a) force a clean {@code FAILED_OPEN} for the target table, and
   * (b) fail the post-{@code FAILED_OPEN} {@code persistToMeta} write so the procedure stays in
   * {@code REPORT_SUCCEED} across the failover.
   */
  public static class FailOpenAndBlockMetaCP implements RegionCoprocessor, RegionObserver {
    static volatile boolean armed = false;
    static volatile TableName failOpenTable = null;
    static volatile byte[] targetRegionMetaRow = null;
    static final AtomicInteger openingWrites = new AtomicInteger(0);
    static final CountDownLatch reachedBlock = new CountDownLatch(1);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c)
      throws IOException {
      if (!armed) {
        return;
      }
      TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
      if (table.equals(failOpenTable)) {
        // An IOException out of open => the RS cleanly reports FAILED_OPEN (it does not abort).
        throw new IOException("Injected open failure for " + table);
      }
    }

    @Override
    public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
      WALEdit edit) throws IOException {
      if (!armed || targetRegionMetaRow == null) {
        return;
      }
      RegionInfo ri = c.getEnvironment().getRegion().getRegionInfo();
      if (!ri.isMetaRegion()) {
        return;
      }
      if (!Bytes.equals(put.getRow(), targetRegionMetaRow) || !setsStateOpening(put)) {
        return;
      }
      // First OPENING write = the assign's regionOpening (before dispatch). Let it through.
      // Second OPENING write = the persistToMeta right after FAILED_OPEN. Fail it, which freezes
      // the OpenRegionProcedure in REPORT_SUCCEED until we fail the master over.
      int n = openingWrites.incrementAndGet();
      LOG.info("Meta OPENING write #{} for target region", n);
      if (n >= 2) {
        reachedBlock.countDown();
        // Fail (do NOT block) so we never hold the meta row lock. This makes persistToMeta fail
        // exactly like the JIRA's 'hbase:meta unavailable' condition, so the OpenRegionProcedure
        // keeps retrying and stays in REPORT_SUCCEED until we fail the master over.
        throw new IOException("Injected hbase:meta unavailability for target region");
      }
    }

    private static boolean setsStateOpening(Put put) {
      List<Cell> cells = put.getFamilyCellMap().get(HConstants.CATALOG_FAMILY);
      if (cells == null) {
        return false;
      }
      byte[] opening = Bytes.toBytes(RegionState.State.OPENING.name());
      for (Cell cell : cells) {
        if (
          CellUtil.matchingQualifier(cell, HConstants.STATE_QUALIFIER)
            && CellUtil.matchingValue(cell, opening)
        ) {
          return true;
        }
      }
      return false;
    }
  }
}
