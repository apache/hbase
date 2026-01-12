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
package org.apache.hadoop.hbase.master.procedure;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseServerBase;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;

/**
 * Test of the HBCK-version of SCP. The HBCKSCP is an SCP only it reads hbase:meta for list of
 * Regions that were on the server-to-process rather than consult Master in-memory-state.
 */
@Category({ MasterTests.class, LargeTests.class })
@RunWith(Parameterized.class)
public class TestHBCKSCP extends TestSCPBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBCKSCP.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBCKSCP.class);
  @Rule
  public TableNameTestRule tableNameTestRule = new TableNameTestRule();

  private final int replicas;
  private final HBCKSCPScheduler hbckscpScheduler;
  private final RegionSelector regionSelector;

  public TestHBCKSCP(final int replicas, final HBCKSCPScheduler hbckscpScheduler,
    final RegionSelector regionSelector) {
    this.replicas = replicas;
    this.hbckscpScheduler = hbckscpScheduler;
    this.regionSelector = regionSelector;
  }

  @Parameterized.Parameters(name = "replicas:{0} scheduler:{1} selector:{2}")
  public static Object[][] params() {
    return new Object[][] {
      { 1, new ScheduleServerCrashProcedure(), new PrimaryNotMetaRegionSelector() },
      { 3, new ScheduleServerCrashProcedure(), new ReplicaNonMetaRegionSelector() },
      { 1, new ScheduleSCPsForUnknownServers(), new PrimaryNotMetaRegionSelector() },
      { 3, new ScheduleSCPsForUnknownServers(), new ReplicaNonMetaRegionSelector() } };
  }

  @Test
  public void test() throws Exception {
    // we are about to do one for it?
    SingleProcessHBaseCluster cluster = this.util.getHBaseCluster();

    // Assert that we have three RegionServers. Test depends on there being multiple.
    assertEquals(RS_COUNT, cluster.getLiveRegionServerThreads().size());

    int count;
    try (Table table = createTable(tableNameTestRule.getTableName())) {
      // Load the table with a bit of data so some logs to split and some edits in each region.
      this.util.loadTable(table, HBaseTestingUtil.COLUMNS[0]);
      count = HBaseTestingUtil.countRows(table);
    }
    assertTrue("expected some rows", count > 0);

    // Make the test easier by not working on server hosting meta...
    // Find another RS. Purge it from Master memory w/o running SCP (if
    // SCP runs, it will clear entries from hbase:meta which frustrates
    // our attempt at manufacturing 'Unknown Servers' condition).
    final ServerName metaServer = util.getMiniHBaseCluster().getServerHoldingMeta();
    final ServerName rsServerName = cluster.getRegionServerThreads().stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer).map(HBaseServerBase::getServerName)
      .filter(sn -> !sn.equals(metaServer)).findAny().orElseThrow(() -> new NoSuchElementException(
        "Cannot locate a region server that is not hosting meta."));
    HMaster master = cluster.getMaster();
    // Get a Region that is on the server.
    final List<RegionInfo> regions = master.getAssignmentManager().getRegionsOnServer(rsServerName);
    LOG.debug("{} is holding {} regions.", rsServerName, regions.size());
    final RegionInfo rsRI =
      regions.stream().peek(info -> LOG.debug("{}", info)).filter(regionSelector::regionFilter)
        .findAny().orElseThrow(regionSelector::regionFilterFailure);
    final int replicaId = rsRI.getReplicaId();
    Result r = MetaTableAccessor.getRegionResult(master.getConnection(), rsRI);
    // Assert region is OPEN.
    assertEquals(RegionState.State.OPEN.toString(), Bytes.toString(
      r.getValue(HConstants.CATALOG_FAMILY, CatalogFamilyFormat.getRegionStateColumn(replicaId))));
    ServerName serverName = CatalogFamilyFormat.getServerName(r, replicaId);
    assertEquals(rsServerName, serverName);
    // moveFrom adds to dead servers and adds it to processing list only we will
    // not be processing this server 'normally'. Remove it from processing by
    // calling 'finish' and then remove it from dead servers so rsServerName
    // becomes an 'Unknown Server' even though it is still around.
    LOG.info("Killing {}", rsServerName);
    cluster.killRegionServer(rsServerName);

    master.getServerManager().moveFromOnlineToDeadServers(rsServerName);
    master.getServerManager().getDeadServers().removeDeadServer(rsServerName);
    master.getAssignmentManager().getRegionStates().removeServer(rsServerName);
    // Kill the server. Nothing should happen since an 'Unknown Server' as far
    // as the Master is concerned; i.e. no SCP.
    HRegionServer hrs = cluster.getRegionServer(rsServerName);
    util.waitFor(TimeUnit.MINUTES.toMillis(1), hrs::isStopped);
    LOG.info("Dead {}", rsServerName);
    // Now assert still references in hbase:meta to the 'dead' server -- they haven't been
    // cleaned up by an SCP or by anything else.
    assertTrue(searchMeta(master, rsServerName));
    // Assert region is OPEN on dead server still.
    r = MetaTableAccessor.getRegionResult(master.getConnection(), rsRI);
    assertEquals(RegionState.State.OPEN.toString(), Bytes.toString(
      r.getValue(HConstants.CATALOG_FAMILY, CatalogFamilyFormat.getRegionStateColumn(replicaId))));
    serverName = CatalogFamilyFormat.getServerName(r, replicaId);
    assertNotNull(cluster.getRegionServer(serverName));
    assertEquals(rsServerName, serverName);

    // I now have 'Unknown Server' references in hbase:meta; i.e. Server references
    // with no corresponding SCP. Queue one.
    long pid = scheduleHBCKSCP(rsServerName, master);
    assertNotEquals(Procedure.NO_PROC_ID, pid);
    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), pid);
    // After SCP, assert region is OPEN on new server.
    r = MetaTableAccessor.getRegionResult(master.getConnection(), rsRI);
    assertEquals(RegionState.State.OPEN.toString(), Bytes.toString(
      r.getValue(HConstants.CATALOG_FAMILY, CatalogFamilyFormat.getRegionStateColumn(replicaId))));
    serverName = CatalogFamilyFormat.getServerName(r, 0);
    assertNotNull(cluster.getRegionServer(serverName));
    assertNotEquals(rsServerName, serverName);
    // Make sure no mention of old server post SCP.
    assertFalse(searchMeta(master, rsServerName));
  }

  protected long scheduleHBCKSCP(ServerName rsServerName, HMaster master) throws ServiceException {
    return hbckscpScheduler.scheduleHBCKSCP(rsServerName, master);
  }

  @Override
  protected int getRegionReplication() {
    return replicas;
  }

  /** Returns True if we find reference to <code>sn</code> in meta table. */
  private boolean searchMeta(HMaster master, ServerName sn) throws IOException {
    List<Pair<RegionInfo, ServerName>> ps =
      MetaTableAccessor.getTableRegionsAndLocations(master.getConnection(), null);
    for (Pair<RegionInfo, ServerName> p : ps) {
      if (p.getSecond().equals(sn)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Encapsulates the choice of which HBCK2 method to call.
   */
  private abstract static class HBCKSCPScheduler {
    abstract long scheduleHBCKSCP(ServerName rsServerName, HMaster master) throws ServiceException;

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }

  /**
   * Invokes {@code MasterRpcServices#scheduleServerCrashProcedure}.
   */
  private static class ScheduleServerCrashProcedure extends HBCKSCPScheduler {
    @Override
    public long scheduleHBCKSCP(ServerName rsServerName, HMaster master) throws ServiceException {
      MasterProtos.ScheduleServerCrashProcedureResponse response = master.getMasterRpcServices()
        .scheduleServerCrashProcedure(null, MasterProtos.ScheduleServerCrashProcedureRequest
          .newBuilder().addServerName(ProtobufUtil.toServerName(rsServerName)).build());
      assertEquals(1, response.getPidCount());
      return response.getPid(0);
    }
  }

  /**
   * Invokes {@code MasterRpcServices#scheduleSCPsForUnknownServers}.
   */
  private static class ScheduleSCPsForUnknownServers extends HBCKSCPScheduler {
    @Override
    long scheduleHBCKSCP(ServerName rsServerName, HMaster master) throws ServiceException {
      MasterProtos.ScheduleSCPsForUnknownServersResponse response =
        master.getMasterRpcServices().scheduleSCPsForUnknownServers(null,
          MasterProtos.ScheduleSCPsForUnknownServersRequest.newBuilder().build());
      assertEquals(1, response.getPidCount());
      return response.getPid(0);
    }
  }

  /**
   * Encapsulates how the target region is selected.
   */
  private static abstract class RegionSelector {
    abstract boolean regionFilter(RegionInfo info);

    abstract Exception regionFilterFailure();

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }

  /**
   * Selects a non-meta region that is also a primary region.
   */
  private static class PrimaryNotMetaRegionSelector extends RegionSelector {
    @Override
    boolean regionFilter(final RegionInfo info) {
      return !Objects.equals(connection.getMetaTableName(), info.getTable())
        && Objects.equals(RegionInfo.DEFAULT_REPLICA_ID, info.getReplicaId());
    }

    @Override
    Exception regionFilterFailure() {
      return new NoSuchElementException("Cannot locate a primary, non-meta region.");
    }
  }

  /**
   * Selects a non-meta region that is also a replica region.
   */
  private static class ReplicaNonMetaRegionSelector extends RegionSelector {
    @Override
    boolean regionFilter(RegionInfo info) {
      return !Objects.equals(connection.getMetaTableName(), info.getTable())
        && !Objects.equals(RegionInfo.DEFAULT_REPLICA_ID, info.getReplicaId());
    }

    @Override
    Exception regionFilterFailure() {
      return new NoSuchElementException("Cannot locate a replica, non-meta region.");
    }
  }
}
