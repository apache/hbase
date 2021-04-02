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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class AssignmentTestingUtil {
  private static final Logger LOG = LoggerFactory.getLogger(AssignmentTestingUtil.class);

  private AssignmentTestingUtil() {}

  public static void waitForRegionToBeInTransition(final HBaseTestingUtility util,
      final RegionInfo hri) throws Exception {
    while (!getMaster(util).getAssignmentManager().getRegionStates().isRegionInTransition(hri)) {
      Threads.sleep(10);
    }
  }

  public static void waitForRsToBeDead(final HBaseTestingUtility util,
      final ServerName serverName) throws Exception {
    util.waitFor(60000, new ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() {
        return getMaster(util).getServerManager().isServerDead(serverName);
      }

      @Override
      public String explainFailure() {
        return "Server " + serverName + " is not dead";
      }
    });
  }

  public static void stopRs(final HBaseTestingUtility util, final ServerName serverName)
      throws Exception {
    LOG.info("STOP REGION SERVER " + serverName);
    util.getMiniHBaseCluster().stopRegionServer(serverName);
    waitForRsToBeDead(util, serverName);
  }

  public static void killRs(final HBaseTestingUtility util, final ServerName serverName)
      throws Exception {
    LOG.info("KILL REGION SERVER " + serverName);
    util.getMiniHBaseCluster().killRegionServer(serverName);
    waitForRsToBeDead(util, serverName);
  }

  public static void crashRs(final HBaseTestingUtility util, final ServerName serverName,
      final boolean kill) throws Exception {
    if (kill) {
      killRs(util, serverName);
    } else {
      stopRs(util, serverName);
    }
  }

  public static ServerName crashRsWithRegion(final HBaseTestingUtility util,
      final RegionInfo hri, final boolean kill) throws Exception {
    ServerName serverName = getServerHoldingRegion(util, hri);
    crashRs(util, serverName, kill);
    return serverName;
  }

  public static ServerName getServerHoldingRegion(final HBaseTestingUtility util,
      final RegionInfo hri) throws Exception {
    ServerName serverName = util.getMiniHBaseCluster().getServerHoldingRegion(
      hri.getTable(), hri.getRegionName());
    ServerName amServerName = getMaster(util).getAssignmentManager().getRegionStates()
      .getRegionServerOfRegion(hri);

    // Make sure AM and MiniCluster agrees on the Server holding the region
    // and that the server is online.
    assertEquals(amServerName, serverName);
    assertEquals(true, getMaster(util).getServerManager().isServerOnline(serverName));
    return serverName;
  }

  public static boolean isServerHoldingMeta(final HBaseTestingUtility util,
      final ServerName serverName) throws Exception {
    for (RegionInfo hri: getMetaRegions(util)) {
      if (serverName.equals(getServerHoldingRegion(util, hri))) {
        return true;
      }
    }
    return false;
  }

  public static Set<RegionInfo> getMetaRegions(final HBaseTestingUtility util) {
    return getMaster(util).getAssignmentManager().getMetaRegionSet();
  }

  private static HMaster getMaster(final HBaseTestingUtility util) {
    return util.getMiniHBaseCluster().getMaster();
  }

  public static boolean waitForAssignment(AssignmentManager am, RegionInfo regionInfo)
      throws IOException {
    // This method can be called before the regionInfo has made it into the regionStateMap
    // so wait around here a while.
    Waiter.waitFor(am.getConfiguration(), 10000,
      () -> am.getRegionStates().getRegionStateNode(regionInfo) != null);
    RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(regionInfo);
    // Wait until the region has already been open, or we have a TRSP along with it.
    Waiter.waitFor(am.getConfiguration(), 30000,
      () -> regionNode.isInState(State.OPEN) || regionNode.isInTransition());
    TransitRegionStateProcedure proc = regionNode.getProcedure();
    regionNode.lock();
    try {
      if (regionNode.isInState(State.OPEN)) {
        return true;
      }
      proc = regionNode.getProcedure();
    } finally {
      regionNode.unlock();
    }
    assertNotNull(proc);
    ProcedureSyncWait.waitForProcedureToCompleteIOE(am.getMaster().getMasterProcedureExecutor(),
      proc, 5L * 60 * 1000);
    return true;
  }

  public static void insertData(final HBaseTestingUtility UTIL, final TableName tableName,
    int rowCount, int startRowNum, String... cfs) throws IOException {
    insertData(UTIL, tableName, rowCount, startRowNum, false, cfs);
  }

  public static void insertData(final HBaseTestingUtility UTIL, final TableName tableName,
    int rowCount, int startRowNum, boolean flushOnce, String... cfs) throws IOException {
    Table t = UTIL.getConnection().getTable(tableName);
    Put p;
    for (int i = 0; i < rowCount / 2; i++) {
      p = new Put(Bytes.toBytes("" + (startRowNum + i)));
      for (String cf : cfs) {
        p.addColumn(Bytes.toBytes(cf), Bytes.toBytes("q"), Bytes.toBytes(i));
      }
      t.put(p);
      p = new Put(Bytes.toBytes("" + (startRowNum + rowCount - i - 1)));
      for (String cf : cfs) {
        p.addColumn(Bytes.toBytes(cf), Bytes.toBytes("q"), Bytes.toBytes(i));
      }
      t.put(p);
      if (i % 5 == 0 && !flushOnce) {
        UTIL.getAdmin().flush(tableName);
      }
    }
    if (flushOnce) {
      UTIL.getAdmin().flush(tableName);
    }
  }
}
