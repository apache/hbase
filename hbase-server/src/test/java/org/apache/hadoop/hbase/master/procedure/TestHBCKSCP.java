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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;


/**
 * Test of the HBCK-version of SCP.
 * The HBCKSCP is an SCP only it reads hbase:meta for list of Regions that were
 * on the server-to-process rather than consult Master in-memory-state.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestHBCKSCP extends TestSCPBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBCKSCP.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBCKSCP.class);
  @Rule
  public TestName name = new TestName();

  @Test
  public void test() throws Exception {
    // we are about to do one for it?
    MiniHBaseCluster cluster = this.util.getHBaseCluster();

    // Assert that we have three RegionServers. Test depends on there being multiple.
    assertEquals(RS_COUNT, cluster.getLiveRegionServerThreads().size());

    int count;
    try (Table table = createTable(TableName.valueOf(this.name.getMethodName()))) {
      // Load the table with a bit of data so some logs to split and some edits in each region.
      this.util.loadTable(table, HBaseTestingUtility.COLUMNS[0]);
      count = util.countRows(table);
    }
    assertTrue("expected some rows", count > 0);

    // Make the test easier by not working on server hosting meta...
    // Find another RS. Purge it from Master memory w/o running SCP (if
    // SCP runs, it will clear entries from hbase:meta which frustrates
    // our attempt at manufacturing 'Unknown Servers' condition).
    int metaIndex = this.util.getMiniHBaseCluster().getServerWithMeta();
    int rsIndex = (metaIndex + 1) % RS_COUNT;
    ServerName rsServerName = cluster.getRegionServer(rsIndex).getServerName();
    HMaster master = cluster.getMaster();
    // Get a Region that is on the server.
    RegionInfo rsRI = master.getAssignmentManager().getRegionsOnServer(rsServerName).get(0);
    Result r = MetaTableAccessor.getRegionResult(master.getConnection(), rsRI.getRegionName());
    // Assert region is OPEN.
    assertEquals(RegionState.State.OPEN.toString(),
        Bytes.toString(r.getValue(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER)));
    ServerName serverName = MetaTableAccessor.getServerName(r, 0);
    assertEquals(rsServerName, serverName);
    // moveFrom adds to dead servers and adds it to processing list only we will
    // not be processing this server 'normally'. Remove it from processing by
    // calling 'finish' and then remove it from dead servers so rsServerName
    // becomes an 'Unknown Server' even though it is still around.
    LOG.info("Killing {}", rsServerName);
    cluster.killRegionServer(rsServerName);

    master.getServerManager().moveFromOnlineToDeadServers(rsServerName);
    master.getServerManager().getDeadServers().finish(rsServerName);
    master.getServerManager().getDeadServers().removeDeadServer(rsServerName);
    master.getAssignmentManager().getRegionStates().removeServer(rsServerName);
    // Kill the server. Nothing should happen since an 'Unknown Server' as far
    // as the Master is concerned; i.e. no SCP.
    HRegionServer hrs = cluster.getRegionServer(rsServerName);
    while (!hrs.isStopped()) {
      Threads.sleep(10);
    }
    LOG.info("Dead {}", rsServerName);
    // Now assert still references in hbase:meta to the 'dead' server -- they haven't been
    // cleaned up by an SCP or by anything else.
    assertTrue(searchMeta(master, rsServerName));
    // Assert region is OPEN on dead server still.
    r = MetaTableAccessor.getRegionResult(master.getConnection(), rsRI.getRegionName());
    assertEquals(RegionState.State.OPEN.toString(),
        Bytes.toString(r.getValue(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER)));
    serverName = MetaTableAccessor.getServerName(r, 0);
    assertNotNull(cluster.getRegionServer(serverName));
    assertEquals(rsServerName, serverName);

    // I now have 'Unknown Server' references in hbase:meta; i.e. Server references
    // with no corresponding SCP. Queue one.
    long pid = scheduleHBCKSCP(rsServerName, master);
    assertNotEquals(Procedure.NO_PROC_ID, pid);
    while (master.getMasterProcedureExecutor().getActiveProcIds().contains(pid)) {
      Threads.sleep(10);
    }
    // After SCP, assert region is OPEN on new server.
    r = MetaTableAccessor.getRegionResult(master.getConnection(), rsRI.getRegionName());
    assertEquals(RegionState.State.OPEN.toString(),
        Bytes.toString(r.getValue(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER)));
    serverName = MetaTableAccessor.getServerName(r, 0);
    assertNotNull(cluster.getRegionServer(serverName));
    assertNotEquals(rsServerName, serverName);
    // Make sure no mention of old server post SCP.
    assertFalse(searchMeta(master, rsServerName));
  }

  protected long scheduleHBCKSCP(ServerName rsServerName, HMaster master) throws ServiceException {
    MasterProtos.ScheduleServerCrashProcedureResponse response =
        master.getMasterRpcServices().scheduleServerCrashProcedure(null,
            MasterProtos.ScheduleServerCrashProcedureRequest.newBuilder().
                addServerName(ProtobufUtil.toServerName(rsServerName)).build());
    assertEquals(1, response.getPidCount());
    long pid = response.getPid(0);
    return pid;
  }

  /**
   * @return True if we find reference to <code>sn</code> in meta table.
   */
  private boolean searchMeta(HMaster master, ServerName sn) throws IOException {
    List<Pair<RegionInfo, ServerName>> ps =
      MetaTableAccessor.getTableRegionsAndLocations(master.getConnection(), null);
    for (Pair<RegionInfo, ServerName> p: ps) {
      if (p.getSecond().equals(sn)) {
        return true;
      }
    }
    return false;
  }
}
