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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;

/**
 * Test for HBASE-30142. Verifies that {@code scheduleSCPsForUnknownServers} (i.e. the
 * recoverUnknown HBCK command) does not throw a NullPointerException when a region's
 * {@code regionLocation} is null while its state is something other than OFFLINE.
 * <p/>
 * Before HBASE-30142, {@link ServerManager#isServerUnknown(org.apache.hadoop.hbase.ServerName)}
 * returned {@code true} for a {@code null} server name, so a region whose location had been
 * temporarily nulled (e.g. between region transitions or while marking it FAILED_OPEN /
 * ABNORMALLY_CLOSED) was treated as living on an "unknown" server. The downstream call to
 * {@code shouldSubmitSCP(null)} then dereferenced the null and crashed.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestRecoverUnknownWithNullRegionLocation {

  private static HBaseTestingUtil UTIL;
  private static final TableName TABLE_NAME =
    TableName.valueOf("TestRecoverUnknownWithNullRegionLocation");
  private static final byte[] FAMILY = Bytes.toBytes("cf");

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtil();
    UTIL.startMiniCluster(2);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    if (UTIL != null) {
      UTIL.shutdownMiniCluster();
    }
  }

  /**
   * Drive the region into ABNORMALLY_CLOSED through the
   * {@link AssignmentManager#regionClosedAbnormally(RegionStateNode)} path (the same path SCP
   * uses). That method also nulls {@code regionLocation} while leaving state non-OFFLINE. Then call
   * {@code scheduleSCPsForUnknownServers} and assert no NPE.
   */
  @Test
  public void testRecoverUnknownWithAbnormallyClosedRegion() throws Exception {
    try (Table ignored = UTIL.createTable(TABLE_NAME, FAMILY)) {
      UTIL.waitTableAvailable(TABLE_NAME);

      HMaster master = UTIL.getMiniHBaseCluster().getMaster();
      AssignmentManager am = master.getAssignmentManager();
      List<RegionInfo> regions = am.getTableRegions(TABLE_NAME, true);
      assertFalse(regions.isEmpty(), "expected at least one region");
      RegionStateNode node = am.getRegionStates().getRegionStateNode(regions.get(0));
      assertNotNull(node, "expected a region state node for the test region");

      node.lock();
      try {
        am.regionClosedAbnormally(node).get();
      } finally {
        node.unlock();
      }

      assertTrue(node.isInState(RegionState.State.ABNORMALLY_CLOSED),
        "regionClosedAbnormally must move state to ABNORMALLY_CLOSED");
      assertNull(node.getRegionLocation(), "regionClosedAbnormally must null out the location");

      MasterProtos.ScheduleSCPsForUnknownServersResponse response =
        master.getMasterRpcServices().scheduleSCPsForUnknownServers(null,
          MasterProtos.ScheduleSCPsForUnknownServersRequest.newBuilder().build());
      assertEquals(0, response.getPidCount(),
        "no SCPs should be scheduled for an ABNORMALLY_CLOSED region with null location");
    }
  }
}
