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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.DualAsyncFSWAL;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.SyncReplicationTestBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-20456.
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplicationShipperQuit extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationShipperQuit.class);

  @Test
  public void testShipperQuitWhenDA() throws Exception {
    // set to serial replication
    UTIL1.getAdmin().updateReplicationPeerConfig(PEER_ID, ReplicationPeerConfig
      .newBuilder(UTIL1.getAdmin().getReplicationPeerConfig(PEER_ID)).setSerial(true).build());
    UTIL2.getAdmin().updateReplicationPeerConfig(PEER_ID, ReplicationPeerConfig
      .newBuilder(UTIL2.getAdmin().getReplicationPeerConfig(PEER_ID)).setSerial(true).build());
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);

    writeAndVerifyReplication(UTIL1, UTIL2, 0, 100);
    HRegionServer rs = UTIL1.getRSForFirstRegionInTable(TABLE_NAME);
    DualAsyncFSWAL wal =
      (DualAsyncFSWAL) rs.getWAL(RegionInfoBuilder.newBuilder(TABLE_NAME).build());
    String walGroupId =
      AbstractFSWALProvider.getWALPrefixFromWALName(wal.getCurrentFileName().getName());
    ReplicationSourceShipper shipper =
      ((ReplicationSource) ((Replication) rs.getReplicationSourceService()).getReplicationManager()
        .getSource(PEER_ID)).workerThreads.get(walGroupId);
    assertFalse(shipper.isFinished());

    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    writeAndVerifyReplication(UTIL1, UTIL2, 100, 200);

    ReplicationSource source = (ReplicationSource) ((Replication) rs.getReplicationSourceService())
      .getReplicationManager().getSource(PEER_ID);
    // the peer is serial so here we can make sure that the previous wals have already been
    // replicated, and finally the shipper should be removed from the worker pool
    UTIL1.waitFor(10000, () -> !source.workerThreads.containsKey(walGroupId));
    assertTrue(shipper.isFinished());
  }
}
