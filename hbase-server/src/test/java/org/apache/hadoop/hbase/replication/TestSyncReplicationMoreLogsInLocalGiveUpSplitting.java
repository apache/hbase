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
package org.apache.hadoop.hbase.replication;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.DualAsyncFSWAL;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.SyncReplicationWALProvider;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplicationMoreLogsInLocalGiveUpSplitting extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationMoreLogsInLocalGiveUpSplitting.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestSyncReplicationMoreLogsInLocalGiveUpSplitting.class);

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL1.getConfiguration().setClass(SyncReplicationWALProvider.DUAL_WAL_IMPL,
      DualAsyncFSWALForTest.class, DualAsyncFSWAL.class);
    UTIL2.getConfiguration().setClass(SyncReplicationWALProvider.DUAL_WAL_IMPL,
      DualAsyncFSWALForTest.class, DualAsyncFSWAL.class);
    SyncReplicationTestBase.setUp();
  }

  @Test
  public void testSplitLog() throws Exception {
    UTIL1.getAdmin().disableReplicationPeer(PEER_ID);
    UTIL2.getAdmin().disableReplicationPeer(PEER_ID);
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);
    try (Table table = UTIL1.getConnection().getTable(TABLE_NAME)) {
      table.put(new Put(Bytes.toBytes(0)).addColumn(CF, CQ, Bytes.toBytes(0)));
    }
    HRegionServer rs = UTIL1.getRSForFirstRegionInTable(TABLE_NAME);
    DualAsyncFSWALForTest wal =
      (DualAsyncFSWALForTest) rs.getWAL(RegionInfoBuilder.newBuilder(TABLE_NAME).build());
    wal.setRemoteBroken();
    wal.suspendLogRoll();
    try (AsyncConnection conn =
      ConnectionFactory.createAsyncConnection(UTIL1.getConfiguration()).get()) {
      AsyncTable<?> table = conn.getTableBuilder(TABLE_NAME).setMaxAttempts(1)
        .setWriteRpcTimeout(5, TimeUnit.SECONDS).build();
      try {
        table.put(new Put(Bytes.toBytes(1)).addColumn(CF, CQ, Bytes.toBytes(1))).get();
        fail("Should fail since the rs will hang and we will get a rpc timeout");
      } catch (ExecutionException e) {
        // expected
        LOG.info("Expected error:", e);
      }
    }
    wal.waitUntilArrive();
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    wal.resumeLogRoll();
    try (Table table = UTIL2.getConnection().getTable(TABLE_NAME)) {
      assertEquals(0, Bytes.toInt(table.get(new Get(Bytes.toBytes(0))).getValue(CF, CQ)));
      // we failed to write this entry to remote so it should not exist
      assertFalse(table.exists(new Get(Bytes.toBytes(1))));
    }
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    // make sure that the region is online. We can not use waitTableAvailable since the table in
    // stand by state can not be read from client.
    try (Table table = UTIL1.getConnection().getTable(TABLE_NAME)) {
      try {
        table.exists(new Get(Bytes.toBytes(0)));
      } catch (DoNotRetryIOException | RetriesExhaustedException e) {
        // expected
        assertThat(e.getMessage(), containsString("STANDBY"));
      }
    }
    HRegion region = UTIL1.getMiniHBaseCluster().getRegions(TABLE_NAME).get(0);
    // we give up splitting the whole wal file so this record will also be gone.
    assertTrue(region.get(new Get(Bytes.toBytes(0))).isEmpty());
    UTIL2.getAdmin().enableReplicationPeer(PEER_ID);
    // finally it should be replicated back
    waitUntilReplicationDone(UTIL1, 1);
  }
}
