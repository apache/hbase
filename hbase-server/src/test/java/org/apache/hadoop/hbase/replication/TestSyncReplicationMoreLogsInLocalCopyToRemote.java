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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
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
public class TestSyncReplicationMoreLogsInLocalCopyToRemote extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationMoreLogsInLocalCopyToRemote.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestSyncReplicationMoreLogsInLocalCopyToRemote.class);

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
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);
    HRegionServer rs = UTIL1.getRSForFirstRegionInTable(TABLE_NAME);
    DualAsyncFSWALForTest wal =
      (DualAsyncFSWALForTest) rs.getWAL(RegionInfoBuilder.newBuilder(TABLE_NAME).build());
    wal.setRemoteBroken();
    try (AsyncConnection conn =
      ConnectionFactory.createAsyncConnection(UTIL1.getConfiguration()).get()) {
      AsyncTable<?> table = conn.getTableBuilder(TABLE_NAME).setMaxAttempts(1).build();
      try {
        table.put(new Put(Bytes.toBytes(0)).addColumn(CF, CQ, Bytes.toBytes(0))).get();
        fail("Should fail since the rs will crash and we will not retry");
      } catch (ExecutionException e) {
        // expected
        LOG.info("Expected error:", e);
      }
    }
    UTIL1.waitFor(60000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        try (Table table = UTIL1.getConnection().getTable(TABLE_NAME)) {
          return table.exists(new Get(Bytes.toBytes(0)));
        }
      }

      @Override
      public String explainFailure() throws Exception {
        return "The row is still not available";
      }
    });
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    // We should have copied the local log to remote, so we should be able to get the value
    try (Table table = UTIL2.getConnection().getTable(TABLE_NAME)) {
      assertEquals(0, Bytes.toInt(table.get(new Get(Bytes.toBytes(0))).getValue(CF, CQ)));
    }
  }
}
