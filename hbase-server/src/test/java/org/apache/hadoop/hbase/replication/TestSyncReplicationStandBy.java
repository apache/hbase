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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplicationStandBy extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationStandBy.class);

  @FunctionalInterface
  private interface TableAction {

    void call(Table table) throws IOException;
  }

  private void assertDisallow(Table table, TableAction action) throws IOException {
    try {
      action.call(table);
    } catch (DoNotRetryIOException | RetriesExhaustedException e) {
      // expected
      assertThat(e.getMessage(), containsString("STANDBY"));
    }
  }

  @Test
  public void testStandby() throws Exception {
    MasterFileSystem mfs = UTIL2.getHBaseCluster().getMaster().getMasterFileSystem();
    Path remoteWALDir = getRemoteWALDir(mfs, PEER_ID);
    assertFalse(mfs.getWALFileSystem().exists(remoteWALDir));
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    assertTrue(mfs.getWALFileSystem().exists(remoteWALDir));
    try (Table table = UTIL2.getConnection().getTable(TABLE_NAME)) {
      assertDisallow(table, t -> t.get(new Get(Bytes.toBytes("row"))));
      assertDisallow(table,
        t -> t.put(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("row"))));
      assertDisallow(table, t -> t.delete(new Delete(Bytes.toBytes("row"))));
      assertDisallow(table, t -> t.incrementColumnValue(Bytes.toBytes("row"), CF, CQ, 1));
      assertDisallow(table,
        t -> t.append(new Append(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("row"))));
      assertDisallow(table,
        t -> t.get(Arrays.asList(new Get(Bytes.toBytes("row")), new Get(Bytes.toBytes("row1")))));
      assertDisallow(table,
        t -> t.put(
          Arrays.asList(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("row")),
            new Put(Bytes.toBytes("row1")).addColumn(CF, CQ, Bytes.toBytes("row1")))));
      assertDisallow(table, t -> t.mutateRow(new RowMutations(Bytes.toBytes("row"))
          .add((Mutation) new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("row")))));
    }
    // We should still allow replication writes
    writeAndVerifyReplication(UTIL1, UTIL2, 0, 100);

    // Remove the peers in ACTIVE & STANDBY cluster.
    FileSystem fs2 = remoteWALDir2.getFileSystem(UTIL2.getConfiguration());
    Assert.assertTrue(fs2.exists(getRemoteWALDir(remoteWALDir2, PEER_ID)));

    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    Assert.assertFalse(fs2.exists(getRemoteWALDir(remoteWALDir2, PEER_ID)));
    Assert.assertFalse(fs2.exists(getReplayRemoteWALs(remoteWALDir2, PEER_ID)));

    UTIL1.getAdmin().removeReplicationPeer(PEER_ID);
    verifyRemovedPeer(PEER_ID, remoteWALDir1, UTIL1);

    // Peer remoteWAL dir will be renamed to replay WAL dir when transit from S to DA, and the
    // replay WAL dir will be removed after replaying all WALs, so create a emtpy dir here to test
    // whether the removeReplicationPeer would remove the remoteWAL dir.
    fs2.create(getRemoteWALDir(remoteWALDir2, PEER_ID));
    fs2.create(getReplayRemoteWALs(remoteWALDir2, PEER_ID));
    Assert.assertTrue(fs2.exists(getRemoteWALDir(remoteWALDir2, PEER_ID)));
    Assert.assertTrue(fs2.exists(getReplayRemoteWALs(remoteWALDir2, PEER_ID)));
    UTIL2.getAdmin().removeReplicationPeer(PEER_ID);
    verifyRemovedPeer(PEER_ID, remoteWALDir2, UTIL2);
  }
}
