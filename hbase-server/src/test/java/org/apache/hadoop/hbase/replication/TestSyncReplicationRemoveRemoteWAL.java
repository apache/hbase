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

import static org.hamcrest.CoreMatchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplicationRemoveRemoteWAL extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationRemoveRemoteWAL.class);

  private void waitUntilDeleted(Path remoteWAL) throws Exception {
    MasterFileSystem mfs = UTIL2.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    UTIL1.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return !mfs.getWALFileSystem().exists(remoteWAL);
      }

      @Override
      public String explainFailure() throws Exception {
        return remoteWAL + " has not been deleted yet";
      }
    });
  }

  @Test
  public void testRemoveRemoteWAL() throws Exception {
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);

    MasterFileSystem mfs = UTIL2.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path remoteWALDir = ReplicationUtils.getPeerRemoteWALDir(
      new Path(mfs.getWALRootDir(), ReplicationUtils.REMOTE_WAL_DIR_NAME), PEER_ID);
    FileStatus[] remoteWALStatus = mfs.getWALFileSystem().listStatus(remoteWALDir);
    assertEquals(1, remoteWALStatus.length);
    Path remoteWAL = remoteWALStatus[0].getPath();
    assertThat(remoteWAL.getName(), endsWith(ReplicationUtils.SYNC_WAL_SUFFIX));
    writeAndVerifyReplication(UTIL1, UTIL2, 0, 100);

    HRegionServer rs = UTIL1.getRSForFirstRegionInTable(TABLE_NAME);
    rs.getWalRoller().requestRollAll();
    // The replicated wal file should be deleted finally
    waitUntilDeleted(remoteWAL);
    remoteWALStatus = mfs.getWALFileSystem().listStatus(remoteWALDir);
    assertEquals(1, remoteWALStatus.length);
    remoteWAL = remoteWALStatus[0].getPath();
    assertThat(remoteWAL.getName(), endsWith(ReplicationUtils.SYNC_WAL_SUFFIX));

    UTIL1.getAdmin().disableReplicationPeer(PEER_ID);
    write(UTIL1, 100, 200);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);

    // should still be there since the peer is disabled and we haven't replicated the data yet
    assertTrue(mfs.getWALFileSystem().exists(remoteWAL));

    UTIL1.getAdmin().enableReplicationPeer(PEER_ID);
    waitUntilReplicationDone(UTIL2, 200);
    verifyThroughRegion(UTIL2, 100, 200);

    // Confirm that we will also remove the remote wal files in DA state
    waitUntilDeleted(remoteWAL);
  }
}
