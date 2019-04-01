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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.LogRoller;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Testcase to confirm that serial replication will not be stuck when using along with synchronous
 * replication. See HBASE-21486 for more details.
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestSerialSyncReplication extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSerialSyncReplication.class);

  @Test
  public void test() throws Exception {
    // change to serial
    UTIL1.getAdmin().updateReplicationPeerConfig(PEER_ID, ReplicationPeerConfig
      .newBuilder(UTIL1.getAdmin().getReplicationPeerConfig(PEER_ID)).setSerial(true).build());
    UTIL2.getAdmin().updateReplicationPeerConfig(PEER_ID, ReplicationPeerConfig
      .newBuilder(UTIL2.getAdmin().getReplicationPeerConfig(PEER_ID)).setSerial(true).build());

    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);

    UTIL2.getAdmin().disableReplicationPeer(PEER_ID);

    writeAndVerifyReplication(UTIL1, UTIL2, 0, 100);

    MasterFileSystem mfs = UTIL2.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path remoteWALDir = ReplicationUtils.getPeerRemoteWALDir(
      new Path(mfs.getWALRootDir(), ReplicationUtils.REMOTE_WAL_DIR_NAME), PEER_ID);
    FileStatus[] remoteWALStatus = mfs.getWALFileSystem().listStatus(remoteWALDir);
    assertEquals(1, remoteWALStatus.length);
    Path remoteWAL = remoteWALStatus[0].getPath();
    assertThat(remoteWAL.getName(), endsWith(ReplicationUtils.SYNC_WAL_SUFFIX));
    // roll the wal writer, so that we will delete the remore wal. This is used to make sure that we
    // will not replay this wal when transiting to DA.
    for (RegionServerThread t : UTIL1.getMiniHBaseCluster().getRegionServerThreads()) {
      LogRoller roller = t.getRegionServer().getWalRoller();
      roller.requestRollAll();
      roller.waitUntilWalRollFinished();
    }
    waitUntilDeleted(UTIL2, remoteWAL);

    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    // let's reopen the region
    RegionInfo region = Iterables.getOnlyElement(UTIL2.getAdmin().getRegions(TABLE_NAME));
    HRegionServer target = UTIL2.getOtherRegionServer(UTIL2.getRSForFirstRegionInTable(TABLE_NAME));
    UTIL2.getAdmin().move(region.getEncodedNameAsBytes(), target.getServerName());
    // here we will remove all the pending wals. This is not a normal operation sequence but anyway,
    // user could do this.
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    // transit back to DA
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);

    UTIL2.getAdmin().enableReplicationPeer(PEER_ID);
    // make sure that the async replication still works
    writeAndVerifyReplication(UTIL2, UTIL1, 100, 200);
  }
}
