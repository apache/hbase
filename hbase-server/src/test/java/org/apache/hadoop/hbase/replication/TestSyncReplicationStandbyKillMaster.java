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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplicationStandbyKillMaster extends SyncReplicationTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSyncReplicationStandbyKillMaster.class);

  private final long SLEEP_TIME = 2000;

  private final int COUNT = 1000;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSyncReplicationStandbyKillMaster.class);

  @Test
  public void testStandbyKillMaster() throws Exception {
    MasterFileSystem mfs = UTIL2.getHBaseCluster().getMaster().getMasterFileSystem();
    Path remoteWALDir = getRemoteWALDir(mfs, PEER_ID);
    assertFalse(mfs.getWALFileSystem().exists(remoteWALDir));
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
        SyncReplicationState.STANDBY);
    assertTrue(mfs.getWALFileSystem().exists(remoteWALDir));
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
        SyncReplicationState.ACTIVE);

    // Disable async replication and write data, then shutdown
    UTIL1.getAdmin().disableReplicationPeer(PEER_ID);
    write(UTIL1, 0, COUNT);
    UTIL1.shutdownMiniCluster();

    Thread t = new Thread(() -> {
      try {
        Thread.sleep(SLEEP_TIME);
        UTIL2.getMiniHBaseCluster().getMaster().stop("Stop master for test");
      } catch (Exception e) {
        LOG.error("Failed to stop master", e);
      }
    });
    t.start();

    // Transit standby to DA to replay logs
    try {
      UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
          SyncReplicationState.DOWNGRADE_ACTIVE);
    } catch (Exception e) {
      LOG.error("Failed to transit standby cluster to " + SyncReplicationState.DOWNGRADE_ACTIVE);
    }

    while (UTIL2.getAdmin().getReplicationPeerSyncReplicationState(PEER_ID)
        != SyncReplicationState.DOWNGRADE_ACTIVE) {
      Thread.sleep(SLEEP_TIME);
    }
    verify(UTIL2, 0, COUNT);
  }
}
