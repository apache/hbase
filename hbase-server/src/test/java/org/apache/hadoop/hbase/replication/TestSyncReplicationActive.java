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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplicationActive extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSyncReplicationActive.class);


  @Test
  public void testActive() throws Exception {
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);

    // confirm that peer with state A will reject replication request.
    verifyReplicationRequestRejection(UTIL1, true);
    verifyReplicationRequestRejection(UTIL2, false);

    UTIL1.getAdmin().disableReplicationPeer(PEER_ID);
    write(UTIL1, 0, 100);
    Thread.sleep(2000);
    // peer is disabled so no data have been replicated
    verifyNotReplicatedThroughRegion(UTIL2, 0, 100);

    // Ensure that there's no cluster id in remote log entries.
    verifyNoClusterIdInRemoteLog(UTIL2, PEER_ID);

    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    // confirm that peer with state DA will reject replication request.
    verifyReplicationRequestRejection(UTIL2, true);
    // confirm that the data is there after we convert the peer to DA
    verify(UTIL2, 0, 100);

    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);

    writeAndVerifyReplication(UTIL2, UTIL1, 100, 200);

    // shutdown the cluster completely
    UTIL1.shutdownMiniCluster();
    // confirm that we can convert to DA even if the remote slave cluster is down
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    // confirm that peer with state DA will reject replication request.
    verifyReplicationRequestRejection(UTIL2, true);
    write(UTIL2, 200, 300);
  }

  private void verifyNoClusterIdInRemoteLog(HBaseTestingUtility utility, String peerId)
      throws Exception {
    FileSystem fs2 = utility.getTestFileSystem();
    Path remoteDir =
        new Path(utility.getMiniHBaseCluster().getMaster().getMasterFileSystem().getRootDir(),
            "remoteWALs").makeQualified(fs2.getUri(), fs2.getWorkingDirectory());
    FileStatus[] files = fs2.listStatus(new Path(remoteDir, peerId));
    Assert.assertTrue(files.length > 0);
    for (FileStatus file : files) {
      try (Reader reader =
          WALFactory.createReader(fs2, file.getPath(), utility.getConfiguration())) {
        Entry entry = reader.next();
        Assert.assertTrue(entry != null);
        while (entry != null) {
          Assert.assertEquals(entry.getKey().getClusterIds().size(), 0);
          entry = reader.next();
        }
      }
    }
  }
}
