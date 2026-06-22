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
package org.apache.hadoop.hbase.replication;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ReplicationTests.TAG)
@Tag(LargeTests.TAG)
public class TestSyncReplicationStandbyKillMaster extends SyncReplicationTestBaseNoBeforeAll {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestSyncReplicationStandbyKillMaster.class);

  private static final int COUNT = 1000;

  private static volatile boolean KILL_MASTER = false;

  public static final class TransitCP implements MasterCoprocessor, MasterObserver {

    @Override
    public void preTransitReplicationPeerSyncReplicationState(
      ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId, SyncReplicationState state)
      throws IOException {
      if (KILL_MASTER) {
        KILL_MASTER = false;
        UTIL2.getMiniHBaseCluster().getMaster().abort("Stop master for test");
      }
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL2.getConfiguration().set(MASTER_COPROCESSOR_CONF_KEY, TransitCP.class.getName());
    startClusters();
  }

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

    KILL_MASTER = true;
    // Transit standby to DA to replay logs
    try {
      UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
        SyncReplicationState.DOWNGRADE_ACTIVE);
    } catch (Exception e) {
      // we will kill master in the transit procedure's preCheck method, not in the rpc thread, so
      // we can not know whether we could successfully get the returned proc id. If we can get the
      // proc id, the call should be succeeded and we will not arrive here, and the below await
      // check will pass immediately, if not, we will get this exception and then we need to rely on
      // the below await check to make sure the transition is successfully finished.
      LOG.warn("Failed to transit standby cluster to {}", SyncReplicationState.DOWNGRADE_ACTIVE);
    }

    await().atMost(Duration.ofMinutes(3))
      .untilAsserted(() -> assertEquals(SyncReplicationState.DOWNGRADE_ACTIVE,
        UTIL2.getAdmin().getReplicationPeerSyncReplicationState(PEER_ID)));

    verify(UTIL2, 0, COUNT);
  }
}
