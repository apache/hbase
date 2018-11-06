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

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerSyncReplicationStateTransitionState.REOPEN_ALL_REGIONS_IN_PEER_VALUE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.master.replication.TransitPeerSyncReplicationStateProcedure;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-21441.
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplicationNewRSJoinBetweenRefreshes extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationNewRSJoinBetweenRefreshes.class);

  private static boolean HALT;

  private static CountDownLatch ARRIVE;

  private static CountDownLatch RESUME;

  public static final class HaltCP implements RegionServerObserver, RegionServerCoprocessor {

    @Override
    public Optional<RegionServerObserver> getRegionServerObserver() {
      return Optional.of(this);
    }

    @Override
    public void postExecuteProcedures(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
        throws IOException {
      synchronized (HaltCP.class) {
        if (!HALT) {
          return;
        }
        UTIL1.getMiniHBaseCluster().getMaster().getProcedures().stream()
          .filter(p -> p instanceof TransitPeerSyncReplicationStateProcedure)
          .filter(p -> !p.isFinished()).map(p -> (TransitPeerSyncReplicationStateProcedure) p)
          .findFirst().ifPresent(proc -> {
            // this is the next state of REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_BEGIN_VALUE
            if (proc.getCurrentStateId() == REOPEN_ALL_REGIONS_IN_PEER_VALUE) {
              // tell the main thread to start a new region server
              ARRIVE.countDown();
              try {
                // wait for the region server to online
                RESUME.await();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              HALT = false;
            }
          });
      }
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL1.getConfiguration().setClass(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      HaltCP.class, RegionServerObserver.class);
    SyncReplicationTestBase.setUp();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);

    ARRIVE = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    HALT = true;
    Thread t = new Thread(() -> {
      try {
        UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
          SyncReplicationState.DOWNGRADE_ACTIVE);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    t.start();
    ARRIVE.await();
    UTIL1.getMiniHBaseCluster().startRegionServer();
    RESUME.countDown();
    t.join();
    assertEquals(SyncReplicationState.DOWNGRADE_ACTIVE,
      UTIL1.getAdmin().getReplicationPeerSyncReplicationState(PEER_ID));
  }
}
