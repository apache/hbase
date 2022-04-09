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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.replication.DisablePeerProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;

/**
 * This UT is used to make sure that we will not accidentally change the way to generate online
 * servers. See HBASE-25774 and HBASE-25032 for more details.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestRefreshPeerWhileRegionServerRestarts extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRefreshPeerWhileRegionServerRestarts.class);

  private static CountDownLatch ARRIVE;

  private static CountDownLatch RESUME;

  public static final class RegionServerForTest extends HRegionServer {

    public RegionServerForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected void tryRegionServerReport(long reportStartTime, long reportEndTime)
      throws IOException {
      if (ARRIVE != null) {
        ARRIVE.countDown();
        ARRIVE = null;
        try {
          RESUME.await();
        } catch (InterruptedException e) {
        }
      }
      super.tryRegionServerReport(reportStartTime, reportEndTime);
    }
  }

  @Test
  public void testRestart() throws Exception {
    UTIL1.getMiniHBaseCluster().getConfiguration().setClass(HConstants.REGION_SERVER_IMPL,
      RegionServerForTest.class, HRegionServer.class);
    CountDownLatch arrive = new CountDownLatch(1);
    ARRIVE = arrive;
    RESUME = new CountDownLatch(1);
    // restart a new region server, and wait until it finish initialization and want to call
    // regionServerReport, so it will load the peer state to peer cache.
    Future<HRegionServer> regionServerFuture = ForkJoinPool.commonPool()
      .submit(() -> UTIL1.getMiniHBaseCluster().startRegionServer().getRegionServer());
    ARRIVE.await();
    // change the peer state, wait until it reach the last state, where we have already get the
    // region server list for refreshing
    Future<Void> future = hbaseAdmin.disableReplicationPeerAsync(PEER_ID2);
    try {
      UTIL1.waitFor(30000, () -> {
        for (Procedure<?> proc : UTIL1.getMiniHBaseCluster().getMaster().getProcedures()) {
          if (proc instanceof DisablePeerProcedure) {
            return ((DisablePeerProcedure) proc)
              .getCurrentStateId() == PeerModificationState.POST_PEER_MODIFICATION_VALUE;
          }
        }
        return false;
      });
    } finally {
      // let the new region server go
      RESUME.countDown();
    }
    // wait the disable peer operation to finish
    future.get();
    // assert that the peer cache on the new region server has also been refreshed
    ReplicationPeer peer = regionServerFuture.get().getReplicationSourceService()
      .getReplicationPeers().getPeer(PEER_ID2);
    assertEquals(PeerState.DISABLED, peer.getPeerState());
  }
}
