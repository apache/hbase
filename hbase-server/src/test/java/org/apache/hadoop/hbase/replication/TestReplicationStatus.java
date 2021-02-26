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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationStatus extends TestReplicationBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationStatus.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationStatus.class);

  static void insertRowsOnSource() throws IOException {
    final byte[] qualName = Bytes.toBytes("q");
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      htable1.put(p);
    }
  }

  /**
   * Test for HBASE-9531.
   * <p/>
   * put a few rows into htable1, which should be replicated to htable2 <br/>
   * create a ClusterStatus instance 'status' from HBaseAdmin <br/>
   * test : status.getLoad(server).getReplicationLoadSourceList() <br/>
   * test : status.getLoad(server).getReplicationLoadSink()
   */
  @Test
  public void testReplicationStatus() throws Exception {
    // This test wants two RS's up. We only run one generally so add one.
    UTIL1.getMiniHBaseCluster().startRegionServer();
    Waiter.waitFor(UTIL1.getConfiguration(), 30000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        return UTIL1.getMiniHBaseCluster().getLiveRegionServerThreads().size() > 1;
      }
    });
    Admin hbaseAdmin = UTIL1.getAdmin();
    // disable peer <= WHY? I DON'T GET THIS DISABLE BUT TEST FAILS W/O IT.
    hbaseAdmin.disableReplicationPeer(PEER_ID2);
    insertRowsOnSource();
    LOG.info("AFTER PUTS");
    // TODO: Change this wait to a barrier. I tried waiting on replication stats to
    // change but sleeping in main thread seems to mess up background replication.
    // HACK! To address flakeyness.
    Threads.sleep(10000);
    ClusterMetrics metrics = hbaseAdmin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
    for (JVMClusterUtil.RegionServerThread thread : UTIL1.getHBaseCluster().
        getRegionServerThreads()) {
      ServerName server = thread.getRegionServer().getServerName();
      assertTrue("" + server, metrics.getLiveServerMetrics().containsKey(server));
      ServerMetrics sm = metrics.getLiveServerMetrics().get(server);
      List<ReplicationLoadSource> rLoadSourceList = sm.getReplicationLoadSourceList();
      ReplicationLoadSink rLoadSink = sm.getReplicationLoadSink();

      // check SourceList only has one entry, because only has one peer
      assertEquals("Failed to get ReplicationLoadSourceList " +
        rLoadSourceList + ", " + server,1, rLoadSourceList.size());
      assertEquals(PEER_ID2, rLoadSourceList.get(0).getPeerID());

      // check Sink exist only as it is difficult to verify the value on the fly
      assertTrue("failed to get ReplicationLoadSink.AgeOfLastShippedOp ",
        (rLoadSink.getAgeOfLastAppliedOp() >= 0));
      assertTrue("failed to get ReplicationLoadSink.TimeStampsOfLastAppliedOp ",
        (rLoadSink.getTimestampsOfLastAppliedOp() >= 0));
    }

    // Stop rs1, then the queue of rs1 will be transfered to rs0
    HRegionServer hrs = UTIL1.getHBaseCluster().getRegionServer(1);
    hrs.stop("Stop RegionServer");
    while(!hrs.isShutDown()) {
      Threads.sleep(100);
    }
    // To be sure it dead and references cleaned up. TODO: Change this to a barrier.
    // I tried waiting on replication stats to change but sleeping in main thread
    // seems to mess up background replication.
    Threads.sleep(10000);
    ServerName server = UTIL1.getHBaseCluster().getRegionServer(0).getServerName();
    List<ReplicationLoadSource> rLoadSourceList = waitOnMetricsReport(1, server);
    // The remaining server should now have two queues -- the original and then the one that was
    // added because of failover. The original should still be PEER_ID2 though.
    assertEquals("Failed ReplicationLoadSourceList " + rLoadSourceList, 2, rLoadSourceList.size());
    assertEquals(PEER_ID2, rLoadSourceList.get(0).getPeerID());
  }

  /**
   * Wait until Master shows metrics counts for ReplicationLoadSourceList that are
   * greater than <code>greaterThan</code> for <code>serverName</code> before
   * returning. We want to avoid case where RS hasn't yet updated Master before
   * allowing test proceed.
   * @param greaterThan size of replicationLoadSourceList must be greater before we proceed
   */
  private List<ReplicationLoadSource> waitOnMetricsReport(int greaterThan, ServerName serverName)
      throws IOException {
    ClusterMetrics metrics = hbaseAdmin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
    List<ReplicationLoadSource> list =
      metrics.getLiveServerMetrics().get(serverName).getReplicationLoadSourceList();
    while(list.size() <= greaterThan) {
      Threads.sleep(1000);
    }
    return list;
  }
}
