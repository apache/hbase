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
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationStatus extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationStatus.class);

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
    Admin hbaseAdmin = UTIL1.getAdmin();
    // disable peer
    hbaseAdmin.disableReplicationPeer(PEER_ID2);

    final byte[] qualName = Bytes.toBytes("q");
    Put p;

    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      htable1.put(p);
    }

    ClusterMetrics metrics = hbaseAdmin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));

    for (JVMClusterUtil.RegionServerThread thread : UTIL1.getHBaseCluster()
      .getRegionServerThreads()) {
      ServerName server = thread.getRegionServer().getServerName();
      ServerMetrics sm = metrics.getLiveServerMetrics().get(server);
      List<ReplicationLoadSource> rLoadSourceList = sm.getReplicationLoadSourceList();
      ReplicationLoadSink rLoadSink = sm.getReplicationLoadSink();

      // check SourceList only has one entry, because only has one peer
      assertEquals("failed to get ReplicationLoadSourceList", 1, rLoadSourceList.size());
      assertEquals(PEER_ID2, rLoadSourceList.get(0).getPeerID());

      // check Sink exist only as it is difficult to verify the value on the fly
      assertTrue("failed to get ReplicationLoadSink.AgeOfLastShippedOp ",
        (rLoadSink.getAgeOfLastAppliedOp() >= 0));
      assertTrue("failed to get ReplicationLoadSink.TimeStampsOfLastAppliedOp ",
        (rLoadSink.getTimestampsOfLastAppliedOp() >= 0));
    }

    // Stop rs1, then the queue of rs1 will be transfered to rs0
    UTIL1.getHBaseCluster().getRegionServer(1).stop("Stop RegionServer");
    Thread.sleep(10000);
    metrics = hbaseAdmin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
    ServerName server = UTIL1.getHBaseCluster().getRegionServer(0).getServerName();
    ServerMetrics sm = metrics.getLiveServerMetrics().get(server);
    List<ReplicationLoadSource> rLoadSourceList = sm.getReplicationLoadSourceList();

    // check SourceList still only has one entry
    assertEquals("failed to get ReplicationLoadSourceList", 2, rLoadSourceList.size());
    assertEquals(PEER_ID2, rLoadSourceList.get(0).getPeerID());
  }
}
