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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationStatusAfterLagging extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationStatusAfterLagging.class);

  @Test
  public void testReplicationStatusAfterLagging() throws Exception {
    UTIL2.shutdownMiniHBaseCluster();
    restartSourceCluster(1);
    // add some values to cluster 1
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, Bytes.toBytes("col1"), Bytes.toBytes("val" + i));
      htable1.put(p);
    }
    UTIL2.startMiniHBaseCluster();
    Thread.sleep(10000);
    Admin hbaseAdmin = UTIL1.getAdmin();
    ServerName serverName = UTIL1.getHBaseCluster().getRegionServer(0).getServerName();
    ClusterMetrics metrics = hbaseAdmin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
    List<ReplicationLoadSource> loadSources =
      metrics.getLiveServerMetrics().get(serverName).getReplicationLoadSourceList();
    assertEquals(1, loadSources.size());
    ReplicationLoadSource loadSource = loadSources.get(0);
    assertTrue(loadSource.hasEditsSinceRestart());
    assertTrue(loadSource.getTimestampOfLastShippedOp() > 0);
    assertEquals(0, loadSource.getReplicationLag());
  }
}
