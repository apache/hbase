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

import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.util.EnumSet;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationStatusSink extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationStatusSink.class);

  @Test
  public void testReplicationStatusSink() throws Exception {
    try (Admin admin = UTIL2.getConnection().getAdmin()) {
      ServerName server = UTIL2.getHBaseCluster().getRegionServer(0).getServerName();
      ReplicationLoadSink loadSink = getLatestSinkMetric(admin, server);
      //First checks if status of timestamp of last applied op is same as RS start, since no edits
      //were replicated yet
      Assert.assertEquals(loadSink.getTimestampStarted(), loadSink.getTimestampsOfLastAppliedOp());
      //now insert some rows on source, so that it gets delivered to target
      TestReplicationStatus.insertRowsOnSource();
      long wait =
        Waiter.waitFor(UTIL2.getConfiguration(), 10000, (Waiter.Predicate<Exception>) () -> {
          ReplicationLoadSink loadSink1 = getLatestSinkMetric(admin, server);
          return loadSink1.getTimestampsOfLastAppliedOp() > loadSink1.getTimestampStarted();
        });
      Assert.assertNotEquals(-1, wait);
    }
  }

  private ReplicationLoadSink getLatestSinkMetric(Admin admin, ServerName server)
      throws IOException {
    ClusterMetrics metrics =
      admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.LIVE_SERVERS));
    ServerMetrics sm = metrics.getLiveServerMetrics().get(server);
    return sm.getReplicationLoadSink();
  }

}
