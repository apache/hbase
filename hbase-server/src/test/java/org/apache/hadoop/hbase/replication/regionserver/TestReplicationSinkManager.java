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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSinkManager.SinkPeer;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.mockito.Mockito;

@Category({ReplicationTests.class, SmallTests.class})
public class TestReplicationSinkManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationSinkManager.class);

  private static final String PEER_CLUSTER_ID = "PEER_CLUSTER_ID";

  private ReplicationSinkManager sinkManager;
  private HBaseReplicationEndpoint replicationEndpoint;

  /**
   * Manage the 'getRegionServers' for the tests below. Override the base class handling
   * of Regionservers. We used to use a mock for this but updated guava/errorprone disallows
   * mocking of classes that implement Service.
   */
  private static class SetServersHBaseReplicationEndpoint extends HBaseReplicationEndpoint {
    List<ServerName> regionServers;

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      return false;
    }

    @Override
    public synchronized void setRegionServers(List<ServerName> regionServers) {
      this.regionServers = regionServers;
    }

    @Override
    public List<ServerName> getRegionServers() {
      return this.regionServers;
    }
  }

  @Before
  public void setUp() {
    this.replicationEndpoint = new SetServersHBaseReplicationEndpoint();
    sinkManager = new ReplicationSinkManager(mock(ClusterConnection.class), PEER_CLUSTER_ID,
      replicationEndpoint, new Configuration());
  }

  @Test
  public void testChooseSinks() {
    List<ServerName> serverNames = Lists.newArrayList();
    int totalServers = 20;
    for (int i = 0; i < totalServers; i++) {
      serverNames.add(mock(ServerName.class));
    }
    replicationEndpoint.setRegionServers(serverNames);
    sinkManager.chooseSinks();
    int expected = (int) (totalServers * ReplicationSinkManager.DEFAULT_REPLICATION_SOURCE_RATIO);
    assertEquals(expected, sinkManager.getNumSinks());

  }

  @Test
  public void testChooseSinks_LessThanRatioAvailable() {
    List<ServerName> serverNames = Lists.newArrayList(mock(ServerName.class),
      mock(ServerName.class));
    replicationEndpoint.setRegionServers(serverNames);
    sinkManager.chooseSinks();
    assertEquals(1, sinkManager.getNumSinks());
  }

  @Test
  public void testReportBadSink() {
    ServerName serverNameA = mock(ServerName.class);
    ServerName serverNameB = mock(ServerName.class);
    replicationEndpoint.setRegionServers(Lists.newArrayList(serverNameA, serverNameB));
    sinkManager.chooseSinks();
    // Sanity check
    assertEquals(1, sinkManager.getNumSinks());

    SinkPeer sinkPeer = new SinkPeer(serverNameA, mock(AdminService.BlockingInterface.class));

    sinkManager.reportBadSink(sinkPeer);

    // Just reporting a bad sink once shouldn't have an effect
    assertEquals(1, sinkManager.getNumSinks());

  }

  /**
   * Once a SinkPeer has been reported as bad more than BAD_SINK_THRESHOLD times, it should not
   * be replicated to anymore.
   */
  @Test
  public void testReportBadSink_PastThreshold() {
    List<ServerName> serverNames = Lists.newArrayList();
    int totalServers = 30;
    for (int i = 0; i < totalServers; i++) {
      serverNames.add(mock(ServerName.class));
    }
    replicationEndpoint.setRegionServers(serverNames);
    sinkManager.chooseSinks();
    // Sanity check
    int expected = (int) (totalServers * ReplicationSinkManager.DEFAULT_REPLICATION_SOURCE_RATIO);
    assertEquals(expected, sinkManager.getNumSinks());

    ServerName serverName = sinkManager.getSinksForTesting().get(0);

    SinkPeer sinkPeer = new SinkPeer(serverName, mock(AdminService.BlockingInterface.class));

    sinkManager.reportSinkSuccess(sinkPeer); // has no effect, counter does not go negative
    for (int i = 0; i <= ReplicationSinkManager.DEFAULT_BAD_SINK_THRESHOLD; i++) {
      sinkManager.reportBadSink(sinkPeer);
    }

    // Reporting a bad sink more than the threshold count should remove it
    // from the list of potential sinks
    assertEquals(expected - 1, sinkManager.getNumSinks());

    //
    // now try a sink that has some successes
    //
    serverName = sinkManager.getSinksForTesting().get(0);

    sinkPeer = new SinkPeer(serverName, mock(AdminService.BlockingInterface.class));
    for (int i = 0; i <= ReplicationSinkManager.DEFAULT_BAD_SINK_THRESHOLD-1; i++) {
      sinkManager.reportBadSink(sinkPeer);
    }
    sinkManager.reportSinkSuccess(sinkPeer); // one success
    sinkManager.reportBadSink(sinkPeer);

    // did not remove the sink, since we had one successful try
    assertEquals(expected - 1, sinkManager.getNumSinks());

    for (int i = 0; i <= ReplicationSinkManager.DEFAULT_BAD_SINK_THRESHOLD-2; i++) {
      sinkManager.reportBadSink(sinkPeer);
    }
    // still not remove, since the success reset the counter
    assertEquals(expected - 1, sinkManager.getNumSinks());

    sinkManager.reportBadSink(sinkPeer);
    // but we exhausted the tries
    assertEquals(expected - 2, sinkManager.getNumSinks());
  }

  @Test
  public void testReportBadSink_DownToZeroSinks() {
    List<ServerName> serverNames = Lists.newArrayList();
    int totalServers = 4;
    for (int i = 0; i < totalServers; i++) {
      serverNames.add(mock(ServerName.class));
    }
    replicationEndpoint.setRegionServers(serverNames);
    sinkManager.chooseSinks();
    // Sanity check
    List<ServerName> sinkList = sinkManager.getSinksForTesting();
    int expected = (int) (totalServers * ReplicationSinkManager.DEFAULT_REPLICATION_SOURCE_RATIO);
    assertEquals(expected, sinkList.size());

    ServerName serverNameA = sinkList.get(0);
    ServerName serverNameB = sinkList.get(1);

    SinkPeer sinkPeerA = new SinkPeer(serverNameA, mock(AdminService.BlockingInterface.class));
    SinkPeer sinkPeerB = new SinkPeer(serverNameB, mock(AdminService.BlockingInterface.class));

    for (int i = 0; i <= ReplicationSinkManager.DEFAULT_BAD_SINK_THRESHOLD; i++) {
      sinkManager.reportBadSink(sinkPeerA);
      sinkManager.reportBadSink(sinkPeerB);
    }

    // We've gone down to 0 good sinks, so the replication sinks
    // should have been refreshed now, so out of 4 servers, 2 are not considered as they are
    // reported as bad.
    expected = (int) ((totalServers - 2) * ReplicationSinkManager.DEFAULT_REPLICATION_SOURCE_RATIO);
    assertEquals(expected, sinkManager.getNumSinks());
  }

}
