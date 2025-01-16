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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint.SinkPeer;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ ReplicationTests.class, SmallTests.class })
public class TestHBaseReplicationEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseReplicationEndpoint.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private HBaseReplicationEndpoint endpoint;

  @Before
  public void setUp() throws Exception {
    ReplicationPeer replicationPeer = mock(ReplicationPeer.class);
    ReplicationPeerConfig peerConfig = mock(ReplicationPeerConfig.class);
    when(replicationPeer.getPeerConfig()).thenReturn(peerConfig);
    when(peerConfig.getClusterKey()).thenReturn("hbase+zk://server1:2181/hbase");
    ReplicationEndpoint.Context context =
      new ReplicationEndpoint.Context(null, UTIL.getConfiguration(), UTIL.getConfiguration(), null,
        null, null, replicationPeer, null, null, null);
    endpoint = new DummyHBaseReplicationEndpoint();
    endpoint.init(context);
  }

  @Test
  public void testChooseSinks() {
    List<ServerName> serverNames = Lists.newArrayList();
    int totalServers = 20;
    for (int i = 0; i < totalServers; i++) {
      serverNames.add(mock(ServerName.class));
    }
    ((DummyHBaseReplicationEndpoint) endpoint).setRegionServers(serverNames);
    endpoint.chooseSinks();
    int expected = (int) (totalServers * HBaseReplicationEndpoint.DEFAULT_REPLICATION_SOURCE_RATIO);
    assertEquals(expected, endpoint.getNumSinks());
  }

  @Test
  public void testChooseSinksLessThanRatioAvailable() {
    List<ServerName> serverNames =
      Lists.newArrayList(mock(ServerName.class), mock(ServerName.class));
    ((DummyHBaseReplicationEndpoint) endpoint).setRegionServers(serverNames);
    endpoint.chooseSinks();
    assertEquals(1, endpoint.getNumSinks());
  }

  @Test
  public void testReportBadSink() {
    ServerName serverNameA = mock(ServerName.class);
    ServerName serverNameB = mock(ServerName.class);
    ((DummyHBaseReplicationEndpoint) endpoint)
      .setRegionServers(Lists.newArrayList(serverNameA, serverNameB));
    endpoint.chooseSinks();
    // Sanity check
    assertEquals(1, endpoint.getNumSinks());

    SinkPeer sinkPeer = new SinkPeer(serverNameA, mock(AsyncRegionServerAdmin.class));
    endpoint.reportBadSink(sinkPeer);
    // Just reporting a bad sink once shouldn't have an effect
    assertEquals(1, endpoint.getNumSinks());
  }

  /**
   * Once a SinkPeer has been reported as bad more than BAD_SINK_THRESHOLD times, it should not be
   * replicated to anymore.
   */
  @Test
  public void testReportBadSinkPastThreshold() {
    List<ServerName> serverNames = Lists.newArrayList();
    int totalServers = 30;
    for (int i = 0; i < totalServers; i++) {
      serverNames.add(mock(ServerName.class));
    }
    ((DummyHBaseReplicationEndpoint) endpoint).setRegionServers(serverNames);
    endpoint.chooseSinks();
    // Sanity check
    int expected = (int) (totalServers * HBaseReplicationEndpoint.DEFAULT_REPLICATION_SOURCE_RATIO);
    assertEquals(expected, endpoint.getNumSinks());

    ServerName badSinkServer0 = endpoint.getSinkServers().get(0);
    SinkPeer sinkPeer = new SinkPeer(badSinkServer0, mock(AsyncRegionServerAdmin.class));
    for (int i = 0; i <= HBaseReplicationEndpoint.DEFAULT_BAD_SINK_THRESHOLD; i++) {
      endpoint.reportBadSink(sinkPeer);
    }
    // Reporting a bad sink more than the threshold count should remove it
    // from the list of potential sinks
    assertEquals(expected - 1, endpoint.getNumSinks());

    // now try a sink that has some successes
    ServerName badSinkServer1 = endpoint.getSinkServers().get(0);
    sinkPeer = new SinkPeer(badSinkServer1, mock(AsyncRegionServerAdmin.class));
    for (int i = 0; i < HBaseReplicationEndpoint.DEFAULT_BAD_SINK_THRESHOLD; i++) {
      endpoint.reportBadSink(sinkPeer);
    }
    endpoint.reportSinkSuccess(sinkPeer); // one success
    endpoint.reportBadSink(sinkPeer);
    // did not remove the sink, since we had one successful try
    assertEquals(expected - 1, endpoint.getNumSinks());

    for (int i = 0; i < HBaseReplicationEndpoint.DEFAULT_BAD_SINK_THRESHOLD - 1; i++) {
      endpoint.reportBadSink(sinkPeer);
    }
    // still not remove, since the success reset the counter
    assertEquals(expected - 1, endpoint.getNumSinks());
    endpoint.reportBadSink(sinkPeer);
    // but we exhausted the tries
    assertEquals(expected - 2, endpoint.getNumSinks());
  }

  @Test
  public void testReportBadSinkDownToZeroSinks() {
    List<ServerName> serverNames = Lists.newArrayList();
    int totalServers = 4;
    for (int i = 0; i < totalServers; i++) {
      serverNames.add(mock(ServerName.class));
    }
    ((DummyHBaseReplicationEndpoint) endpoint).setRegionServers(serverNames);
    endpoint.chooseSinks();
    // Sanity check
    int expected = (int) (totalServers * HBaseReplicationEndpoint.DEFAULT_REPLICATION_SOURCE_RATIO);
    assertEquals(expected, endpoint.getNumSinks());

    ServerName serverNameA = endpoint.getSinkServers().get(0);
    ServerName serverNameB = endpoint.getSinkServers().get(1);

    serverNames.remove(serverNameA);
    serverNames.remove(serverNameB);

    SinkPeer sinkPeerA = new SinkPeer(serverNameA, mock(AsyncRegionServerAdmin.class));
    SinkPeer sinkPeerB = new SinkPeer(serverNameB, mock(AsyncRegionServerAdmin.class));

    for (int i = 0; i <= HBaseReplicationEndpoint.DEFAULT_BAD_SINK_THRESHOLD; i++) {
      endpoint.reportBadSink(sinkPeerA);
      endpoint.reportBadSink(sinkPeerB);
    }

    // We've gone down to 0 good sinks, so the replication sinks
    // should have been refreshed now, so out of 4 servers, 2 are not considered as they are
    // reported as bad.
    expected =
      (int) ((totalServers - 2) * HBaseReplicationEndpoint.DEFAULT_REPLICATION_SOURCE_RATIO);
    assertEquals(expected, endpoint.getNumSinks());
  }

  private static class DummyHBaseReplicationEndpoint extends HBaseReplicationEndpoint {

    List<ServerName> regionServers;

    public void setRegionServers(List<ServerName> regionServers) {
      this.regionServers = regionServers;
    }

    @Override
    protected Collection<ServerName> fetchPeerAddresses() {
      return regionServers;
    }

    @Override
    public ReplicationResult replicate(ReplicateContext replicateContext) {
      return ReplicationResult.FAILED;
    }

    @Override
    public AsyncClusterConnection createConnection(URI clusterURI, Configuration conf)
      throws IOException {
      return null;
    }
  }
}
