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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, SmallTests.class })
public class TestPeerProcedureHandlerImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPeerProcedureHandlerImpl.class);

  private ReplicationSourceManager mockSourceManager;
  private ReplicationPeers mockReplicationPeers;
  private ReplicationPeerImpl mockPeer;
  private PeerProcedureHandlerImpl handler;
  private static final String PEER_ID = "testPeer";

  @Before
  public void setup() throws Exception {
    mockSourceManager = mock(ReplicationSourceManager.class);
    mockReplicationPeers = mock(ReplicationPeers.class);
    mockPeer = mock(ReplicationPeerImpl.class);

    when(mockSourceManager.getReplicationPeers()).thenReturn(mockReplicationPeers);
    when(mockReplicationPeers.getPeer(PEER_ID)).thenReturn(mockPeer);

    handler = new PeerProcedureHandlerImpl(mockSourceManager);
  }

  @Test
  public void testReplicationSourceConfigChangeTriggers() throws Exception {
    ReplicationPeerConfig oldConfig = ReplicationPeerConfig.newBuilder().setClusterKey("oldCluster")
      .putConfiguration("replication.source.sleepforretries", "1000").build();

    ReplicationPeerConfig newConfig = ReplicationPeerConfig.newBuilder().setClusterKey("oldCluster")
      .putConfiguration("replication.source.sleepforretries", "5000").build();

    when(mockPeer.getPeerConfig()).thenReturn(oldConfig);
    when(mockPeer.getPeerState()).thenReturn(PeerState.ENABLED);
    when(mockReplicationPeers.refreshPeerConfig(PEER_ID)).thenReturn(newConfig);
    when(mockReplicationPeers.refreshPeerState(PEER_ID)).thenReturn(PeerState.ENABLED);

    handler.updatePeerConfig(PEER_ID);

    verify(mockSourceManager, times(1)).refreshSources(PEER_ID);
  }

  @Test
  public void testNonReplicationSourceConfigDoesNotTrigger() throws Exception {
    ReplicationPeerConfig oldConfig = ReplicationPeerConfig.newBuilder().setClusterKey("oldCluster")
      .putConfiguration("some.other.config", "value1").build();

    ReplicationPeerConfig newConfig = ReplicationPeerConfig.newBuilder().setClusterKey("oldCluster")
      .putConfiguration("some.other.config", "value2").build();

    when(mockPeer.getPeerConfig()).thenReturn(oldConfig);
    when(mockPeer.getPeerState()).thenReturn(PeerState.ENABLED);
    when(mockReplicationPeers.refreshPeerConfig(PEER_ID)).thenReturn(newConfig);
    when(mockReplicationPeers.refreshPeerState(PEER_ID)).thenReturn(PeerState.ENABLED);

    handler.updatePeerConfig(PEER_ID);

    verify(mockSourceManager, never()).refreshSources(anyString());
  }

  @Test
  public void testNewReplicationSourceConfigTriggers() throws Exception {
    ReplicationPeerConfig oldConfig =
      ReplicationPeerConfig.newBuilder().setClusterKey("oldCluster").build();

    ReplicationPeerConfig newConfig = ReplicationPeerConfig.newBuilder().setClusterKey("oldCluster")
      .putConfiguration("replication.source.sleepforretries", "5000").build();

    when(mockPeer.getPeerConfig()).thenReturn(oldConfig);
    when(mockPeer.getPeerState()).thenReturn(PeerState.ENABLED);
    when(mockReplicationPeers.refreshPeerConfig(PEER_ID)).thenReturn(newConfig);
    when(mockReplicationPeers.refreshPeerState(PEER_ID)).thenReturn(PeerState.ENABLED);

    handler.updatePeerConfig(PEER_ID);

    verify(mockSourceManager, times(1)).refreshSources(PEER_ID);
  }

  @Test
  public void testRemovedReplicationSourceConfigTriggers() throws Exception {
    ReplicationPeerConfig oldConfig = ReplicationPeerConfig.newBuilder().setClusterKey("oldCluster")
      .putConfiguration("replication.source.sleepforretries", "2000").build();

    ReplicationPeerConfig newConfig =
      ReplicationPeerConfig.newBuilder().setClusterKey("oldCluster").build();

    when(mockPeer.getPeerConfig()).thenReturn(oldConfig);
    when(mockPeer.getPeerState()).thenReturn(PeerState.ENABLED);
    when(mockReplicationPeers.refreshPeerConfig(PEER_ID)).thenReturn(newConfig);
    when(mockReplicationPeers.refreshPeerState(PEER_ID)).thenReturn(PeerState.ENABLED);

    handler.updatePeerConfig(PEER_ID);

    verify(mockSourceManager, times(1)).refreshSources(PEER_ID);
  }
}
