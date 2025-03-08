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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Make sure we could start the cluster with RegionReplicaReplicationEndpoint configured.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestStartupWithLegacyRegionReplicationEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStartupWithLegacyRegionReplicationEndpoint.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    // add a peer to force initialize the replication storage
    UTIL.getAdmin().addReplicationPeer("1", ReplicationPeerConfig.newBuilder()
      .setClusterKey(UTIL.getZkCluster().getAddress().toString() + ":/1").build());
    UTIL.getAdmin().removeReplicationPeer("1");
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    String peerId = "legacy";
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey("127.0.0.1:2181:/hbase")
      .setReplicationEndpointImpl(ReplicationUtils.LEGACY_REGION_REPLICATION_ENDPOINT_NAME).build();
    SingleProcessHBaseCluster cluster = UTIL.getMiniHBaseCluster();
    HMaster master = cluster.getMaster();
    // can not use Admin.addPeer as it will fail with ClassNotFound
    master.getReplicationPeerManager().addPeer(peerId, peerConfig, true);
    // add a wal file to the queue
    ServerName rsName = cluster.getRegionServer(0).getServerName();
    master.getReplicationPeerManager().getQueueStorage().setOffset(
      new ReplicationQueueId(rsName, ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER), "",
      new ReplicationGroupOffset("test-wal-file", 0), Collections.emptyMap());
    cluster.stopRegionServer(0);
    RegionServerThread rst = cluster.startRegionServer();
    // we should still have this peer
    assertNotNull(UTIL.getAdmin().getReplicationPeerConfig(peerId));
    // but at RS side, we should not have this peer loaded as replication source
    assertTrue(
      rst.getRegionServer().getReplicationSourceService().getReplicationManager().getSources()
        .stream().map(ReplicationSourceInterface::getPeerId).noneMatch(p -> p.equals(peerId)));

    UTIL.shutdownMiniHBaseCluster();
    UTIL.restartHBaseCluster(1);
    // now we should have removed the peer
    assertThrows(ReplicationPeerNotFoundException.class,
      () -> UTIL.getAdmin().getReplicationPeerConfig("legacy"));

    // make sure that we can finish the SCP
    UTIL.waitFor(15000,
      () -> UTIL.getMiniHBaseCluster().getMaster().getProcedures().stream()
        .filter(p -> p instanceof ServerCrashProcedure).map(p -> (ServerCrashProcedure) p)
        .allMatch(Procedure::isSuccess));
    // we will delete the legacy peer while migrating, so here we do not assert the replication data
  }
}
