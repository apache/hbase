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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
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
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey("127.0.0.1:2181:/hbase")
      .setReplicationEndpointImpl(ReplicationUtils.LEGACY_REGION_REPLICATION_ENDPOINT_NAME).build();
    // can not use Admin.addPeer as it will fail with ClassNotFound
    UTIL.getMiniHBaseCluster().getMaster().getReplicationPeerManager().addPeer("legacy", peerConfig,
      true);
    UTIL.getMiniHBaseCluster().stopRegionServer(0);
    RegionServerThread rst = UTIL.getMiniHBaseCluster().startRegionServer();
    // we should still have this peer
    assertNotNull(UTIL.getAdmin().getReplicationPeerConfig("legacy"));
    // but at RS side, we should not have this peer loaded as replication source
    assertTrue(rst.getRegionServer().getReplicationSourceService().getReplicationManager()
      .getSources().isEmpty());
    UTIL.shutdownMiniHBaseCluster();
    UTIL.restartHBaseCluster(1);
    // now we should have removed the peer
    assertThrows(ReplicationPeerNotFoundException.class,
      () -> UTIL.getAdmin().getReplicationPeerConfig("legacy"));
    // at rs side, we should not have the peer this time, not only for not having replication source
    assertTrue(UTIL.getMiniHBaseCluster().getRegionServer(0).getReplicationSourceService()
      .getReplicationManager().getReplicationPeers().getAllPeerIds().isEmpty());
  }
}
