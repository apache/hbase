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
package org.apache.hadoop.hbase.master.replication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.replication.ZKReplicationStorageBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Category({ MasterTests.class, LargeTests.class })
public class TestMigrateReplicationQueue extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMigrateReplicationQueue.class);

  private int disableAndInsert() throws Exception {
    UTIL1.getAdmin().disableReplicationPeer(PEER_ID2);
    return UTIL1.loadTable(htable1, famName);
  }

  private String getQueuesZNode() throws IOException {
    Configuration conf = UTIL1.getConfiguration();
    ZKWatcher zk = UTIL1.getZooKeeperWatcher();
    String replicationZNode = ZNodePaths.joinZNode(zk.getZNodePaths().baseZNode,
      conf.get(ZKReplicationStorageBase.REPLICATION_ZNODE,
        ZKReplicationStorageBase.REPLICATION_ZNODE_DEFAULT));
    return ZNodePaths.joinZNode(replicationZNode, conf.get("zookeeper.znode.replication.rs", "rs"));
  }

  private void mockData() throws Exception {
    // fake a region_replica_replication peer and its queue data
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey("127.0.0.1:2181:/hbase")
      .setReplicationEndpointImpl(ReplicationUtils.LEGACY_REGION_REPLICATION_ENDPOINT_NAME).build();
    HMaster master = UTIL1.getMiniHBaseCluster().getMaster();
    master.getReplicationPeerManager()
      .addPeer(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER, peerConfig, true);
    ServerName rsName = UTIL1.getMiniHBaseCluster().getRegionServer(0).getServerName();
    master.getReplicationPeerManager().getQueueStorage().setOffset(
      new ReplicationQueueId(rsName, ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER), "",
      new ReplicationGroupOffset("test-wal-file", 0), Collections.emptyMap());

    // delete the replication queue table to simulate upgrading from an older version of hbase
    TableName replicationQueueTableName = TableName
      .valueOf(UTIL1.getConfiguration().get(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME,
        ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME_DEFAULT.getNameAsString()));
    List<ReplicationQueueData> queueDatas =
      master.getReplicationPeerManager().getQueueStorage().listAllQueues();
    // have an extra mocked queue data for region_replica_replication peer
    assertEquals(UTIL1.getMiniHBaseCluster().getRegionServerThreads().size() + 1,
      queueDatas.size());
    UTIL1.getAdmin().disableTable(replicationQueueTableName);
    UTIL1.getAdmin().deleteTable(replicationQueueTableName);
    // shutdown the hbase cluster
    UTIL1.shutdownMiniHBaseCluster();
    ZKWatcher zk = UTIL1.getZooKeeperWatcher();
    String queuesZNode = getQueuesZNode();
    for (ReplicationQueueData queueData : queueDatas) {
      String replicatorZNode =
        ZNodePaths.joinZNode(queuesZNode, queueData.getId().getServerName().toString());
      String queueZNode = ZNodePaths.joinZNode(replicatorZNode, queueData.getId().getPeerId());
      assertEquals(1, queueData.getOffsets().size());
      ReplicationGroupOffset offset = Iterables.getOnlyElement(queueData.getOffsets().values());
      String walZNode = ZNodePaths.joinZNode(queueZNode, offset.getWal());
      ZKUtil.createSetData(zk, walZNode, ZKUtil.positionToByteArray(offset.getOffset()));
    }
  }

  @Test
  public void testMigrate() throws Exception {
    int count = disableAndInsert();
    mockData();
    restartSourceCluster(1);
    UTIL1.waitFor(60000,
      () -> UTIL1.getMiniHBaseCluster().getMaster().getProcedures().stream()
        .filter(p -> p instanceof MigrateReplicationQueueFromZkToTableProcedure).findAny()
        .map(Procedure::isSuccess).orElse(false));
    TableName replicationQueueTableName = TableName
      .valueOf(UTIL1.getConfiguration().get(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME,
        ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME_DEFAULT.getNameAsString()));
    assertTrue(UTIL1.getAdmin().tableExists(replicationQueueTableName));
    ZKWatcher zk = UTIL1.getZooKeeperWatcher();
    assertEquals(-1, ZKUtil.checkExists(zk, getQueuesZNode()));
    // wait until MigrateReplicationQueueFromZkToTableProcedure finishes
    UTIL1.waitFor(15000, () -> UTIL1.getMiniHBaseCluster().getMaster().getProcedures().stream()
      .anyMatch(p -> p instanceof MigrateReplicationQueueFromZkToTableProcedure));
    UTIL1.waitFor(60000,
      () -> UTIL1.getMiniHBaseCluster().getMaster().getProcedures().stream()
        .filter(p -> p instanceof MigrateReplicationQueueFromZkToTableProcedure)
        .allMatch(Procedure::isSuccess));
    // make sure the region_replica_replication peer is gone, and there is no data on zk
    assertThat(UTIL1.getMiniHBaseCluster().getMaster().getReplicationPeerManager().getPeerStorage()
      .listPeerIds(), not(contains(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER)));
    assertEquals(-1, ZKUtil.checkExists(zk, getQueuesZNode()));

    // wait until SCP finishes, which means we can finish the claim queue operation
    UTIL1.waitFor(60000, () -> UTIL1.getMiniHBaseCluster().getMaster().getProcedures().stream()
      .filter(p -> p instanceof ServerCrashProcedure).allMatch(Procedure::isSuccess));

    List<ReplicationQueueData> queueDatas = UTIL1.getMiniHBaseCluster().getMaster()
      .getReplicationPeerManager().getQueueStorage().listAllQueues();
    assertEquals(1, queueDatas.size());
    // should have 1 recovered queue, as we haven't replicated anything out so there is no queue
    // data for the new alive region server
    assertTrue(queueDatas.get(0).getId().isRecovered());
    assertEquals(1, queueDatas.get(0).getOffsets().size());
    // the peer is still disabled, so no data has been replicated
    assertFalse(UTIL1.getAdmin().isReplicationPeerEnabled(PEER_ID2));
    assertEquals(0, HBaseTestingUtil.countRows(htable2));
    // enable peer, and make sure the replication can continue correctly
    UTIL1.getAdmin().enableReplicationPeer(PEER_ID2);
    waitForReplication(count, 100);
  }
}
