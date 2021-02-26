/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestGetReplicationLoad {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGetReplicationLoad.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestGetReplicationLoad.class);

  private static MiniHBaseCluster cluster;
  private static HMaster master;
  private static HBaseTestingUtility TEST_UTIL;

  public static class MyMaster extends HMaster {
    public MyMaster(Configuration conf) throws IOException, KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    protected void tryRegionServerReport(long reportStartTime, long reportEndTime) {
      // do nothing
    }
  }

  @BeforeClass
  public static void startCluster() throws Exception {
    LOG.info("Starting cluster");
    TEST_UTIL = new HBaseTestingUtility();
    // Set master class and use default values for other options.
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .masterClass(TestMasterMetrics.MyMaster.class).build();
    TEST_UTIL.startMiniCluster(option);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
  }

  @AfterClass
  public static void after() throws Exception {
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testGetReplicationMetrics() throws Exception {
    String peer1 = "test1", peer2 = "test2", queueId="1";
    long ageOfLastShippedOp = 2,
        replicationLag = 3,
        timeStampOfLastShippedOp = 4,
        timeStampOfNextToReplicate=5,
        editsRead=6,
        oPsShipped=7;
    int sizeOfLogQueue = 8;
    boolean recovered=false,
        running=false,
        editsSinceRestart=false;
    RegionServerStatusProtos.RegionServerReportRequest.Builder request =
        RegionServerStatusProtos.RegionServerReportRequest.newBuilder();
    ServerName serverName = cluster.getMaster(0).getServerName();
    request.setServer(ProtobufUtil.toServerName(serverName));
    ClusterStatusProtos.ReplicationLoadSource rload1 = ClusterStatusProtos.ReplicationLoadSource
        .newBuilder().setPeerID(peer1)
        .setAgeOfLastShippedOp(ageOfLastShippedOp)
        .setReplicationLag(replicationLag)
        .setTimeStampOfLastShippedOp(timeStampOfLastShippedOp)
        .setSizeOfLogQueue(sizeOfLogQueue)
        .setTimeStampOfNextToReplicate(timeStampOfNextToReplicate)
        .setQueueId(queueId)
        .setEditsRead(editsRead)
        .setOPsShipped(oPsShipped)
        .setRunning(running)
        .setRecovered(recovered)
        .setEditsSinceRestart(editsSinceRestart)
        .build();
    ClusterStatusProtos.ReplicationLoadSource rload2 =
        ClusterStatusProtos.ReplicationLoadSource
            .newBuilder()
            .setPeerID(peer2)
            .setAgeOfLastShippedOp(ageOfLastShippedOp + 1)
            .setReplicationLag(replicationLag + 1)
            .setTimeStampOfLastShippedOp(timeStampOfLastShippedOp + 1)
            .setSizeOfLogQueue(sizeOfLogQueue + 1)
            .setTimeStampOfNextToReplicate(timeStampOfNextToReplicate+1)
            .setQueueId(queueId)
            .setEditsRead(editsRead+1)
            .setOPsShipped(oPsShipped+1)
            .setRunning(running)
            .setRecovered(recovered)
            .setEditsSinceRestart(editsSinceRestart)
            .build();

    ClusterStatusProtos.ServerLoad sl = ClusterStatusProtos.ServerLoad.newBuilder()
        .addReplLoadSource(rload1).addReplLoadSource(rload2).build();
    request.setLoad(sl);
    master.getReplicationPeerManager().addPeer(peer1,
      ReplicationPeerConfig.newBuilder().setClusterKey("test").build(), true);
    master.getReplicationPeerManager().addPeer(peer2,
      ReplicationPeerConfig.newBuilder().setClusterKey("test").build(), true);
    master.getMasterRpcServices().regionServerReport(null, request.build());
    HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>> replicationLoad =
        master.getReplicationLoad(new ServerName[] { serverName });
    assertEquals("peer size ", 2, replicationLoad.size());
    assertEquals("load size ", 1, replicationLoad.get(peer1).size());
    assertEquals("log queue size of peer1", sizeOfLogQueue,
      replicationLoad.get(peer1).get(0).getSecond().getSizeOfLogQueue());
    assertEquals("replication lag of peer2", replicationLag + 1,
      replicationLoad.get(peer2).get(0).getSecond().getReplicationLag());
    master.stopMaster();
  }
}
