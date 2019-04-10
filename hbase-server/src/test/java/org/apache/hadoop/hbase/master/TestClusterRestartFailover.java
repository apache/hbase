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
package org.apache.hadoop.hbase.master;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.ServerState;
import org.apache.hadoop.hbase.master.assignment.ServerStateNode;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestClusterRestartFailover extends AbstractTestRestartCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClusterRestartFailover.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestClusterRestartFailover.class);

  @Override
  protected boolean splitWALCoordinatedByZk() {
    return true;
  }

  private ServerStateNode getServerStateNode(ServerName serverName) {
    return UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
      .getServerNode(serverName);
  }

  @Test
  public void test() throws Exception {
    UTIL.startMiniCluster(3);
    UTIL.waitFor(60000, () -> UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    // wait for all SCPs finished
    UTIL.waitFor(60000, () -> UTIL.getHBaseCluster().getMaster().getProcedures().stream()
      .noneMatch(p -> p instanceof ServerCrashProcedure));
    TableName tableName = TABLES[0];
    ServerName testServer = UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    UTIL.waitFor(10000, () -> getServerStateNode(testServer) != null);
    ServerStateNode serverNode = getServerStateNode(testServer);
    Assert.assertNotNull(serverNode);
    Assert.assertTrue("serverNode should be ONLINE when cluster runs normally",
      serverNode.isInState(ServerState.ONLINE));
    UTIL.createMultiRegionTable(tableName, FAMILY);
    UTIL.waitTableEnabled(tableName);
    Table table = UTIL.getConnection().getTable(tableName);
    for (int i = 0; i < 100; i++) {
      UTIL.loadTable(table, FAMILY);
    }
    List<Integer> ports =
      UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServersList().stream()
        .map(serverName -> serverName.getPort()).collect(Collectors.toList());
    LOG.info("Shutting down cluster");
    UTIL.getHBaseCluster().killAll();
    UTIL.getHBaseCluster().waitUntilShutDown();
    LOG.info("Starting cluster the second time");
    UTIL.restartHBaseCluster(3, ports);
    UTIL.waitFor(10000, () -> UTIL.getHBaseCluster().getMaster().isInitialized());
    serverNode = UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
      .getServerNode(testServer);
    Assert.assertNotNull("serverNode should not be null when restart whole cluster", serverNode);
    Assert.assertFalse(serverNode.isInState(ServerState.ONLINE));
    LOG.info("start to find the procedure of SCP for the severName we choose");
    UTIL.waitFor(60000,
      () -> UTIL.getHBaseCluster().getMaster().getProcedures().stream()
        .anyMatch(procedure -> (procedure instanceof ServerCrashProcedure) &&
          ((ServerCrashProcedure) procedure).getServerName().equals(testServer)));
    Assert.assertFalse("serverNode should not be ONLINE during SCP processing",
      serverNode.isInState(ServerState.ONLINE));
    LOG.info("start to submit the SCP for the same serverName {} which should fail", testServer);
    Assert
      .assertFalse(UTIL.getHBaseCluster().getMaster().getServerManager().expireServer(testServer));
    Procedure<?> procedure = UTIL.getHBaseCluster().getMaster().getProcedures().stream()
      .filter(p -> (p instanceof ServerCrashProcedure) &&
        ((ServerCrashProcedure) p).getServerName().equals(testServer))
      .findAny().get();
    UTIL.waitFor(60000, () -> procedure.isFinished());
    LOG.info("even when the SCP is finished, the duplicate SCP should not be scheduled for {}",
      testServer);
    Assert
      .assertFalse(UTIL.getHBaseCluster().getMaster().getServerManager().expireServer(testServer));
    serverNode = UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
      .getServerNode(testServer);
    Assert.assertNull("serverNode should be deleted after SCP finished", serverNode);
  }
}
