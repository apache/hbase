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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.CloseExcessRegionReplicasProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A test to make sure that we will wait for RIT to finish while closing excess region replicas. See
 * HBASE-28582 and related issues for more details.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestReduceExcessRegionReplicasBlockedByRIT {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReduceExcessRegionReplicasBlockedByRIT.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableDescriptor TD =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("CloseExcessRegionReplicas"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).setRegionReplication(4).build();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    UTIL.getAdmin().createTable(TD);
    UTIL.waitTableAvailable(TD.getTableName());
    UTIL.waitUntilNoRegionsInTransition();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRIT() throws Exception {
    RegionStateNode rsn = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
      .getRegionStates().getTableRegionStateNodes(TD.getTableName()).stream()
      .filter(rn -> rn.getRegionInfo().getReplicaId() > 1).findAny().get();
    // fake a TRSP to block the CloseExcessRegionReplicasProcedure
    TransitRegionStateProcedure trsp = new TransitRegionStateProcedure();
    rsn.setProcedure(trsp);
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(TD).setRegionReplication(2).build();
    CompletableFuture<Void> future = UTIL.getAsyncConnection().getAdmin().modifyTable(newTd);
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    UTIL.waitFor(5000, () -> procExec.getProcedures().stream()
      .anyMatch(p -> p instanceof CloseExcessRegionReplicasProcedure && !p.isFinished()));
    CloseExcessRegionReplicasProcedure proc =
      procExec.getProcedures().stream().filter(p -> p instanceof CloseExcessRegionReplicasProcedure)
        .map(p -> (CloseExcessRegionReplicasProcedure) p).findFirst().get();
    // make sure that the procedure can not finish
    for (int i = 0; i < 5; i++) {
      Thread.sleep(3000);
      assertFalse(proc.isFinished());
    }
    assertTrue(rsn.isInState(RegionState.State.OPEN));
    // unset the procedure, so we could make progress on CloseExcessRegionReplicasProcedure
    rsn.unsetProcedure(trsp);
    UTIL.waitFor(60000, () -> proc.isFinished());

    future.get();

    // the region should be in CLOSED state, and should have been removed from AM
    assertTrue(rsn.isInState(RegionState.State.CLOSED));
    // only 2 replicas now
    assertEquals(2, UTIL.getMiniHBaseCluster().getRegions(TD.getTableName()).size());
  }
}
