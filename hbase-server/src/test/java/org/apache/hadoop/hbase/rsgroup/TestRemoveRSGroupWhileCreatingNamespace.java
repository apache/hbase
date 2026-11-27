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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ModifyNamespaceProcedure;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CreateNamespaceState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyNamespaceState;

/**
 * Removing an RSGroup while a namespace that uses it is being created or modified must not leave
 * the namespace referencing a group that does not exist.
 */
@Tag(RSGroupTests.TAG)
@Tag(MediumTests.TAG)
public class TestRemoveRSGroupWhileCreatingNamespace extends TestRSGroupsBase {

  private static volatile CountDownLatch ARRIVED;

  private static volatile CountDownLatch RESUME;

  /** Waits after the namespace's group was checked and before the namespace is written. */
  private static void hold() throws InterruptedException {
    CountDownLatch arrived = ARRIVED;
    if (arrived != null) {
      arrived.countDown();
      RESUME.await();
    }
  }

  public static class HoldingCreateNamespaceProcedure extends CreateNamespaceProcedure {

    public HoldingCreateNamespaceProcedure() {
    }

    public HoldingCreateNamespaceProcedure(MasterProcedureEnv env, NamespaceDescriptor nsd) {
      super(env, nsd);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, CreateNamespaceState state)
      throws InterruptedException {
      if (state == CreateNamespaceState.CREATE_NAMESPACE_CREATE_DIRECTORY) {
        hold();
      }
      return super.executeFromState(env, state);
    }
  }

  public static class HoldingModifyNamespaceProcedure extends ModifyNamespaceProcedure {

    public HoldingModifyNamespaceProcedure() {
    }

    public HoldingModifyNamespaceProcedure(MasterProcedureEnv env, NamespaceDescriptor nsd) {
      super(env, nsd);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, ModifyNamespaceState state)
      throws InterruptedException {
      if (state == ModifyNamespaceState.MODIFY_NAMESPACE_UPDATE_NS_TABLE) {
        hold();
      }
      return super.executeFromState(env, state);
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    TestRSGroupsBase.setUpTestBeforeClass();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    TestRSGroupsBase.tearDownAfterClass();
  }

  @BeforeEach
  public void beforeMethod(TestInfo testInfo) throws Exception {
    setUpBeforeMethod(testInfo);
  }

  @AfterEach
  public void afterMethod() throws Exception {
    tearDownAfterMethod();
  }

  /**
   * Runs the given procedure up to the point where it has checked the group of the namespace, then
   * empties the group and tries to remove it, which must be refused. Then lets the procedure finish
   * and checks that the namespace references the group, which still exists.
   */
  private void removeGroupWhileHeld(String groupName, String nsName,
    Procedure<MasterProcedureEnv> proc) throws Exception {
    Address server = ADMIN.getRSGroup(groupName).getServers().iterator().next();
    ProcedureExecutor<MasterProcedureEnv> procExec = MASTER.getMasterProcedureExecutor();
    ARRIVED = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    long procId = procExec.submitProcedure(proc);
    try {
      assertTrue(ARRIVED.await(60, TimeUnit.SECONDS));
      ADMIN.moveServersToRSGroup(Collections.singleton(server), RSGroupInfo.DEFAULT_GROUP);
      ConstraintException e =
        assertThrows(ConstraintException.class, () -> ADMIN.removeRSGroup(groupName));
      assertTrue(e.getMessage().contains("being created or modified"), e.getMessage());
    } finally {
      ARRIVED = null;
      RESUME.countDown();
    }
    TEST_UTIL.waitFor(60_000, () -> procExec.isFinished(procId));
    assertTrue(procExec.getResult(procId).isSuccess());
    assertEquals(groupName, ADMIN.getNamespaceDescriptor(nsName)
      .getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP));
    assertNotNull(ADMIN.getRSGroup(groupName));
  }

  @Test
  public void testRemoveGroupWhileNamespaceIsBeingCreated() throws Exception {
    String groupName = getGroupName(name.getMethodName());
    addGroup(groupName, 1);
    String nsName = TABLE_PREFIX + "_ns_" + name.getMethodName();
    NamespaceDescriptor nsd = NamespaceDescriptor.create(nsName)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName).build();
    removeGroupWhileHeld(groupName, nsName, new HoldingCreateNamespaceProcedure(
      MASTER.getMasterProcedureExecutor().getEnvironment(), nsd));
  }

  @Test
  public void testRemoveGroupWhileNamespaceIsBeingMovedToIt() throws Exception {
    String groupName = getGroupName(name.getMethodName());
    addGroup(groupName, 1);
    String nsName = TABLE_PREFIX + "_ns_" + name.getMethodName();
    ADMIN.createNamespace(NamespaceDescriptor.create(nsName).build());
    NamespaceDescriptor nsd = NamespaceDescriptor.create(nsName)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName).build();
    removeGroupWhileHeld(groupName, nsName, new HoldingModifyNamespaceProcedure(
      MASTER.getMasterProcedureExecutor().getEnvironment(), nsd));
  }
}
