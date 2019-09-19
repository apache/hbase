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
package org.apache.hadoop.hbase.procedure;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Test zookeeper-based, procedure controllers
 */
@Category({MasterTests.class, MediumTests.class})
public class TestZKProcedureControllers {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKProcedureControllers.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestZKProcedureControllers.class);
  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String COHORT_NODE_NAME = "expected";
  private static final String CONTROLLER_NODE_NAME = "controller";
  private static final VerificationMode once = Mockito.times(1);

  private final byte[] memberData = new String("data from member").getBytes();

  @BeforeClass
  public static void setupTest() throws Exception {
    UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    UTIL.shutdownMiniZKCluster();
  }

  /**
   * Smaller test to just test the actuation on the cohort member
   * @throws Exception on failure
   */
  @Test
  public void testSimpleZKCohortMemberController() throws Exception {
    ZKWatcher watcher = UTIL.getZooKeeperWatcher();
    final String operationName = "instanceTest";

    final Subprocedure sub = Mockito.mock(Subprocedure.class);
    Mockito.when(sub.getName()).thenReturn(operationName);

    final byte[] data = new byte[] { 1, 2, 3 };
    final CountDownLatch prepared = new CountDownLatch(1);
    final CountDownLatch committed = new CountDownLatch(1);

    final ForeignExceptionDispatcher monitor = spy(new ForeignExceptionDispatcher());
    final ZKProcedureMemberRpcs controller = new ZKProcedureMemberRpcs(
        watcher, "testSimple");

    // mock out cohort member callbacks
    final ProcedureMember member = Mockito
        .mock(ProcedureMember.class);
    Mockito.doReturn(sub).when(member).createSubprocedure(operationName, data);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        controller.sendMemberAcquired(sub);
        prepared.countDown();
        return null;
      }
    }).when(member).submitSubprocedure(sub);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        controller.sendMemberCompleted(sub, memberData);
        committed.countDown();
        return null;
      }
    }).when(member).receivedReachedGlobalBarrier(operationName);

    // start running the listener
    controller.start(COHORT_NODE_NAME, member);

    // set a prepare node from a 'coordinator'
    String prepare = ZKProcedureUtil.getAcquireBarrierNode(controller.getZkController(), operationName);
    ZKUtil.createSetData(watcher, prepare, ProtobufUtil.prependPBMagic(data));
    // wait for the operation to be prepared
    prepared.await();

    // create the commit node so we update the operation to enter the commit phase
    String commit = ZKProcedureUtil.getReachedBarrierNode(controller.getZkController(), operationName);
    LOG.debug("Found prepared, posting commit node:" + commit);
    ZKUtil.createAndFailSilent(watcher, commit);
    LOG.debug("Commit node:" + commit + ", exists:" + ZKUtil.checkExists(watcher, commit));
    committed.await();

    verify(monitor, never()).receive(Mockito.any());
    // XXX: broken due to composition.
//    verify(member, never()).getManager().controllerConnectionFailure(Mockito.anyString(),
//      Mockito.any());
    // cleanup after the test
    ZKUtil.deleteNodeRecursively(watcher, controller.getZkController().getBaseZnode());
    assertEquals("Didn't delete prepare node", -1, ZKUtil.checkExists(watcher, prepare));
    assertEquals("Didn't delete commit node", -1, ZKUtil.checkExists(watcher, commit));
  }

  @Test
  public void testZKCoordinatorControllerWithNoCohort() throws Exception {
    final String operationName = "no cohort controller test";
    final byte[] data = new byte[] { 1, 2, 3 };

    runMockCommitWithOrchestratedControllers(startCoordinatorFirst, operationName, data);
    runMockCommitWithOrchestratedControllers(startCohortFirst, operationName, data);
  }

  @Test
  public void testZKCoordinatorControllerWithSingleMemberCohort() throws Exception {
    final String operationName = "single member controller test";
    final byte[] data = new byte[] { 1, 2, 3 };

    runMockCommitWithOrchestratedControllers(startCoordinatorFirst, operationName, data, "cohort");
    runMockCommitWithOrchestratedControllers(startCohortFirst, operationName, data, "cohort");
  }

  @Test
  public void testZKCoordinatorControllerMultipleCohort() throws Exception {
    final String operationName = "multi member controller test";
    final byte[] data = new byte[] { 1, 2, 3 };

    runMockCommitWithOrchestratedControllers(startCoordinatorFirst, operationName, data, "cohort",
      "cohort2", "cohort3");
    runMockCommitWithOrchestratedControllers(startCohortFirst, operationName, data, "cohort",
      "cohort2", "cohort3");
  }

  private void runMockCommitWithOrchestratedControllers(StartControllers controllers,
      String operationName, byte[] data, String... cohort) throws Exception {
    ZKWatcher watcher = UTIL.getZooKeeperWatcher();
    List<String> expected = Lists.newArrayList(cohort);

    final Subprocedure sub = Mockito.mock(Subprocedure.class);
    Mockito.when(sub.getName()).thenReturn(operationName);

    CountDownLatch prepared = new CountDownLatch(expected.size());
    CountDownLatch committed = new CountDownLatch(expected.size());
    ArrayList<byte[]> dataFromMembers = new ArrayList<>();

    // mock out coordinator so we can keep track of zk progress
    ProcedureCoordinator coordinator = setupMockCoordinator(operationName,
      prepared, committed, dataFromMembers);

    ProcedureMember member = Mockito.mock(ProcedureMember.class);

    Pair<ZKProcedureCoordinator, List<ZKProcedureMemberRpcs>> pair = controllers
        .start(watcher, operationName, coordinator, CONTROLLER_NODE_NAME, member, expected);
    ZKProcedureCoordinator controller = pair.getFirst();
    List<ZKProcedureMemberRpcs> cohortControllers = pair.getSecond();
    // start the operation
    Procedure p = Mockito.mock(Procedure.class);
    Mockito.when(p.getName()).thenReturn(operationName);

    controller.sendGlobalBarrierAcquire(p, data, expected);

    // post the prepare node for each expected node
    for (ZKProcedureMemberRpcs cc : cohortControllers) {
      cc.sendMemberAcquired(sub);
    }

    // wait for all the notifications to reach the coordinator
    prepared.await();
    // make sure we got the all the nodes and no more
    Mockito.verify(coordinator, times(expected.size())).memberAcquiredBarrier(Mockito.eq(operationName),
      Mockito.anyString());

    // kick off the commit phase
    controller.sendGlobalBarrierReached(p, expected);

    // post the committed node for each expected node
    for (ZKProcedureMemberRpcs cc : cohortControllers) {
      cc.sendMemberCompleted(sub, memberData);
    }

    // wait for all commit notifications to reach the coordinator
    committed.await();
    // make sure we got the all the nodes and no more
    Mockito.verify(coordinator, times(expected.size())).memberFinishedBarrier(Mockito.eq(operationName),
      Mockito.anyString(), Mockito.eq(memberData));

    assertEquals("Incorrect number of members returnd data", expected.size(),
      dataFromMembers.size());
    for (byte[] result : dataFromMembers) {
      assertArrayEquals("Incorrect data from member", memberData, result);
    }

    controller.resetMembers(p);

    // verify all behavior
    verifyZooKeeperClean(operationName, watcher, controller.getZkProcedureUtil());
    verifyCohort(member, cohortControllers.size(), operationName, data);
    verifyCoordinator(operationName, coordinator, expected);
  }

  // TODO Broken by composition.
//  @Test
//  public void testCoordinatorControllerHandlesEarlyPrepareNodes() throws Exception {
//    runEarlyPrepareNodes(startCoordinatorFirst, "testEarlyPreparenodes", new byte[] { 1, 2, 3 },
//      "cohort1", "cohort2");
//    runEarlyPrepareNodes(startCohortFirst, "testEarlyPreparenodes", new byte[] { 1, 2, 3 },
//      "cohort1", "cohort2");
//  }

  public void runEarlyPrepareNodes(StartControllers controllers, String operationName, byte[] data,
      String... cohort) throws Exception {
    ZKWatcher watcher = UTIL.getZooKeeperWatcher();
    List<String> expected = Lists.newArrayList(cohort);

    final Subprocedure sub = Mockito.mock(Subprocedure.class);
    Mockito.when(sub.getName()).thenReturn(operationName);

    final CountDownLatch prepared = new CountDownLatch(expected.size());
    final CountDownLatch committed = new CountDownLatch(expected.size());
    ArrayList<byte[]> dataFromMembers = new ArrayList<>();

    // mock out coordinator so we can keep track of zk progress
    ProcedureCoordinator coordinator = setupMockCoordinator(operationName,
      prepared, committed, dataFromMembers);

    ProcedureMember member = Mockito.mock(ProcedureMember.class);
    Procedure p = Mockito.mock(Procedure.class);
    Mockito.when(p.getName()).thenReturn(operationName);

    Pair<ZKProcedureCoordinator, List<ZKProcedureMemberRpcs>> pair = controllers
        .start(watcher, operationName, coordinator, CONTROLLER_NODE_NAME, member, expected);
    ZKProcedureCoordinator controller = pair.getFirst();
    List<ZKProcedureMemberRpcs> cohortControllers = pair.getSecond();

    // post 1/2 the prepare nodes early
    for (int i = 0; i < cohortControllers.size() / 2; i++) {
      cohortControllers.get(i).sendMemberAcquired(sub);
    }

    // start the operation
    controller.sendGlobalBarrierAcquire(p, data, expected);

    // post the prepare node for each expected node
    for (ZKProcedureMemberRpcs cc : cohortControllers) {
      cc.sendMemberAcquired(sub);
    }

    // wait for all the notifications to reach the coordinator
    prepared.await();
    // make sure we got the all the nodes and no more
    Mockito.verify(coordinator, times(expected.size())).memberAcquiredBarrier(Mockito.eq(operationName),
      Mockito.anyString());

    // kick off the commit phase
    controller.sendGlobalBarrierReached(p, expected);

    // post the committed node for each expected node
    for (ZKProcedureMemberRpcs cc : cohortControllers) {
      cc.sendMemberCompleted(sub, memberData);
    }

    // wait for all commit notifications to reach the coordiantor
    committed.await();
    // make sure we got the all the nodes and no more
    Mockito.verify(coordinator, times(expected.size())).memberFinishedBarrier(Mockito.eq(operationName),
      Mockito.anyString(), Mockito.eq(memberData));

    controller.resetMembers(p);

    // verify all behavior
    verifyZooKeeperClean(operationName, watcher, controller.getZkProcedureUtil());
    verifyCohort(member, cohortControllers.size(), operationName, data);
    verifyCoordinator(operationName, coordinator, expected);
  }

  /**
   * @param dataFromMembers
   * @return a mock {@link ProcedureCoordinator} that just counts down the
   *         prepared and committed latch for called to the respective method
   */
  private ProcedureCoordinator setupMockCoordinator(String operationName,
      final CountDownLatch prepared, final CountDownLatch committed,
      final ArrayList<byte[]> dataFromMembers) {
    ProcedureCoordinator coordinator = Mockito
        .mock(ProcedureCoordinator.class);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        prepared.countDown();
        return null;
      }
    }).when(coordinator).memberAcquiredBarrier(Mockito.eq(operationName), Mockito.anyString());
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        dataFromMembers.add(memberData);
        committed.countDown();
        return null;
      }
    }).when(coordinator).memberFinishedBarrier(Mockito.eq(operationName), Mockito.anyString(),
      Mockito.eq(memberData));
    return coordinator;
  }

  /**
   * Verify that the prepare, commit and abort nodes for the operation are removed from zookeeper
   */
  private void verifyZooKeeperClean(String operationName, ZKWatcher watcher,
      ZKProcedureUtil controller) throws Exception {
    String prepare = ZKProcedureUtil.getAcquireBarrierNode(controller, operationName);
    String commit = ZKProcedureUtil.getReachedBarrierNode(controller, operationName);
    String abort = ZKProcedureUtil.getAbortNode(controller, operationName);
    assertEquals("Didn't delete prepare node", -1, ZKUtil.checkExists(watcher, prepare));
    assertEquals("Didn't delete commit node", -1, ZKUtil.checkExists(watcher, commit));
    assertEquals("Didn't delete abort node", -1, ZKUtil.checkExists(watcher, abort));
  }

  /**
   * Verify the cohort controller got called once per expected node to start the operation
   */
  private void verifyCohort(ProcedureMember member, int cohortSize,
      String operationName, byte[] data) {
//    verify(member, Mockito.times(cohortSize)).submitSubprocedure(Mockito.eq(operationName),
//      (byte[]) Mockito.argThat(new ArrayEquals(data)));
    Mockito.verify(member,
      Mockito.atLeast(cohortSize)).submitSubprocedure(Mockito.any());

  }

  /**
   * Verify that the coordinator only got called once for each expected node
   */
  private void verifyCoordinator(String operationName,
      ProcedureCoordinator coordinator, List<String> expected) {
    // verify that we got all the expected nodes
    for (String node : expected) {
      verify(coordinator, once).memberAcquiredBarrier(operationName, node);
      verify(coordinator, once).memberFinishedBarrier(operationName, node, memberData);
    }
  }

  /**
   * Specify how the controllers that should be started (not spy/mockable) for the test.
   */
  private abstract class StartControllers {
    public abstract Pair<ZKProcedureCoordinator, List<ZKProcedureMemberRpcs>> start(
            ZKWatcher watcher, String operationName,
            ProcedureCoordinator coordinator, String controllerName,
            ProcedureMember member, List<String> cohortNames) throws Exception;
  }

  private final StartControllers startCoordinatorFirst = new StartControllers() {

    @Override
    public Pair<ZKProcedureCoordinator, List<ZKProcedureMemberRpcs>> start(
            ZKWatcher watcher, String operationName,
            ProcedureCoordinator coordinator, String controllerName,
            ProcedureMember member, List<String> expected) throws Exception {
      // start the controller
      ZKProcedureCoordinator controller = new ZKProcedureCoordinator(
          watcher, operationName, CONTROLLER_NODE_NAME);
      controller.start(coordinator);

      // make a cohort controller for each expected node

      List<ZKProcedureMemberRpcs> cohortControllers = new ArrayList<>();
      for (String nodeName : expected) {
        ZKProcedureMemberRpcs cc = new ZKProcedureMemberRpcs(watcher, operationName);
        cc.start(nodeName, member);
        cohortControllers.add(cc);
      }
      return new Pair<>(controller, cohortControllers);
    }
  };

  /**
   * Check for the possible race condition where a cohort member starts after the controller and
   * therefore could miss a new operation
   */
  private final StartControllers startCohortFirst = new StartControllers() {

    @Override
    public Pair<ZKProcedureCoordinator, List<ZKProcedureMemberRpcs>> start(
            ZKWatcher watcher, String operationName,
            ProcedureCoordinator coordinator, String controllerName,
            ProcedureMember member, List<String> expected) throws Exception {

      // make a cohort controller for each expected node
      List<ZKProcedureMemberRpcs> cohortControllers = new ArrayList<>();
      for (String nodeName : expected) {
        ZKProcedureMemberRpcs cc = new ZKProcedureMemberRpcs(watcher, operationName);
        cc.start(nodeName, member);
        cohortControllers.add(cc);
      }

      // start the controller
      ZKProcedureCoordinator controller = new ZKProcedureCoordinator(
          watcher, operationName, CONTROLLER_NODE_NAME);
      controller.start(coordinator);

      return new Pair<>(controller, cohortControllers);
    }
  };
}
