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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Demonstrate how Procedure handles single members, multiple members, and errors semantics
 */
@Category(SmallTests.class)
public class TestProcedure {

  ProcedureCoordinator coord;

  @Before
  public void setup() {
    coord = mock(ProcedureCoordinator.class);
    final ProcedureCoordinatorRpcs comms = mock(ProcedureCoordinatorRpcs.class);
    when(coord.getRpcs()).thenReturn(comms); // make it not null
  }

  class LatchedProcedure extends Procedure {
    CountDownLatch startedAcquireBarrier = new CountDownLatch(1);
    CountDownLatch startedDuringBarrier = new CountDownLatch(1);
    CountDownLatch completedProcedure = new CountDownLatch(1);

    public LatchedProcedure(ProcedureCoordinator coord, ForeignExceptionDispatcher monitor,
        long wakeFreq, long timeout, String opName, byte[] data,
        List<String> expectedMembers) {
      super(coord, monitor, wakeFreq, timeout, opName, data, expectedMembers);
    }

    @Override
    public void sendGlobalBarrierStart() {
      startedAcquireBarrier.countDown();
    }

    @Override
    public void sendGlobalBarrierReached() {
      startedDuringBarrier.countDown();
    }

    @Override
    public void sendGlobalBarrierComplete() {
      completedProcedure.countDown();
    }
  };

  /**
   * With a single member, verify ordered execution.  The Coordinator side is run in a separate
   * thread so we can only trigger from members and wait for particular state latches.
   */
  @Test(timeout = 60000)
  public void testSingleMember() throws Exception {
    // The member
    List<String> members =  new ArrayList<String>();
    members.add("member");
    LatchedProcedure proc = new LatchedProcedure(coord, new ForeignExceptionDispatcher(), 100,
        Integer.MAX_VALUE, "op", null, members);
    final LatchedProcedure procspy = spy(proc);
    // coordinator: start the barrier procedure
    new Thread() {
      public void run() {
        procspy.call();
      }
    }.start();

    // coordinator: wait for the barrier to be acquired, then send start barrier
    proc.startedAcquireBarrier.await();

    // we only know that {@link Procedure#sendStartBarrier()} was called, and others are blocked.
    verify(procspy).sendGlobalBarrierStart();
    verify(procspy, never()).sendGlobalBarrierReached();
    verify(procspy, never()).sendGlobalBarrierComplete();
    verify(procspy, never()).barrierAcquiredByMember(anyString());

    // member: trigger global barrier acquisition
    proc.barrierAcquiredByMember(members.get(0));

    // coordinator: wait for global barrier to be acquired.
    proc.acquiredBarrierLatch.await();
    verify(procspy).sendGlobalBarrierStart(); // old news

    // since two threads, we cannot guarantee that {@link Procedure#sendSatsifiedBarrier()} was
    // or was not called here.

    // member: trigger global barrier release
    proc.barrierReleasedByMember(members.get(0), new byte[0]);

    // coordinator: wait for procedure to be completed
    proc.completedProcedure.await();
    verify(procspy).sendGlobalBarrierReached();
    verify(procspy).sendGlobalBarrierComplete();
    verify(procspy, never()).receive(any(ForeignException.class));
  }

  @Test(timeout = 60000)
  public void testMultipleMember() throws Exception {
    // 2 members
    List<String> members =  new ArrayList<String>();
    members.add("member1");
    members.add("member2");

    LatchedProcedure proc = new LatchedProcedure(coord, new ForeignExceptionDispatcher(), 100,
        Integer.MAX_VALUE, "op", null, members);
    final LatchedProcedure procspy = spy(proc);
    // start the barrier procedure
    new Thread() {
      public void run() {
        procspy.call();
      }
    }.start();

    // coordinator: wait for the barrier to be acquired, then send start barrier
    procspy.startedAcquireBarrier.await();

    // we only know that {@link Procedure#sendStartBarrier()} was called, and others are blocked.
    verify(procspy).sendGlobalBarrierStart();
    verify(procspy, never()).sendGlobalBarrierReached();
    verify(procspy, never()).sendGlobalBarrierComplete();
    verify(procspy, never()).barrierAcquiredByMember(anyString()); // no externals

    // member0: [1/2] trigger global barrier acquisition.
    procspy.barrierAcquiredByMember(members.get(0));

    // coordinator not satisified.
    verify(procspy).sendGlobalBarrierStart();
    verify(procspy, never()).sendGlobalBarrierReached();
    verify(procspy, never()).sendGlobalBarrierComplete();

    // member 1: [2/2] trigger global barrier acquisition.
    procspy.barrierAcquiredByMember(members.get(1));

    // coordinator: wait for global barrier to be acquired.
    procspy.startedDuringBarrier.await();
    verify(procspy).sendGlobalBarrierStart(); // old news

    // member 1, 2: trigger global barrier release
    procspy.barrierReleasedByMember(members.get(0), new byte[0]);
    procspy.barrierReleasedByMember(members.get(1), new byte[0]);

    // coordinator wait for procedure to be completed
    procspy.completedProcedure.await();
    verify(procspy).sendGlobalBarrierReached();
    verify(procspy).sendGlobalBarrierComplete();
    verify(procspy, never()).receive(any(ForeignException.class));
  }

  @Test(timeout = 60000)
  public void testErrorPropagation() throws Exception {
    List<String> members =  new ArrayList<String>();
    members.add("member");
    Procedure proc = new Procedure(coord, new ForeignExceptionDispatcher(), 100,
        Integer.MAX_VALUE, "op", null, members);
    final Procedure procspy = spy(proc);

    ForeignException cause = new ForeignException("SRC", "External Exception");
    proc.receive(cause);

    // start the barrier procedure
    Thread t = new Thread() {
      public void run() {
        procspy.call();
      }
    };
    t.start();
    t.join();

    verify(procspy, never()).sendGlobalBarrierStart();
    verify(procspy, never()).sendGlobalBarrierReached();
    verify(procspy).sendGlobalBarrierComplete();
  }

  @Test(timeout = 60000)
  public void testBarrieredErrorPropagation() throws Exception {
    List<String> members =  new ArrayList<String>();
    members.add("member");
    LatchedProcedure proc = new LatchedProcedure(coord, new ForeignExceptionDispatcher(), 100,
        Integer.MAX_VALUE, "op", null, members);
    final LatchedProcedure procspy = spy(proc);

    // start the barrier procedure
    Thread t = new Thread() {
      public void run() {
        procspy.call();
      }
    };
    t.start();

    // now test that we can put an error in before the commit phase runs
    procspy.startedAcquireBarrier.await();
    ForeignException cause = new ForeignException("SRC", "External Exception");
    procspy.receive(cause);
    procspy.barrierAcquiredByMember(members.get(0));
    t.join();

    // verify state of all the object
    verify(procspy).sendGlobalBarrierStart();
    verify(procspy).sendGlobalBarrierComplete();
    verify(procspy, never()).sendGlobalBarrierReached();
  }
}
