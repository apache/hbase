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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionListener;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.errorhandling.TimeoutExceptionInjector;

import com.google.common.collect.Lists;

/**
 * A globally-barriered distributed procedure.  This class encapsulates state and methods for
 * tracking and managing a distributed procedure, as well as aborting if any member encounters
 * a problem or if a cancellation is requested.
 * <p>
 * All procedures first attempt to reach a barrier point with the {@link #sendGlobalBarrierStart()}
 * method.  The procedure contacts all members and waits for all subprocedures to execute
 * {@link Subprocedure#acquireBarrier} to acquire its local piece of the global barrier and then
 * send acquisition info back to the coordinator.  If all acquisitions at subprocedures succeed,
 * the coordinator then will call {@link #sendGlobalBarrierReached()}.  This notifies members to
 * execute the {@link Subprocedure#insideBarrier()} method.  The procedure is blocked until all
 * {@link Subprocedure#insideBarrier} executions complete at the members.  When
 * {@link Subprocedure#insideBarrier} completes at each member, the member sends notification to
 * the coordinator.  Once all members complete, the coordinator calls
 * {@link #sendGlobalBarrierComplete()}.
 * <p>
 * If errors are encountered remotely, they are forwarded to the coordinator, and
 * {@link Subprocedure#cleanup(Exception)} is called.
 * <p>
 * Each Procedure and each Subprocedure enforces a time limit on the execution time. If the time
 * limit expires before the procedure completes the {@link TimeoutExceptionInjector} will trigger
 * an {@link ForeignException} to abort the procedure.  This is particularly useful for situations
 * when running a distributed {@link Subprocedure} so participants can avoid blocking for extreme
 * amounts of time if one of the participants fails or takes a really long time (e.g. GC pause).
 * <p>
 * Users should generally not directly create or subclass instances of this.  They are created
 * for them implicitly via {@link ProcedureCoordinator#startProcedure(ForeignExceptionDispatcher,
 * String, byte[], List)}}
 */
@InterfaceAudience.Private
public class Procedure implements Callable<Void>, ForeignExceptionListener {
  private static final Log LOG = LogFactory.getLog(Procedure.class);

  //
  // Arguments and naming
  //

  // Name of the procedure
  final private String procName;
  // Arguments for this procedure execution
  final private byte[] args;

  //
  // Execution State
  //
  /** latch for waiting until all members have acquire in barrier state */
  final CountDownLatch acquiredBarrierLatch;
  /** latch for waiting until all members have executed and released their in barrier state */
  final CountDownLatch releasedBarrierLatch;
  /** latch for waiting until a procedure has completed */
  final CountDownLatch completedLatch;
  /** monitor to check for errors */
  private final ForeignExceptionDispatcher monitor;

  //
  // Execution Timeout Handling.
  //

  /** frequency to check for errors (ms) */
  protected final long wakeFrequency;
  protected final TimeoutExceptionInjector timeoutInjector;

  //
  // Members' and Coordinator's state
  //

  /** lock to prevent nodes from acquiring and then releasing before we can track them */
  private Object joinBarrierLock = new Object();
  private final List<String> acquiringMembers;
  private final List<String> inBarrierMembers;
  private final HashMap<String, byte[]> dataFromFinishedMembers;
  private ProcedureCoordinator coord;

  /**
   * Creates a procedure. (FOR TESTING)
   *
   * {@link Procedure} state to be run by a {@link ProcedureCoordinator}.
   * @param coord coordinator to call back to for general errors (e.g.
   *          {@link ProcedureCoordinator#rpcConnectionFailure(String, IOException)}).
   * @param monitor error monitor to check for external errors
   * @param wakeFreq frequency to check for errors while waiting
   * @param timeout amount of time to allow the procedure to run before cancelling
   * @param procName name of the procedure instance
   * @param args argument data associated with the procedure instance
   * @param expectedMembers names of the expected members
   */
  public Procedure(ProcedureCoordinator coord, ForeignExceptionDispatcher monitor, long wakeFreq,
      long timeout, String procName, byte[] args, List<String> expectedMembers) {
    this.coord = coord;
    this.acquiringMembers = new ArrayList<String>(expectedMembers);
    this.inBarrierMembers = new ArrayList<String>(acquiringMembers.size());
    this.dataFromFinishedMembers = new HashMap<String, byte[]>();
    this.procName = procName;
    this.args = args;
    this.monitor = monitor;
    this.wakeFrequency = wakeFreq;

    int count = expectedMembers.size();
    this.acquiredBarrierLatch = new CountDownLatch(count);
    this.releasedBarrierLatch = new CountDownLatch(count);
    this.completedLatch = new CountDownLatch(1);
    this.timeoutInjector = new TimeoutExceptionInjector(monitor, timeout);
  }

  /**
   * Create a procedure.
   *
   * Users should generally not directly create instances of this.  They are created them
   * implicitly via {@link ProcedureCoordinator#createProcedure(ForeignExceptionDispatcher,
   * String, byte[], List)}}
   *
   * @param coord coordinator to call back to for general errors (e.g.
   *          {@link ProcedureCoordinator#rpcConnectionFailure(String, IOException)}).
   * @param wakeFreq frequency to check for errors while waiting
   * @param timeout amount of time to allow the procedure to run before cancelling
   * @param procName name of the procedure instance
   * @param args argument data associated with the procedure instance
   * @param expectedMembers names of the expected members
   */
  public Procedure(ProcedureCoordinator coord, long wakeFreq, long timeout,
      String procName, byte[] args, List<String> expectedMembers) {
    this(coord, new ForeignExceptionDispatcher(), wakeFreq, timeout, procName, args,
        expectedMembers);
  }

  public String getName() {
    return procName;
  }

  /**
   * @return String of the procedure members both trying to enter the barrier and already in barrier
   */
  public String getStatus() {
    String waiting, done;
    synchronized (joinBarrierLock) {
      waiting = acquiringMembers.toString();
      done = inBarrierMembers.toString();
    }
    return "Procedure " + procName + " { waiting=" + waiting + " done="+ done + " }";
  }

  /**
   * Get the ForeignExceptionDispatcher
   * @return the Procedure's monitor.
   */
  public ForeignExceptionDispatcher getErrorMonitor() {
    return monitor;
  }

  /**
   * This call is the main execution thread of the barriered procedure.  It sends messages and
   * essentially blocks until all procedure members acquire or later complete but periodically
   * checks for foreign exceptions.
   */
  @Override
  @SuppressWarnings("finally")
  final public Void call() {
    LOG.info("Starting procedure '" + procName + "'");
    // start the timer
    timeoutInjector.start();

    // run the procedure
    try {
      // start by checking for error first
      monitor.rethrowException();
      LOG.debug("Procedure '" + procName + "' starting 'acquire'");
      sendGlobalBarrierStart();

      // wait for all the members to report acquisition
      LOG.debug("Waiting for all members to 'acquire'");
      waitForLatch(acquiredBarrierLatch, monitor, wakeFrequency, "acquired");
      monitor.rethrowException();

      LOG.debug("Procedure '" + procName + "' starting 'in-barrier' execution.");
      sendGlobalBarrierReached();

      // wait for all members to report barrier release
      LOG.debug("Waiting for all members to 'release'");
      waitForLatch(releasedBarrierLatch, monitor, wakeFrequency, "released");

      // make sure we didn't get an error during in barrier execution and release
      monitor.rethrowException();
      LOG.info("Procedure '" + procName + "' execution completed");
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String msg = "Procedure '" + procName +"' execution failed!";
      LOG.error(msg, e);
      receive(new ForeignException(getName(), e));
    } finally {
      LOG.debug("Running finish phase.");
      sendGlobalBarrierComplete();
      completedLatch.countDown();

      // tell the timer we are done, if we get here successfully
      timeoutInjector.complete();
      return null;
    }
  }

  /**
   * Sends a message to Members to create a new {@link Subprocedure} for this Procedure and execute
   * the {@link Subprocedure#acquireBarrier} step.
   * @throws ForeignException
   */
  public void sendGlobalBarrierStart() throws ForeignException {
    // start the procedure
    LOG.debug("Starting procedure '" + procName + "', kicking off acquire phase on members.");
    try {
      // send procedure barrier start to specified list of members. cloning the list to avoid
      // concurrent modification from the controller setting the prepared nodes
      coord.getRpcs().sendGlobalBarrierAcquire(this, args, Lists.newArrayList(this.acquiringMembers));
    } catch (IOException e) {
      coord.rpcConnectionFailure("Can't reach controller.", e);
    } catch (IllegalArgumentException e) {
      throw new ForeignException(getName(), e);
    }
  }

  /**
   * Sends a message to all members that the global barrier condition has been satisfied.  This
   * should only be executed after all members have completed its
   * {@link Subprocedure#acquireBarrier()} call successfully.  This triggers the member
   * {@link Subprocedure#insideBarrier} method.
   * @throws ForeignException
   */
  public void sendGlobalBarrierReached() throws ForeignException {
    try {
      // trigger to have member run {@link Subprocedure#insideBarrier}
      coord.getRpcs().sendGlobalBarrierReached(this, Lists.newArrayList(inBarrierMembers));
    } catch (IOException e) {
      coord.rpcConnectionFailure("Can't reach controller.", e);
    }
  }

  /**
   * Sends a message to members that all {@link Subprocedure#insideBarrier} calls have completed.
   * After this executes, the coordinator can assume that any state resources about this barrier
   * procedure state has been released.
   */
  public void sendGlobalBarrierComplete() {
    LOG.debug("Finished coordinator procedure - removing self from list of running procedures");
    try {
      coord.getRpcs().resetMembers(this);
    } catch (IOException e) {
      coord.rpcConnectionFailure("Failed to reset procedure:" + procName, e);
    }
  }

  //
  // Call backs from other external processes.
  //

  /**
   * Call back triggered by an individual member upon successful local barrier acquisition
   * @param member
   */
  public void barrierAcquiredByMember(String member) {
    LOG.debug("member: '" + member + "' joining acquired barrier for procedure '" + procName
        + "' on coordinator");
    if (this.acquiringMembers.contains(member)) {
      synchronized (joinBarrierLock) {
        if (this.acquiringMembers.remove(member)) {
          this.inBarrierMembers.add(member);
          acquiredBarrierLatch.countDown();
        }
      }
      LOG.debug("Waiting on: " + acquiredBarrierLatch + " remaining members to acquire global barrier");
    } else {
      LOG.warn("Member " + member + " joined barrier, but we weren't waiting on it to join." +
          " Continuing on.");
    }
  }

  /**
   * Call back triggered by a individual member upon successful local in-barrier execution and
   * release
   * @param member
   * @param dataFromMember
   */
  public void barrierReleasedByMember(String member, byte[] dataFromMember) {
    boolean removed = false;
    synchronized (joinBarrierLock) {
      removed = this.inBarrierMembers.remove(member);
      if (removed) {
        releasedBarrierLatch.countDown();
      }
    }
    if (removed) {
      LOG.debug("Member: '" + member + "' released barrier for procedure'" + procName
          + "', counting down latch.  Waiting for " + releasedBarrierLatch.getCount()
          + " more");
    } else {
      LOG.warn("Member: '" + member + "' released barrier for procedure'" + procName
          + "', but we weren't waiting on it to release!");
    }
    dataFromFinishedMembers.put(member, dataFromMember);
  }

  /**
   * Waits until the entire procedure has globally completed, or has been aborted.  If an
   * exception is thrown the procedure may or not have run cleanup to trigger the completion latch
   * yet.
   * @throws ForeignException
   * @throws InterruptedException
   */
  public void waitForCompleted() throws ForeignException, InterruptedException {
    waitForLatch(completedLatch, monitor, wakeFrequency, procName + " completed");
  }

  /**
   * Waits until the entire procedure has globally completed, or has been aborted.  If an
   * exception is thrown the procedure may or not have run cleanup to trigger the completion latch
   * yet.
   * @return data returned from procedure members upon successfully completing subprocedure.
   * @throws ForeignException
   * @throws InterruptedException
   */
  public HashMap<String, byte[]> waitForCompletedWithRet() throws ForeignException, InterruptedException {
    waitForCompleted();
    return dataFromFinishedMembers;
  }

  /**
   * Check if the entire procedure has globally completed, or has been aborted.
   * @throws ForeignException
   */
  public boolean isCompleted() throws ForeignException {
    // Rethrow exception if any
    monitor.rethrowException();
    return (completedLatch.getCount() == 0);
  }

  /**
   * A callback that handles incoming ForeignExceptions.
   */
  @Override
  public void receive(ForeignException e) {
    monitor.receive(e);
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @param monitor monitor to check for errors while waiting
   * @param wakeFrequency frequency to wake up and check for errors (in
   *          {@link TimeUnit#MILLISECONDS})
   * @param latchDescription description of the latch, for logging
   * @throws ForeignException type of error the monitor can throw, if the task fails
   * @throws InterruptedException if we are interrupted while waiting on latch
   */
  public static void waitForLatch(CountDownLatch latch, ForeignExceptionSnare monitor,
      long wakeFrequency, String latchDescription) throws ForeignException,
      InterruptedException {
    boolean released = false;
    while (!released) {
      if (monitor != null) {
        monitor.rethrowException();
      }
      /*
      ForeignExceptionDispatcher.LOG.debug("Waiting for '" + latchDescription + "' latch. (sleep:"
          + wakeFrequency + " ms)"); */
      released = latch.await(wakeFrequency, TimeUnit.MILLISECONDS);
    }
    // check error again in case an error raised during last wait
    if (monitor != null) {
      monitor.rethrowException();
    }
  }
}
