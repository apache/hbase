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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionListener;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.errorhandling.TimeoutExceptionInjector;
import org.apache.zookeeper.KeeperException;

/**
 * Distributed procedure member's Subprocedure.  A procedure is sarted on a ProcedureCoordinator
 * which communicates with ProcedureMembers who create and start its part of the Procedure.  This
 * sub part is called a Subprocedure
 *
 * Users should subclass this and implement {@link #acquireBarrier()} (get local barrier for this
 * member), {@link #insideBarrier()} (execute while globally barriered and release barrier) and
 * {@link #cleanup(Exception)} (release state associated with subprocedure.)
 *
 * When submitted to a ProcedureMemeber, the call method is executed in a separate thread.
 * Latches are use too block its progress and trigger continuations when barrier conditions are
 * met.
 *
 * Exception that makes it out of calls to {@link #acquireBarrier()} or {@link #insideBarrier()}
 * gets converted into {@link ForeignException}, which will get propagated to the
 * {@link ProcedureCoordinator}.
 *
 * There is a category of procedure (ex: online-snapshots), and a user-specified instance-specific
 * barrierName. (ex: snapshot121126).
 */
abstract public class Subprocedure implements Callable<Void> {
  private static final Log LOG = LogFactory.getLog(Subprocedure.class);

  // Name of the procedure
  final private String barrierName;

  //
  // Execution state
  //

  /** wait on before allowing the in barrier phase to proceed */
  private final CountDownLatch inGlobalBarrier;
  /** counted down when the Subprocedure has completed */
  private final CountDownLatch releasedLocalBarrier;

  //
  // Error handling
  //
  /** monitor to check for errors */
  protected final ForeignExceptionDispatcher monitor;
  /** frequency to check for errors (ms) */
  protected final long wakeFrequency;
  protected final TimeoutExceptionInjector executionTimeoutTimer;
  protected final ProcedureMemberRpcs rpcs;

  private volatile boolean complete = false;

  /**
   * @param member reference to the member managing this subprocedure
   * @param procName name of the procedure this subprocedure is associated with
   * @param monitor notified if there is an error in the subprocedure
   * @param wakeFrequency time in millis to wake to check if there is an error via the monitor (in
   *          milliseconds).
   * @param timeout time in millis that will trigger a subprocedure abort if it has not completed
   */
  public Subprocedure(ProcedureMember member, String procName, ForeignExceptionDispatcher monitor,
      long wakeFrequency, long timeout) {
    // Asserts should be caught during unit testing
    assert member != null : "procedure member should be non-null";
    assert member.getRpcs() != null : "rpc handlers should be non-null";
    assert procName != null : "procedure name should be non-null";
    assert monitor != null : "monitor should be non-null";

    // Default to a very large timeout
    this.rpcs = member.getRpcs();
    this.barrierName = procName;
    this.monitor = monitor;
    // forward any failures to coordinator.  Since this is a dispatcher, resend loops should not be
    // possible.
    this.monitor.addListener(new ForeignExceptionListener() {
      @Override
      public void receive(ForeignException ee) {
        // if this is a notification from a remote source, just log
        if (ee.isRemote()) {
          LOG.debug("Was remote foreign exception, not redispatching error", ee);
          return;
        }
        // if this is a local KeeperException, don't attempt to notify other members
        if (ee.getCause() instanceof KeeperException) {
          LOG.debug("Was KeeperException, not redispatching error", ee);
          return;
        }
        // if it is other local error, then send it to the coordinator
        try {
          rpcs.sendMemberAborted(Subprocedure.this, ee);
        } catch (IOException e) {
          // this will fail all the running procedures, since the connection is down
          LOG.error("Can't reach controller, not propagating error", e);
        }
      }
    });

    this.wakeFrequency = wakeFrequency;
    this.inGlobalBarrier = new CountDownLatch(1);
    this.releasedLocalBarrier = new CountDownLatch(1);

    // accept error from timer thread, this needs to be started.
    this.executionTimeoutTimer = new TimeoutExceptionInjector(monitor, timeout);
  }

  public String getName() {
     return barrierName;
  }

  public String getMemberName() {
    return rpcs.getMemberName();
  }

  private void rethrowException() throws ForeignException {
    monitor.rethrowException();
  }

  /**
   * Execute the Subprocedure {@link #acquireBarrier()} and {@link #insideBarrier()} methods
   * while keeping some state for other threads to access.
   *
   * This would normally be executed by the ProcedureMemeber when a acquire message comes from the
   * coordinator.  Rpcs are used to spend message back to the coordinator after different phases
   * are executed.  Any exceptions caught during the execution (except for InterrupedException) get
   * converted and propagated to coordinator via {@link ProcedureMemberRpcs#sendMemberAborted(
   * Subprocedure, ForeignException)}.
   */
  @SuppressWarnings("finally")
  final public Void call() {
    LOG.debug("Starting subprocedure '" + barrierName + "' with timeout " +
        executionTimeoutTimer.getMaxTime() + "ms");
    // start the execution timeout timer
    executionTimeoutTimer.start();

    try {
      // start by checking for error first
      rethrowException();
      LOG.debug("Subprocedure '" + barrierName + "' starting 'acquire' stage");
      acquireBarrier();
      LOG.debug("Subprocedure '" + barrierName + "' locally acquired");

      // vote yes to coordinator about being prepared
      rpcs.sendMemberAcquired(this);
      LOG.debug("Subprocedure '" + barrierName + "' coordinator notified of 'acquire', waiting on" +
          " 'reached' or 'abort' from coordinator");

      // wait for the procedure to reach global barrier before proceding
      waitForReachedGlobalBarrier();
      rethrowException(); // if Coordinator aborts, will bail from here with exception

      // In traditional 2PC, if a member reaches this state the TX has been committed and the
      // member is responsible for rolling forward and recovering and completing the subsequent
      // operations in the case of failure.  It cannot rollback.
      //
      // This implementation is not 2PC since it can still rollback here, and thus has different
      // semantics.

      LOG.debug("Subprocedure '" + barrierName + "' received 'reached' from coordinator.");
      insideBarrier();
      LOG.debug("Subprocedure '" + barrierName + "' locally completed");

      // Ack that the member has executed and released local barrier
      rpcs.sendMemberCompleted(this);
      LOG.debug("Subprocedure '" + barrierName + "' has notified controller of completion");

      // make sure we didn't get an external exception
      rethrowException();
    } catch (Exception e) {
      String msg = null;
      if (e instanceof InterruptedException) {
        msg = "Procedure '" + barrierName + "' aborting due to interrupt!" +
            " Likely due to pool shutdown.";
        Thread.currentThread().interrupt();
      } else if (e instanceof ForeignException) {
        msg = "Subprocedure '" + barrierName + "' aborting due to a ForeignException!";
      } else {
        msg = "Subprocedure '" + barrierName + "' failed!";
      }
      cancel(msg, e);

      LOG.debug("Subprocedure '" + barrierName + "' running cleanup.");
      cleanup(e);
    } finally {
      releasedLocalBarrier.countDown();

      // tell the timer we are done, if we get here successfully
      executionTimeoutTimer.complete();
      complete = true;
      LOG.debug("Subprocedure '" + barrierName + "' completed.");
      return null;
    }
  }

  boolean isComplete() {
    return complete;
  }

  /**
   * exposed for testing.
   */
  ForeignExceptionSnare getErrorCheckable() {
    return this.monitor;
  }

  /**
   * The implementation of this method should gather and hold required resources (locks, disk
   * space, etc) to satisfy the Procedures barrier condition.  For example, this would be where
   * to make all the regions on a RS on the quiescent for an procedure that required all regions
   * to be globally quiesed.
   *
   * Users should override this method.  If a quiescent is not required, this is overkill but
   * can still be used to execute a procedure on all members and to propagate any exceptions.
   *
   * @throws ForeignException
   */
  abstract public void acquireBarrier() throws ForeignException;

  /**
   * The implementation of this method should act with the assumption that the barrier condition
   * has been satisfied.  Continuing the previous example, a condition could be that all RS's
   * globally have been quiesced, and procedures that require this precondition could be
   * implemented here.
   *
   * Users should override this method.  If quiescense is not required, this can be a no-op
   *
   * @throws ForeignException
   */
  abstract public void insideBarrier() throws ForeignException;

  /**
   * Users should override this method. This implementation of this method should rollback and
   * cleanup any temporary or partially completed state that the {@link #acquireBarrier()} may have
   * created.
   * @param e
   */
  abstract public void cleanup(Exception e);

  /**
   * Method to cancel the Subprocedure by injecting an exception from and external source.
   * @param cause
   */
  public void cancel(String msg, Throwable cause) {
    LOG.error(msg, cause);
    complete = true;
    if (cause instanceof ForeignException) {
      monitor.receive((ForeignException) cause);
    } else {
      monitor.receive(new ForeignException(getMemberName(), cause));
    }
  }

  /**
   * Callback for the member rpcs to call when the global barrier has been reached.  This
   * unblocks the main subprocedure exectuion thread so that the Subprocedure's
   * {@link #insideBarrier()} method can be run.
   */
  public void receiveReachedGlobalBarrier() {
    inGlobalBarrier.countDown();
  }

  //
  // Subprocedure Internal State interface
  //

  /**
   * Wait for the reached global barrier notification.
   *
   * Package visibility for testing
   *
   * @throws ForeignException
   * @throws InterruptedException
   */
  void waitForReachedGlobalBarrier() throws ForeignException, InterruptedException {
    Procedure.waitForLatch(inGlobalBarrier, monitor, wakeFrequency,
        barrierName + ":remote acquired");
  }

  /**
   * Waits until the entire procedure has globally completed, or has been aborted.
   * @throws ForeignException
   * @throws InterruptedException
   */
  public void waitForLocallyCompleted() throws ForeignException, InterruptedException {
    Procedure.waitForLatch(releasedLocalBarrier, monitor, wakeFrequency,
        barrierName + ":completed");
  }

  /**
   * Empty Subprocedure for testing.
   *
   * Must be public for stubbing used in testing to work.
   */
  public static class SubprocedureImpl extends Subprocedure {

    public SubprocedureImpl(ProcedureMember member, String opName,
        ForeignExceptionDispatcher monitor, long wakeFrequency, long timeout) {
      super(member, opName, monitor, wakeFrequency, timeout);
    }

    @Override
    public void acquireBarrier() throws ForeignException {}

    @Override
    public void insideBarrier() throws ForeignException {}

    @Override
    public void cleanup(Exception e) {}
  };
}
