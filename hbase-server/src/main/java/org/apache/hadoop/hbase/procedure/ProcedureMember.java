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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.errorhandling.ForeignException;

import com.google.common.collect.MapMaker;

/**
 * Process to kick off and manage a running {@link Subprocedure} on a member. This is the
 * specialized part of a {@link Procedure} that actually does procedure type-specific work
 * and reports back to the coordinator as it completes each phase.
 */
@InterfaceAudience.Private
public class ProcedureMember implements Closeable {
  private static final Log LOG = LogFactory.getLog(ProcedureMember.class);

  final static long KEEP_ALIVE_MILLIS_DEFAULT = 5000;

  private final SubprocedureFactory builder;
  private final ProcedureMemberRpcs rpcs;

  private final ConcurrentMap<String,Subprocedure> subprocs =
      new MapMaker().concurrencyLevel(4).weakValues().makeMap();
  private final ExecutorService pool;

  /**
   * Instantiate a new ProcedureMember.  This is a slave that executes subprocedures.
   *
   * @param rpcs controller used to send notifications to the procedure coordinator
   * @param pool thread pool to submit subprocedures
   * @param factory class that creates instances of a subprocedure.
   */
  public ProcedureMember(ProcedureMemberRpcs rpcs, ThreadPoolExecutor pool,
      SubprocedureFactory factory) {
    this.pool = pool;
    this.rpcs = rpcs;
    this.builder = factory;
  }

  /**
   * Default thread pool for the procedure
   *
   * @param memberName
   * @param procThreads the maximum number of threads to allow in the pool
   */
  public static ThreadPoolExecutor defaultPool(String memberName, int procThreads) {
    return defaultPool(memberName, procThreads, KEEP_ALIVE_MILLIS_DEFAULT);
  }

  /**
   * Default thread pool for the procedure
   *
   * @param memberName
   * @param procThreads the maximum number of threads to allow in the pool
   * @param keepAliveMillis the maximum time (ms) that excess idle threads will wait for new tasks
   */
  public static ThreadPoolExecutor defaultPool(String memberName, int procThreads,
      long keepAliveMillis) {
    return new ThreadPoolExecutor(1, procThreads, keepAliveMillis, TimeUnit.MILLISECONDS,
        new SynchronousQueue<Runnable>(),
        new DaemonThreadFactory("member: '" + memberName + "' subprocedure-pool"));
  }

  /**
   * Package exposed.  Not for public use.
   *
   * @return reference to the Procedure member's rpcs object
   */
  ProcedureMemberRpcs getRpcs() {
    return rpcs;
  }


  /**
   * This is separated from execution so that we can detect and handle the case where the
   * subprocedure is invalid and inactionable due to bad info (like DISABLED snapshot type being
   * sent here)
   * @param opName
   * @param data
   * @return subprocedure
   */
  public Subprocedure createSubprocedure(String opName, byte[] data) {
    return builder.buildSubprocedure(opName, data);
  }

  /**
   * Submit an subprocedure for execution.  This starts the local acquire phase.
   * @param subproc the subprocedure to execute.
   * @return <tt>true</tt> if the subprocedure was started correctly, <tt>false</tt> if it
   *         could not be started. In the latter case, the subprocedure holds a reference to
   *         the exception that caused the failure.
   */
  public boolean submitSubprocedure(Subprocedure subproc) {
     // if the submitted subprocedure was null, bail.
    if (subproc == null) {
      LOG.warn("Submitted null subprocedure, nothing to run here.");
      return false;
    }

    String procName = subproc.getName();
    if (procName == null || procName.length() == 0) {
      LOG.error("Subproc name cannot be null or the empty string");
      return false;
    }

    // make sure we aren't already running an subprocedure of that name
    Subprocedure rsub = subprocs.get(procName);
    if (rsub != null) {
      if (!rsub.isComplete()) {
        LOG.error("Subproc '" + procName + "' is already running. Bailing out");
        return false;
      }
      LOG.warn("A completed old subproc "  +  procName + " is still present, removing");
      if (!subprocs.remove(procName, rsub)) {
        LOG.error("Another thread has replaced existing subproc '" + procName + "'. Bailing out");
        return false;
      }
    }

    LOG.debug("Submitting new Subprocedure:" + procName);

    // kick off the subprocedure
    try {
      if (subprocs.putIfAbsent(procName, subproc) == null) {
        this.pool.submit(subproc);
        return true;
      } else {
        LOG.error("Another thread has submitted subproc '" + procName + "'. Bailing out");
        return false;
      }
    } catch (RejectedExecutionException e) {
      subprocs.remove(procName, subproc);

      // the thread pool is full and we can't run the subprocedure
      String msg = "Subprocedure pool is full!";
      subproc.cancel(msg, e.getCause());
    }

    LOG.error("Failed to start subprocedure '" + procName + "'");
    return false;
  }

   /**
    * Notification that procedure coordinator has reached the global barrier
    * @param procName name of the subprocedure that should start running the in-barrier phase
    */
   public void receivedReachedGlobalBarrier(String procName) {
     Subprocedure subproc = subprocs.get(procName);
     if (subproc == null) {
       LOG.warn("Unexpected reached globa barrier message for Sub-Procedure '" + procName + "'");
       return;
     }
     if (LOG.isTraceEnabled()) {
      LOG.trace("reached global barrier message for Sub-Procedure '" + procName + "'");
     }
     subproc.receiveReachedGlobalBarrier();
   }

  /**
   * Best effort attempt to close the threadpool via Thread.interrupt.
   */
  @Override
  public void close() throws IOException {
    // have to use shutdown now to break any latch waiting
    pool.shutdownNow();
  }

  /**
   * Shutdown the threadpool, and wait for upto timeoutMs millis before bailing
   * @param timeoutMs timeout limit in millis
   * @return true if successfully, false if bailed due to timeout.
   * @throws InterruptedException
   */
  boolean closeAndWait(long timeoutMs) throws InterruptedException {
    pool.shutdown();
    return pool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * The connection to the rest of the procedure group (member and coordinator) has been
   * broken/lost/failed. This should fail any interested subprocedure, but not attempt to notify
   * other members since we cannot reach them anymore.
   * @param message description of the error
   * @param cause the actual cause of the failure
   * @param procName the name of the procedure we'd cancel due to the error.
   */
  public void controllerConnectionFailure(final String message, final Throwable cause,
      final String procName) {
    LOG.error(message, cause);
    if (procName == null) {
      return;
    }
    Subprocedure toNotify = subprocs.get(procName);
    if (toNotify != null) {
      toNotify.cancel(message, cause);
    }
  }

  /**
   * Send abort to the specified procedure
   * @param procName name of the procedure to about
   * @param ee exception information about the abort
   */
  public void receiveAbortProcedure(String procName, ForeignException ee) {
    LOG.debug("Request received to abort procedure " + procName, ee);
    // if we know about the procedure, notify it
    Subprocedure sub = subprocs.get(procName);
    if (sub == null) {
      LOG.info("Received abort on procedure with no local subprocedure " + procName +
          ", ignoring it.", ee);
      return; // Procedure has already completed
    }
    String msg = "Propagating foreign exception to subprocedure " + sub.getName();
    LOG.error(msg, ee);
    sub.cancel(msg, ee);
  }
}
