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

package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.procedure2.util.TimeoutBlockingQueue;
import org.apache.hadoop.hbase.procedure2.util.TimeoutBlockingQueue.TimeoutRetriever;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Preconditions;

/**
 * Thread Pool that executes the submitted procedures.
 * The executor has a ProcedureStore associated.
 * Each operation is logged and on restart the pending procedures are resumed.
 *
 * Unless the Procedure code throws an error (e.g. invalid user input)
 * the procedure will complete (at some point in time), On restart the pending
 * procedures are resumed and the once failed will be rolledback.
 *
 * The user can add procedures to the executor via submitProcedure(proc)
 * check for the finished state via isFinished(procId)
 * and get the result via getResult(procId)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureExecutor<TEnvironment> {
  private static final Log LOG = LogFactory.getLog(ProcedureExecutor.class);

  Testing testing = null;
  public static class Testing {
    protected boolean killBeforeStoreUpdate = false;
    protected boolean toggleKillBeforeStoreUpdate = false;

    protected boolean shouldKillBeforeStoreUpdate() {
      final boolean kill = this.killBeforeStoreUpdate;
      if (this.toggleKillBeforeStoreUpdate) {
        this.killBeforeStoreUpdate = !kill;
        LOG.warn("Toggle Kill before store update to: " + this.killBeforeStoreUpdate);
      }
      return kill;
    }
  }

  public interface ProcedureExecutorListener {
    void procedureLoaded(long procId);
    void procedureAdded(long procId);
    void procedureFinished(long procId);
  }

  /**
   * Used by the TimeoutBlockingQueue to get the timeout interval of the procedure
   */
  private static class ProcedureTimeoutRetriever implements TimeoutRetriever<Procedure> {
    @Override
    public long getTimeout(Procedure proc) {
      return proc.getTimeRemaining();
    }

    @Override
    public TimeUnit getTimeUnit(Procedure proc) {
      return TimeUnit.MILLISECONDS;
    }
  }

  /**
   * Internal cleaner that removes the completed procedure results after a TTL.
   * NOTE: This is a special case handled in timeoutLoop().
   *
   * Since the client code looks more or less like:
   *   procId = master.doOperation()
   *   while (master.getProcResult(procId) == ProcInProgress);
   * The master should not throw away the proc result as soon as the procedure is done
   * but should wait a result request from the client (see executor.removeResult(procId))
   * The client will call something like master.isProcDone() or master.getProcResult()
   * which will return the result/state to the client, and it will mark the completed
   * proc as ready to delete. note that the client may not receive the response from
   * the master (e.g. master failover) so, if we delay a bit the real deletion of
   * the proc result the client will be able to get the result the next try.
   */
  private static class CompletedProcedureCleaner<TEnvironment> extends Procedure<TEnvironment> {
    private static final Log LOG = LogFactory.getLog(CompletedProcedureCleaner.class);

    private static final String CLEANER_INTERVAL_CONF_KEY = "hbase.procedure.cleaner.interval";
    private static final int DEFAULT_CLEANER_INTERVAL = 30 * 1000; // 30sec

    private static final String EVICT_TTL_CONF_KEY = "hbase.procedure.cleaner.evict.ttl";
    private static final int DEFAULT_EVICT_TTL = 15 * 60000; // 15min

    private static final String EVICT_ACKED_TTL_CONF_KEY ="hbase.procedure.cleaner.acked.evict.ttl";
    private static final int DEFAULT_ACKED_EVICT_TTL = 5 * 60000; // 5min

    private final Map<Long, ProcedureResult> completed;
    private final ProcedureStore store;
    private final Configuration conf;

    public CompletedProcedureCleaner(final Configuration conf, final ProcedureStore store,
        final Map<Long, ProcedureResult> completedMap) {
      // set the timeout interval that triggers the periodic-procedure
      setTimeout(conf.getInt(CLEANER_INTERVAL_CONF_KEY, DEFAULT_CLEANER_INTERVAL));
      this.completed = completedMap;
      this.store = store;
      this.conf = conf;
    }

    public void periodicExecute(final TEnvironment env) {
      if (completed.isEmpty()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("No completed procedures to cleanup.");
        }
        return;
      }

      final long evictTtl = conf.getInt(EVICT_TTL_CONF_KEY, DEFAULT_EVICT_TTL);
      final long evictAckTtl = conf.getInt(EVICT_ACKED_TTL_CONF_KEY, DEFAULT_ACKED_EVICT_TTL);

      long now = EnvironmentEdgeManager.currentTime();
      Iterator<Map.Entry<Long, ProcedureResult>> it = completed.entrySet().iterator();
      while (it.hasNext() && store.isRunning()) {
        Map.Entry<Long, ProcedureResult> entry = it.next();
        ProcedureResult result = entry.getValue();

        // TODO: Select TTL based on Procedure type
        if ((result.hasClientAckTime() && (now - result.getClientAckTime()) >= evictAckTtl) ||
            (now - result.getLastUpdate()) >= evictTtl) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Evict completed procedure " + entry.getKey());
          }
          store.delete(entry.getKey());
          it.remove();
        }
      }
    }

    @Override
    protected Procedure[] execute(final TEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void rollback(final TEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(final TEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void serializeStateData(final OutputStream stream) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deserializeStateData(final InputStream stream) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Map the the procId returned by submitProcedure(), the Root-ProcID, to the ProcedureResult.
   * Once a Root-Procedure completes (success or failure), the result will be added to this map.
   * The user of ProcedureExecutor should call getResult(procId) to get the result.
   */
  private final ConcurrentHashMap<Long, ProcedureResult> completed =
    new ConcurrentHashMap<Long, ProcedureResult>();

  /**
   * Map the the procId returned by submitProcedure(), the Root-ProcID, to the RootProcedureState.
   * The RootProcedureState contains the execution stack of the Root-Procedure,
   * It is added to the map by submitProcedure() and removed on procedure completion.
   */
  private final ConcurrentHashMap<Long, RootProcedureState> rollbackStack =
    new ConcurrentHashMap<Long, RootProcedureState>();

  /**
   * Helper map to lookup the live procedures by ID.
   * This map contains every procedure. root-procedures and subprocedures.
   */
  private final ConcurrentHashMap<Long, Procedure> procedures =
    new ConcurrentHashMap<Long, Procedure>();

  /**
   * Timeout Queue that contains Procedures in a WAITING_TIMEOUT state
   * or periodic procedures.
   */
  private final TimeoutBlockingQueue<Procedure> waitingTimeout =
    new TimeoutBlockingQueue<Procedure>(new ProcedureTimeoutRetriever());

  /**
   * Queue that contains runnable procedures.
   */
  private final ProcedureRunnableSet runnables;

  // TODO
  private final ReentrantLock submitLock = new ReentrantLock();
  private final AtomicLong lastProcId = new AtomicLong(-1);

  private final CopyOnWriteArrayList<ProcedureExecutorListener> listeners =
    new CopyOnWriteArrayList<ProcedureExecutorListener>();

  private final AtomicInteger activeExecutorCount = new AtomicInteger(0);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final TEnvironment environment;
  private final ProcedureStore store;
  private final Configuration conf;

  private Thread[] threads;

  public ProcedureExecutor(final Configuration conf, final TEnvironment environment,
      final ProcedureStore store) {
    this(conf, environment, store, new ProcedureSimpleRunQueue());
  }

  public ProcedureExecutor(final Configuration conf, final TEnvironment environment,
      final ProcedureStore store, final ProcedureRunnableSet runqueue) {
    this.environment = environment;
    this.runnables = runqueue;
    this.store = store;
    this.conf = conf;
  }

  private void load(final boolean abortOnCorruption) throws IOException {
    Preconditions.checkArgument(completed.isEmpty());
    Preconditions.checkArgument(rollbackStack.isEmpty());
    Preconditions.checkArgument(procedures.isEmpty());
    Preconditions.checkArgument(waitingTimeout.isEmpty());
    Preconditions.checkArgument(runnables.size() == 0);

    store.load(new ProcedureStore.ProcedureLoader() {
      @Override
      public void setMaxProcId(long maxProcId) {
        assert lastProcId.get() < 0 : "expected only one call to setMaxProcId()";
        LOG.debug("load procedures maxProcId=" + maxProcId);
        lastProcId.set(maxProcId);
      }

      @Override
      public void load(ProcedureIterator procIter) throws IOException {
        loadProcedures(procIter, abortOnCorruption);
      }

      @Override
      public void handleCorrupted(ProcedureIterator procIter) throws IOException {
        int corruptedCount = 0;
        while (procIter.hasNext()) {
          Procedure proc = procIter.next();
          LOG.error("corrupted procedure: " + proc);
          corruptedCount++;
        }
        if (abortOnCorruption && corruptedCount > 0) {
          throw new IOException("found " + corruptedCount + " procedures on replay");
        }
      }
    });
  }

  private void loadProcedures(final ProcedureIterator procIter,
      final boolean abortOnCorruption) throws IOException {
    // 1. Build the rollback stack
    int runnablesCount = 0;
    while (procIter.hasNext()) {
      Procedure proc = procIter.next();
      if (!proc.hasParent() && !proc.isFinished()) {
        rollbackStack.put(proc.getProcId(), new RootProcedureState());
      }
      // add the procedure to the map
      proc.beforeReplay(getEnvironment());
      procedures.put(proc.getProcId(), proc);

      if (proc.getState() == ProcedureState.RUNNABLE) {
        runnablesCount++;
      }
    }

    // 2. Initialize the stacks
    ArrayList<Procedure> runnableList = new ArrayList(runnablesCount);
    HashSet<Procedure> waitingSet = null;
    procIter.reset();
    while (procIter.hasNext()) {
      Procedure proc = procIter.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Loading procedure state=%s isFailed=%s: %s",
                    proc.getState(), proc.hasException(), proc));
      }

      Long rootProcId = getRootProcedureId(proc);
      if (rootProcId == null) {
        // The 'proc' was ready to run but the root procedure was rolledback?
        runnables.addBack(proc);
        continue;
      }

      if (!proc.hasParent() && proc.isFinished()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("The procedure is completed state=%s isFailed=%s",
                    proc.getState(), proc.hasException()));
        }
        assert !rollbackStack.containsKey(proc.getProcId());
        procedures.remove(proc.getProcId());
        completed.put(proc.getProcId(), newResultFromProcedure(proc));
        continue;
      }

      if (proc.hasParent() && !proc.isFinished()) {
        Procedure parent = procedures.get(proc.getParentProcId());
        // corrupted procedures are handled later at step 3
        if (parent != null) {
          parent.incChildrenLatch();
        }
      }

      RootProcedureState procStack = rollbackStack.get(rootProcId);
      procStack.loadStack(proc);

      switch (proc.getState()) {
        case RUNNABLE:
          runnableList.add(proc);
          break;
        case WAITING_TIMEOUT:
          if (waitingSet == null) {
            waitingSet = new HashSet<Procedure>();
          }
          waitingSet.add(proc);
          break;
        case FINISHED:
          if (proc.hasException()) {
            // add the proc to the runnables to perform the rollback
            runnables.addBack(proc);
            break;
          }
        case ROLLEDBACK:
        case INITIALIZING:
          String msg = "Unexpected " + proc.getState() + " state for " + proc;
          LOG.error(msg);
          throw new UnsupportedOperationException(msg);
        default:
          break;
      }
    }

    // 3. Validate the stacks
    int corruptedCount = 0;
    Iterator<Map.Entry<Long, RootProcedureState>> itStack = rollbackStack.entrySet().iterator();
    while (itStack.hasNext()) {
      Map.Entry<Long, RootProcedureState> entry = itStack.next();
      RootProcedureState procStack = entry.getValue();
      if (procStack.isValid()) continue;

      for (Procedure proc: procStack.getSubprocedures()) {
        LOG.error("corrupted procedure: " + proc);
        procedures.remove(proc.getProcId());
        runnableList.remove(proc);
        if (waitingSet != null) waitingSet.remove(proc);
        corruptedCount++;
      }
      itStack.remove();
    }

    if (abortOnCorruption && corruptedCount > 0) {
      throw new IOException("found " + corruptedCount + " procedures on replay");
    }

    // 4. Push the runnables
    if (!runnableList.isEmpty()) {
      // TODO: See ProcedureWALFormatReader#hasFastStartSupport
      // some procedure may be started way before this stuff.
      for (int i = runnableList.size() - 1; i >= 0; --i) {
        Procedure proc = runnableList.get(i);
        if (!proc.hasParent()) {
          sendProcedureLoadedNotification(proc.getProcId());
        }
        if (proc.wasExecuted()) {
          runnables.addFront(proc);
        } else {
          // if it was not in execution, it can wait.
          runnables.addBack(proc);
        }
      }
    }
  }

  /**
   * Start the procedure executor.
   * It calls ProcedureStore.recoverLease() and ProcedureStore.load() to
   * recover the lease, and ensure a single executor, and start the procedure
   * replay to resume and recover the previous pending and in-progress perocedures.
   *
   * @param numThreads number of threads available for procedure execution.
   * @param abortOnCorruption true if you want to abort your service in case
   *          a corrupted procedure is found on replay. otherwise false.
   */
  public void start(int numThreads, boolean abortOnCorruption) throws IOException {
    if (running.getAndSet(true)) {
      LOG.warn("Already running");
      return;
    }

    // We have numThreads executor + one timer thread used for timing out
    // procedures and triggering periodic procedures.
    threads = new Thread[numThreads + 1];
    LOG.info("Starting procedure executor threads=" + threads.length);

    // Initialize procedures executor
    for (int i = 0; i < numThreads; ++i) {
      threads[i] = new Thread("ProcedureExecutorThread-" + i) {
        @Override
        public void run() {
          execLoop();
        }
      };
    }

    // Initialize procedures timeout handler (this is the +1 thread)
    threads[numThreads] = new Thread("ProcedureExecutorTimeoutThread") {
      @Override
      public void run() {
        timeoutLoop();
      }
    };

    // Acquire the store lease.
    store.recoverLease();

    // TODO: Split in two steps.
    // TODO: Handle corrupted procedures (currently just a warn)
    // The first one will make sure that we have the latest id,
    // so we can start the threads and accept new procedures.
    // The second step will do the actual load of old procedures.
    load(abortOnCorruption);

    // Start the executors. Here we must have the lastProcId set.
    for (int i = 0; i < threads.length; ++i) {
      threads[i].start();
    }

    // Add completed cleaner
    waitingTimeout.add(new CompletedProcedureCleaner(conf, store, completed));
  }

  public void stop() {
    if (!running.getAndSet(false)) {
      return;
    }

    LOG.info("Stopping the procedure executor");
    runnables.signalAll();
    waitingTimeout.signalAll();
  }

  public void join() {
    boolean interrupted = false;

    for (int i = 0; i < threads.length; ++i) {
      try {
        threads[i].join();
      } catch (InterruptedException ex) {
        interrupted = true;
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }

    completed.clear();
    rollbackStack.clear();
    procedures.clear();
    waitingTimeout.clear();
    runnables.clear();
    lastProcId.set(-1);
  }

  public boolean isRunning() {
    return running.get();
  }

  /**
   * @return the number of execution threads.
   */
  public int getNumThreads() {
    return threads == null ? 0 : (threads.length - 1);
  }

  public int getActiveExecutorCount() {
    return activeExecutorCount.get();
  }

  public TEnvironment getEnvironment() {
    return this.environment;
  }

  public ProcedureStore getStore() {
    return this.store;
  }

  public void registerListener(ProcedureExecutorListener listener) {
    this.listeners.add(listener);
  }

  public boolean unregisterListener(ProcedureExecutorListener listener) {
    return this.listeners.remove(listener);
  }

  /**
   * Add a new root-procedure to the executor.
   * @param proc the new procedure to execute.
   * @return the procedure id, that can be used to monitor the operation
   */
  public long submitProcedure(final Procedure proc) {
    Preconditions.checkArgument(proc.getState() == ProcedureState.INITIALIZING);
    Preconditions.checkArgument(isRunning());
    Preconditions.checkArgument(lastProcId.get() >= 0);
    Preconditions.checkArgument(!proc.hasParent());

    // Initialize the Procedure ID
    proc.setProcId(nextProcId());

    // Commit the transaction
    store.insert(proc, null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Procedure " + proc + " added to the store.");
    }

    // Create the rollback stack for the procedure
    RootProcedureState stack = new RootProcedureState();
    rollbackStack.put(proc.getProcId(), stack);

    // Submit the new subprocedures
    assert !procedures.containsKey(proc.getProcId());
    procedures.put(proc.getProcId(), proc);
    sendProcedureAddedNotification(proc.getProcId());
    runnables.addBack(proc);
    return proc.getProcId();
  }

  public ProcedureResult getResult(final long procId) {
    return completed.get(procId);
  }

  /**
   * Return true if the procedure is finished.
   * The state may be "completed successfully" or "failed and rolledback".
   * Use getResult() to check the state or get the result data.
   * @param procId the ID of the procedure to check
   * @return true if the procedure execution is finished, otherwise false.
   */
  public boolean isFinished(final long procId) {
    return completed.containsKey(procId);
  }

  /**
   * Return true if the procedure is started.
   * @param procId the ID of the procedure to check
   * @return true if the procedure execution is started, otherwise false.
   */
  public boolean isStarted(final long procId) {
    Procedure proc = procedures.get(procId);
    if (proc == null) {
      return completed.get(procId) != null;
    }
    return proc.wasExecuted();
  }

  /**
   * Mark the specified completed procedure, as ready to remove.
   * @param procId the ID of the procedure to remove
   */
  public void removeResult(final long procId) {
    ProcedureResult result = completed.get(procId);
    if (result == null) {
      assert !procedures.containsKey(procId) : "procId=" + procId + " is still running";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Procedure procId=" + procId + " already removed by the cleaner.");
      }
      return;
    }

    // The CompletedProcedureCleaner will take care of deletion, once the TTL is expired.
    result.setClientAckTime(EnvironmentEdgeManager.currentTime());
  }

  /**
   * Send an abort notification the specified procedure.
   * Depending on the procedure implementation the abort can be considered or ignored.
   * @param procId the procedure to abort
   * @return true if the procedure exist and has received the abort, otherwise false.
   */
  public boolean abort(final long procId) {
    Procedure proc = procedures.get(procId);
    if (proc != null) {
      return proc.abort(getEnvironment());
    }
    return false;
  }

  public Map<Long, ProcedureResult> getResults() {
    return Collections.unmodifiableMap(completed);
  }

  public Procedure getProcedure(final long procId) {
    return procedures.get(procId);
  }

  protected ProcedureRunnableSet getRunnableSet() {
    return runnables;
  }

  /**
   * Execution loop (N threads)
   * while the executor is in a running state,
   * fetch a procedure from the runnables queue and start the execution.
   */
  private void execLoop() {
    while (isRunning()) {
      Long procId = runnables.poll();
      Procedure proc = procId != null ? procedures.get(procId) : null;
      if (proc == null) continue;

      try {
        activeExecutorCount.incrementAndGet();
        execLoop(proc);
      } finally {
        activeExecutorCount.decrementAndGet();
      }
    }
  }

  private void execLoop(Procedure proc) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Trying to start the execution of " + proc);
    }

    Long rootProcId = getRootProcedureId(proc);
    if (rootProcId == null) {
      // The 'proc' was ready to run but the root procedure was rolledback
      executeRollback(proc);
      return;
    }

    RootProcedureState procStack = rollbackStack.get(rootProcId);
    if (procStack == null) return;

    do {
      // Try to acquire the execution
      if (!procStack.acquire(proc)) {
        if (procStack.setRollback()) {
          // we have the 'rollback-lock' we can start rollingback
          if (!executeRollback(rootProcId, procStack)) {
            procStack.unsetRollback();
            runnables.yield(proc);
          }
        } else {
          // if we can't rollback means that some child is still running.
          // the rollback will be executed after all the children are done.
          // If the procedure was never executed, remove and mark it as rolledback.
          if (!proc.wasExecuted()) {
            if (!executeRollback(proc)) {
              runnables.yield(proc);
            }
          }
        }
        break;
      }

      // Execute the procedure
      assert proc.getState() == ProcedureState.RUNNABLE;
      if (proc.acquireLock(getEnvironment())) {
        execProcedure(procStack, proc);
        proc.releaseLock(getEnvironment());
      } else {
        runnables.yield(proc);
      }
      procStack.release(proc);

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && !isRunning()) {
        break;
      }

      if (proc.getProcId() == rootProcId && proc.isSuccess()) {
        // Finalize the procedure state
        if (LOG.isDebugEnabled()) {
          LOG.debug("Procedure completed in " +
              StringUtils.humanTimeDiff(proc.elapsedTime()) + ": " + proc);
        }
        procedureFinished(proc);
        break;
      }

      // if the procedure is kind enough to pass the slot to someone else, yield
      if (proc.isYieldAfterExecutionStep(getEnvironment())) {
        runnables.yield(proc);
        break;
      }
    } while (procStack.isFailed());
  }

  private void timeoutLoop() {
    while (isRunning()) {
      Procedure proc = waitingTimeout.poll();
      if (proc == null) continue;

      if (proc.getTimeRemaining() > 100) {
        // got an early wake, maybe a stop?
        // re-enqueue the task in case was not a stop or just a signal
        waitingTimeout.add(proc);
        continue;
      }

      // ----------------------------------------------------------------------------
      // TODO-MAYBE: Should we provide a notification to the store with the
      // full set of procedures pending and completed to write a compacted
      // version of the log (in case is a log)?
      // In theory no, procedures are have a short life, so at some point the store
      // will have the tracker saying everything is in the last log.
      // ----------------------------------------------------------------------------

      // The CompletedProcedureCleaner is a special case, and it acts as a chore.
      // instead of bringing the Chore class in, we reuse this timeout thread for
      // this special case.
      if (proc instanceof CompletedProcedureCleaner) {
        try {
          ((CompletedProcedureCleaner)proc).periodicExecute(getEnvironment());
        } catch (Throwable e) {
          LOG.error("Ignoring CompletedProcedureCleaner exception: " + e.getMessage(), e);
        }
        proc.setStartTime(EnvironmentEdgeManager.currentTime());
        waitingTimeout.add(proc);
        continue;
      }

      // The procedure received an "abort-timeout", call abort() and
      // add the procedure back in the queue for rollback.
      if (proc.setTimeoutFailure()) {
        long rootProcId = Procedure.getRootProcedureId(procedures, proc);
        RootProcedureState procStack = rollbackStack.get(rootProcId);
        procStack.abort();
        store.update(proc);
        runnables.addFront(proc);
        continue;
      }
    }
  }

  /**
   * Execute the rollback of the full procedure stack.
   * Once the procedure is rolledback, the root-procedure will be visible as
   * finished to user, and the result will be the fatal exception.
   */
  private boolean executeRollback(final long rootProcId, final RootProcedureState procStack) {
    Procedure rootProc = procedures.get(rootProcId);
    RemoteProcedureException exception = rootProc.getException();
    if (exception == null) {
      exception = procStack.getException();
      rootProc.setFailure(exception);
      store.update(rootProc);
    }

    List<Procedure> subprocStack = procStack.getSubprocedures();
    assert subprocStack != null : "Called rollback with no steps executed rootProc=" + rootProc;

    int stackTail = subprocStack.size();
    boolean reuseLock = false;
    while (stackTail --> 0) {
      final Procedure proc = subprocStack.get(stackTail);

      if (!reuseLock && !proc.acquireLock(getEnvironment())) {
        // can't take a lock on the procedure, add the root-proc back on the
        // queue waiting for the lock availability
        return false;
      }

      boolean abortRollback = !executeRollback(proc);
      abortRollback |= !isRunning() || !store.isRunning();

      // If the next procedure is the same to this one
      // (e.g. StateMachineProcedure reuse the same instance)
      // we can avoid to lock/unlock each step
      reuseLock = stackTail > 0 && (subprocStack.get(stackTail - 1) == proc) && !abortRollback;
      if (!reuseLock) {
        proc.releaseLock(getEnvironment());
      }

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (abortRollback) {
        return false;
      }

      subprocStack.remove(stackTail);

      // if the procedure is kind enough to pass the slot to someone else, yield
      if (proc.isYieldAfterExecutionStep(getEnvironment())) {
        return false;
      }
    }

    // Finalize the procedure state
    LOG.info("Rolledback procedure " + rootProc +
             " exec-time=" + StringUtils.humanTimeDiff(rootProc.elapsedTime()) +
             " exception=" + exception.getMessage());
    procedureFinished(rootProc);
    return true;
  }

  /**
   * Execute the rollback of the procedure step.
   * It updates the store with the new state (stack index)
   * or will remove completly the procedure in case it is a child.
   */
  private boolean executeRollback(final Procedure proc) {
    try {
      proc.doRollback(getEnvironment());
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("rollback attempt failed for " + proc, e);
      }
      return false;
    } catch (InterruptedException e) {
      handleInterruptedException(proc, e);
      return false;
    } catch (Throwable e) {
      // Catch NullPointerExceptions or similar errors...
      LOG.fatal("CODE-BUG: Uncatched runtime exception for procedure: " + proc, e);
    }

    // allows to kill the executor before something is stored to the wal.
    // useful to test the procedure recovery.
    if (testing != null && testing.shouldKillBeforeStoreUpdate()) {
      LOG.debug("TESTING: Kill before store update");
      stop();
      return false;
    }

    if (proc.removeStackIndex()) {
      proc.setState(ProcedureState.ROLLEDBACK);
      if (proc.hasParent()) {
        store.delete(proc.getProcId());
        procedures.remove(proc.getProcId());
      } else {
        store.update(proc);
      }
    } else {
      store.update(proc);
    }

    return true;
  }

  /**
   * Executes the specified procedure
   *  - calls the doExecute() of the procedure
   *  - if the procedure execution didn't fail (e.g. invalid user input)
   *     - ...and returned subprocedures
   *        - the subprocedures are initialized.
   *        - the subprocedures are added to the store
   *        - the subprocedures are added to the runnable queue
   *        - the procedure is now in a WAITING state, waiting for the subprocedures to complete
   *     - ...if there are no subprocedure
   *        - the procedure completed successfully
   *        - if there is a parent (WAITING)
   *            - the parent state will be set to RUNNABLE
   *  - in case of failure
   *    - the store is updated with the new state
   *    - the executor (caller of this method) will start the rollback of the procedure
   */
  private void execProcedure(final RootProcedureState procStack, final Procedure procedure) {
    Preconditions.checkArgument(procedure.getState() == ProcedureState.RUNNABLE);

    // Execute the procedure
    boolean reExecute = false;
    Procedure[] subprocs = null;
    do {
      reExecute = false;
      try {
        subprocs = procedure.doExecute(getEnvironment());
        if (subprocs != null && subprocs.length == 0) {
          subprocs = null;
        }
      } catch (ProcedureYieldException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Yield procedure: " + procedure + ": " + e.getMessage());
        }
        runnables.yield(procedure);
        return;
      } catch (InterruptedException e) {
        handleInterruptedException(procedure, e);
        runnables.yield(procedure);
        return;
      } catch (Throwable e) {
        // Catch NullPointerExceptions or similar errors...
        String msg = "CODE-BUG: Uncatched runtime exception for procedure: " + procedure;
        LOG.error(msg, e);
        procedure.setFailure(new RemoteProcedureException(msg, e));
      }

      if (!procedure.isFailed()) {
        if (subprocs != null) {
          if (subprocs.length == 1 && subprocs[0] == procedure) {
            // quick-shortcut for a state machine like procedure
            subprocs = null;
            reExecute = true;
          } else {
            // yield the current procedure, and make the subprocedure runnable
            for (int i = 0; i < subprocs.length; ++i) {
              Procedure subproc = subprocs[i];
              if (subproc == null) {
                String msg = "subproc[" + i + "] is null, aborting the procedure";
                procedure.setFailure(new RemoteProcedureException(msg,
                  new IllegalArgumentException(msg)));
                subprocs = null;
                break;
              }

              assert subproc.getState() == ProcedureState.INITIALIZING;
              subproc.setParentProcId(procedure.getProcId());
              subproc.setProcId(nextProcId());
            }

            if (!procedure.isFailed()) {
              procedure.setChildrenLatch(subprocs.length);
              switch (procedure.getState()) {
                case RUNNABLE:
                  procedure.setState(ProcedureState.WAITING);
                  break;
                case WAITING_TIMEOUT:
                  waitingTimeout.add(procedure);
                  break;
                default:
                  break;
              }
            }
          }
        } else if (procedure.getState() == ProcedureState.WAITING_TIMEOUT) {
          waitingTimeout.add(procedure);
        } else {
          // No subtask, so we are done
          procedure.setState(ProcedureState.FINISHED);
        }
      }

      // Add the procedure to the stack
      procStack.addRollbackStep(procedure);

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && testing.shouldKillBeforeStoreUpdate()) {
        LOG.debug("TESTING: Kill before store update");
        stop();
        return;
      }

      // Commit the transaction
      if (subprocs != null && !procedure.isFailed()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Store add " + procedure + " children " + Arrays.toString(subprocs));
        }
        store.insert(procedure, subprocs);
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Store update " + procedure);
        }
        store.update(procedure);
      }

      // if the store is not running we are aborting
      if (!store.isRunning()) {
        return;
      }

      // if the procedure is kind enough to pass the slot to someone else, yield
      if (reExecute && procedure.isYieldAfterExecutionStep(getEnvironment())) {
        return;
      }

      assert (reExecute && subprocs == null) || !reExecute;
    } while (reExecute);

    // Submit the new subprocedures
    if (subprocs != null && !procedure.isFailed()) {
      for (int i = 0; i < subprocs.length; ++i) {
        Procedure subproc = subprocs[i];
        assert !procedures.containsKey(subproc.getProcId());
        procedures.put(subproc.getProcId(), subproc);
        runnables.addFront(subproc);
      }
    }

    if (procedure.isFinished() && procedure.hasParent()) {
      Procedure parent = procedures.get(procedure.getParentProcId());
      if (parent == null) {
        assert procStack.isRollingback();
        return;
      }

      // If this procedure is the last child awake the parent procedure
      if (LOG.isTraceEnabled()) {
        LOG.trace(parent + " child is done: " + procedure);
      }
      if (parent.childrenCountDown() && parent.getState() == ProcedureState.WAITING) {
        parent.setState(ProcedureState.RUNNABLE);
        store.update(parent);
        runnables.addFront(parent);
        if (LOG.isTraceEnabled()) {
          LOG.trace(parent + " all the children finished their work, resume.");
        }
        return;
      }
    }
  }

  private void handleInterruptedException(final Procedure proc, final InterruptedException e) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("got an interrupt during " + proc + ". suspend and retry it later.", e);
    }

    // NOTE: We don't call Thread.currentThread().interrupt()
    // because otherwise all the subsequent calls e.g. Thread.sleep() will throw
    // the InterruptedException. If the master is going down, we will be notified
    // and the executor/store will be stopped.
    // (The interrupted procedure will be retried on the next run)
  }

  private void sendProcedureLoadedNotification(final long procId) {
    if (!this.listeners.isEmpty()) {
      for (ProcedureExecutorListener listener: this.listeners) {
        try {
          listener.procedureLoaded(procId);
        } catch (Throwable e) {
          LOG.error("The listener " + listener + " had an error: " + e.getMessage(), e);
        }
      }
    }
  }

  private void sendProcedureAddedNotification(final long procId) {
    if (!this.listeners.isEmpty()) {
      for (ProcedureExecutorListener listener: this.listeners) {
        try {
          listener.procedureAdded(procId);
        } catch (Throwable e) {
          LOG.error("The listener " + listener + " had an error: " + e.getMessage(), e);
        }
      }
    }
  }

  private void sendProcedureFinishedNotification(final long procId) {
    if (!this.listeners.isEmpty()) {
      for (ProcedureExecutorListener listener: this.listeners) {
        try {
          listener.procedureFinished(procId);
        } catch (Throwable e) {
          LOG.error("The listener " + listener + " had an error: " + e.getMessage(), e);
        }
      }
    }
  }

  private long nextProcId() {
    long procId = lastProcId.incrementAndGet();
    if (procId < 0) {
      while (!lastProcId.compareAndSet(procId, 0)) {
        procId = lastProcId.get();
        if (procId >= 0)
          break;
      }
      while (procedures.containsKey(procId)) {
        procId = lastProcId.incrementAndGet();
      }
    }
    return procId;
  }

  private Long getRootProcedureId(Procedure proc) {
    return Procedure.getRootProcedureId(procedures, proc);
  }

  private void procedureFinished(final Procedure proc) {
    // call the procedure completion cleanup handler
    try {
      proc.completionCleanup(getEnvironment());
    } catch (Throwable e) {
      // Catch NullPointerExceptions or similar errors...
      LOG.error("CODE-BUG: uncatched runtime exception for procedure: " + proc, e);
    }

    // update the executor internal state maps
    completed.put(proc.getProcId(), newResultFromProcedure(proc));
    rollbackStack.remove(proc.getProcId());
    procedures.remove(proc.getProcId());

    // call the runnableSet completion cleanup handler
    try {
      runnables.completionCleanup(proc);
    } catch (Throwable e) {
      // Catch NullPointerExceptions or similar errors...
      LOG.error("CODE-BUG: uncatched runtime exception for runnableSet: " + runnables, e);
    }

    // Notify the listeners
    sendProcedureFinishedNotification(proc.getProcId());
  }

  public Pair<ProcedureResult, Procedure> getResultOrProcedure(final long procId) {
    ProcedureResult result = completed.get(procId);
    Procedure proc = null;
    if (result == null) {
      proc = procedures.get(procId);
      if (proc == null) {
        result = completed.get(procId);
      }
    }
    return new Pair(result, proc);
  }

  private static ProcedureResult newResultFromProcedure(final Procedure proc) {
    if (proc.isFailed()) {
      return new ProcedureResult(proc.getStartTime(), proc.getLastUpdate(), proc.getException());
    }
    return new ProcedureResult(proc.getStartTime(), proc.getLastUpdate(), proc.getResult());
  }
}
