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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.procedure2.util.DelayedUtil;
import org.apache.hadoop.hbase.procedure2.util.DelayedUtil.DelayedWithTimeout;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.Pair;

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

  public static final String CHECK_OWNER_SET_CONF_KEY = "hbase.procedure.check.owner.set";
  private static final boolean DEFAULT_CHECK_OWNER_SET = false;

  public static final String WORKER_KEEP_ALIVE_TIME_CONF_KEY =
      "hbase.procedure.worker.keep.alive.time.msec";
  private static final long DEFAULT_WORKER_KEEP_ALIVE_TIME = Long.MAX_VALUE;

  Testing testing = null;
  public static class Testing {
    protected boolean killIfSuspended = false;
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

    protected boolean shouldKillBeforeStoreUpdate(final boolean isSuspended) {
      return (isSuspended && !killIfSuspended) ? false : shouldKillBeforeStoreUpdate();
    }
  }

  public interface ProcedureExecutorListener {
    void procedureLoaded(long procId);
    void procedureAdded(long procId);
    void procedureFinished(long procId);
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
  private static class CompletedProcedureCleaner<TEnvironment>
      extends ProcedureInMemoryChore<TEnvironment> {
    private static final Log LOG = LogFactory.getLog(CompletedProcedureCleaner.class);

    private static final String CLEANER_INTERVAL_CONF_KEY = "hbase.procedure.cleaner.interval";
    private static final int DEFAULT_CLEANER_INTERVAL = 30 * 1000; // 30sec

    private static final String EVICT_TTL_CONF_KEY = "hbase.procedure.cleaner.evict.ttl";
    private static final int DEFAULT_EVICT_TTL = 15 * 60000; // 15min

    private static final String EVICT_ACKED_TTL_CONF_KEY ="hbase.procedure.cleaner.acked.evict.ttl";
    private static final int DEFAULT_ACKED_EVICT_TTL = 5 * 60000; // 5min

    private static final String BATCH_SIZE_CONF_KEY = "hbase.procedure.cleaner.evict.batch.size";
    private static final int DEFAULT_BATCH_SIZE = 32;

    private final Map<Long, ProcedureInfo> completed;
    private final Map<NonceKey, Long> nonceKeysToProcIdsMap;
    private final ProcedureStore store;
    private Configuration conf;

    public CompletedProcedureCleaner(final Configuration conf, final ProcedureStore store,
        final Map<Long, ProcedureInfo> completedMap,
        final Map<NonceKey, Long> nonceKeysToProcIdsMap) {
      // set the timeout interval that triggers the periodic-procedure
      super(conf.getInt(CLEANER_INTERVAL_CONF_KEY, DEFAULT_CLEANER_INTERVAL));
      this.completed = completedMap;
      this.nonceKeysToProcIdsMap = nonceKeysToProcIdsMap;
      this.store = store;
      this.conf = conf;
    }

    @Override
    protected void periodicExecute(final TEnvironment env) {
      if (completed.isEmpty()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("No completed procedures to cleanup.");
        }
        return;
      }

      final long evictTtl = conf.getInt(EVICT_TTL_CONF_KEY, DEFAULT_EVICT_TTL);
      final long evictAckTtl = conf.getInt(EVICT_ACKED_TTL_CONF_KEY, DEFAULT_ACKED_EVICT_TTL);
      final int batchSize = conf.getInt(BATCH_SIZE_CONF_KEY, DEFAULT_BATCH_SIZE);

      final long[] batchIds = new long[batchSize];
      int batchCount = 0;

      final long now = EnvironmentEdgeManager.currentTime();
      final Iterator<Map.Entry<Long, ProcedureInfo>> it = completed.entrySet().iterator();
      final boolean isDebugEnabled = LOG.isDebugEnabled();
      while (it.hasNext() && store.isRunning()) {
        final Map.Entry<Long, ProcedureInfo> entry = it.next();
        final ProcedureInfo procInfo = entry.getValue();

        // TODO: Select TTL based on Procedure type
        if ((procInfo.hasClientAckTime() && (now - procInfo.getClientAckTime()) >= evictAckTtl) ||
            (now - procInfo.getLastUpdate()) >= evictTtl) {
          if (isDebugEnabled) {
            LOG.debug("Evict completed procedure: " + procInfo);
          }
          batchIds[batchCount++] = entry.getKey();
          if (batchCount == batchIds.length) {
            store.delete(batchIds, 0, batchCount);
            batchCount = 0;
          }
          it.remove();

          final NonceKey nonceKey = procInfo.getNonceKey();
          if (nonceKey != null) {
            nonceKeysToProcIdsMap.remove(nonceKey);
          }
        }
      }
      if (batchCount > 0) {
        store.delete(batchIds, 0, batchCount);
      }
    }
  }

  /**
   * Map the the procId returned by submitProcedure(), the Root-ProcID, to the ProcedureInfo.
   * Once a Root-Procedure completes (success or failure), the result will be added to this map.
   * The user of ProcedureExecutor should call getResult(procId) to get the result.
   */
  private final ConcurrentHashMap<Long, ProcedureInfo> completed =
    new ConcurrentHashMap<Long, ProcedureInfo>();

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
   * Helper map to lookup whether the procedure already issued from the same client.
   * This map contains every root procedure.
   */
  private final ConcurrentHashMap<NonceKey, Long> nonceKeysToProcIdsMap =
      new ConcurrentHashMap<NonceKey, Long>();

  private final CopyOnWriteArrayList<ProcedureExecutorListener> listeners =
    new CopyOnWriteArrayList<ProcedureExecutorListener>();

  private Configuration conf;
  private ThreadGroup threadGroup;
  private CopyOnWriteArrayList<WorkerThread> workerThreads;
  private TimeoutExecutorThread timeoutExecutor;
  private int corePoolSize;

  private volatile long keepAliveTime = Long.MAX_VALUE;

  /**
   * Scheduler/Queue that contains runnable procedures.
   */
  private final ProcedureScheduler scheduler;

  private final AtomicLong lastProcId = new AtomicLong(-1);
  private final AtomicLong workerId = new AtomicLong(0);
  private final AtomicInteger activeExecutorCount = new AtomicInteger(0);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final TEnvironment environment;
  private final ProcedureStore store;
  private final boolean checkOwnerSet;

  public ProcedureExecutor(final Configuration conf, final TEnvironment environment,
      final ProcedureStore store) {
    this(conf, environment, store, new SimpleProcedureScheduler());
  }

  public ProcedureExecutor(final Configuration conf, final TEnvironment environment,
      final ProcedureStore store, final ProcedureScheduler scheduler) {
    this.environment = environment;
    this.scheduler = scheduler;
    this.store = store;
    this.conf = conf;
    this.checkOwnerSet = conf.getBoolean(CHECK_OWNER_SET_CONF_KEY, DEFAULT_CHECK_OWNER_SET);
    refreshConfiguration(conf);
  }

  private void load(final boolean abortOnCorruption) throws IOException {
    Preconditions.checkArgument(completed.isEmpty(), "completed not empty");
    Preconditions.checkArgument(rollbackStack.isEmpty(), "rollback state not empty");
    Preconditions.checkArgument(procedures.isEmpty(), "procedure map not empty");
    Preconditions.checkArgument(scheduler.size() == 0, "run queue not empty");

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
          ProcedureInfo proc = procIter.nextAsProcedureInfo();
          LOG.error("corrupted procedure: " + proc);
          corruptedCount++;
        }
        if (abortOnCorruption && corruptedCount > 0) {
          throw new IOException("found " + corruptedCount + " corrupted procedure(s) on replay");
        }
      }
    });
  }

  private void loadProcedures(final ProcedureIterator procIter,
      final boolean abortOnCorruption) throws IOException {
    final boolean isDebugEnabled = LOG.isDebugEnabled();

    // 1. Build the rollback stack
    int runnablesCount = 0;
    while (procIter.hasNext()) {
      final NonceKey nonceKey;
      final long procId;

      if (procIter.isNextCompleted()) {
        ProcedureInfo proc = procIter.nextAsProcedureInfo();
        nonceKey = proc.getNonceKey();
        procId = proc.getProcId();
        completed.put(proc.getProcId(), proc);
        if (isDebugEnabled) {
          LOG.debug("The procedure is completed: " + proc);
        }
      } else {
        Procedure proc = procIter.nextAsProcedure();
        nonceKey = proc.getNonceKey();
        procId = proc.getProcId();

        if (!proc.hasParent()) {
          assert !proc.isFinished() : "unexpected finished procedure";
          rollbackStack.put(proc.getProcId(), new RootProcedureState());
        }

        // add the procedure to the map
        proc.beforeReplay(getEnvironment());
        procedures.put(proc.getProcId(), proc);

        if (proc.getState() == ProcedureState.RUNNABLE) {
          runnablesCount++;
        }
      }

      // add the nonce to the map
      if (nonceKey != null) {
        nonceKeysToProcIdsMap.put(nonceKey, procId);
      }
    }

    // 2. Initialize the stacks
    final ArrayList<Procedure> runnableList = new ArrayList(runnablesCount);
    HashSet<Procedure> waitingSet = null;
    procIter.reset();
    while (procIter.hasNext()) {
      if (procIter.isNextCompleted()) {
        procIter.skipNext();
        continue;
      }

      Procedure proc = procIter.nextAsProcedure();
      assert !(proc.isFinished() && !proc.hasParent()) : "unexpected completed proc=" + proc;

      if (isDebugEnabled) {
        LOG.debug(String.format("Loading procedure state=%s isFailed=%s: %s",
                    proc.getState(), proc.hasException(), proc));
      }

      Long rootProcId = getRootProcedureId(proc);
      if (rootProcId == null) {
        // The 'proc' was ready to run but the root procedure was rolledback?
        scheduler.addBack(proc);
        continue;
      }

      if (proc.hasParent()) {
        Procedure parent = procedures.get(proc.getParentProcId());
        // corrupted procedures are handled later at step 3
        if (parent != null && !proc.isFinished()) {
          parent.incChildrenLatch();
        }
      }

      RootProcedureState procStack = rollbackStack.get(rootProcId);
      procStack.loadStack(proc);

      proc.setRootProcId(rootProcId);
      switch (proc.getState()) {
        case RUNNABLE:
          runnableList.add(proc);
          break;
        case WAITING:
          if (!proc.hasChildren()) {
            runnableList.add(proc);
          }
          break;
        case WAITING_TIMEOUT:
          if (waitingSet == null) {
            waitingSet = new HashSet<Procedure>();
          }
          waitingSet.add(proc);
          break;
        case FINISHED:
          if (proc.hasException()) {
            // add the proc to the scheduler to perform the rollback
            scheduler.addBack(proc);
          }
          break;
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

      for (Procedure proc: procStack.getSubproceduresStack()) {
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

    // 4. Push the procedures to the timeout executor
    if (waitingSet != null && !waitingSet.isEmpty()) {
      for (Procedure proc: waitingSet) {
        proc.afterReplay(getEnvironment());
        timeoutExecutor.add(proc);
      }
    }

    // 5. Push the procedure to the scheduler
    if (!runnableList.isEmpty()) {
      // TODO: See ProcedureWALFormatReader#hasFastStartSupport
      // some procedure may be started way before this stuff.
      for (int i = runnableList.size() - 1; i >= 0; --i) {
        Procedure proc = runnableList.get(i);
        proc.afterReplay(getEnvironment());
        if (!proc.hasParent()) {
          sendProcedureLoadedNotification(proc.getProcId());
        }
        if (proc.wasExecuted()) {
          scheduler.addFront(proc);
        } else {
          // if it was not in execution, it can wait.
          scheduler.addBack(proc);
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
    this.corePoolSize = numThreads;
    LOG.info("Starting procedure executor threads=" + corePoolSize);

    // Create the Thread Group for the executors
    threadGroup = new ThreadGroup("ProcedureExecutor");

    // Create the timeout executor
    timeoutExecutor = new TimeoutExecutorThread(threadGroup);

    // Create the workers
    workerId.set(0);
    workerThreads = new CopyOnWriteArrayList<WorkerThread>();
    for (int i = 0; i < corePoolSize; ++i) {
      workerThreads.add(new WorkerThread(threadGroup));
    }

    long st, et;

    // Acquire the store lease.
    st = EnvironmentEdgeManager.currentTime();
    store.recoverLease();
    et = EnvironmentEdgeManager.currentTime();
    LOG.info(String.format("recover procedure store (%s) lease: %s",
      store.getClass().getSimpleName(), StringUtils.humanTimeDiff(et - st)));

    // start the procedure scheduler
    scheduler.start();

    // TODO: Split in two steps.
    // TODO: Handle corrupted procedures (currently just a warn)
    // The first one will make sure that we have the latest id,
    // so we can start the threads and accept new procedures.
    // The second step will do the actual load of old procedures.
    st = EnvironmentEdgeManager.currentTime();
    load(abortOnCorruption);
    et = EnvironmentEdgeManager.currentTime();
    LOG.info(String.format("load procedure store (%s): %s",
      store.getClass().getSimpleName(), StringUtils.humanTimeDiff(et - st)));

    // Start the executors. Here we must have the lastProcId set.
    LOG.debug("start workers " + workerThreads.size());
    timeoutExecutor.start();
    for (WorkerThread worker: workerThreads) {
      worker.start();
    }

    // Internal chores
    timeoutExecutor.add(new WorkerMonitor());

    // Add completed cleaner chore
    addChore(new CompletedProcedureCleaner(conf, store, completed, nonceKeysToProcIdsMap));
  }

  public void stop() {
    if (!running.getAndSet(false)) {
      return;
    }

    LOG.info("Stopping the procedure executor");
    scheduler.stop();
    timeoutExecutor.sendStopSignal();
  }

  public void join() {
    assert !isRunning() : "expected not running";

    // stop the timeout executor
    timeoutExecutor.awaitTermination();
    timeoutExecutor = null;

    // stop the worker threads
    for (WorkerThread worker: workerThreads) {
      worker.awaitTermination();
    }
    workerThreads = null;

    // Destroy the Thread Group for the executors
    try {
      threadGroup.destroy();
    } catch (IllegalThreadStateException e) {
      LOG.error("thread group " + threadGroup + " contains running threads");
      threadGroup.list();
    } finally {
      threadGroup = null;
    }

    // reset the in-memory state for testing
    completed.clear();
    rollbackStack.clear();
    procedures.clear();
    nonceKeysToProcIdsMap.clear();
    scheduler.clear();
    lastProcId.set(-1);
  }

  public void refreshConfiguration(final Configuration conf) {
    this.conf = conf;
    setKeepAliveTime(conf.getLong(WORKER_KEEP_ALIVE_TIME_CONF_KEY,
        DEFAULT_WORKER_KEEP_ALIVE_TIME), TimeUnit.MILLISECONDS);
  }

  // ==========================================================================
  //  Accessors
  // ==========================================================================
  public boolean isRunning() {
    return running.get();
  }

  /**
   * @return the current number of worker threads.
   */
  public int getWorkerThreadCount() {
    return workerThreads.size();
  }

  /**
   * @return the core pool size settings.
   */
  public int getCorePoolSize() {
    return corePoolSize;
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

  protected ProcedureScheduler getScheduler() {
    return scheduler;
  }

  public void setKeepAliveTime(final long keepAliveTime, final TimeUnit timeUnit) {
    this.keepAliveTime = timeUnit.toMillis(keepAliveTime);
    this.scheduler.signalAll();
  }

  public long getKeepAliveTime(final TimeUnit timeUnit) {
    return timeUnit.convert(keepAliveTime, TimeUnit.MILLISECONDS);
  }

  // ==========================================================================
  //  Submit/Remove Chores
  // ==========================================================================

  /**
   * Add a chore procedure to the executor
   * @param chore the chore to add
   */
  public void addChore(final ProcedureInMemoryChore chore) {
    chore.setState(ProcedureState.WAITING_TIMEOUT);
    timeoutExecutor.add(chore);
  }

  /**
   * Remove a chore procedure from the executor
   * @param chore the chore to remove
   * @return whether the chore is removed, or it will be removed later
   */
  public boolean removeChore(final ProcedureInMemoryChore chore) {
    chore.setState(ProcedureState.FINISHED);
    return timeoutExecutor.remove(chore);
  }

  // ==========================================================================
  //  Submit/Abort Procedure
  // ==========================================================================

  /**
   * Add a new root-procedure to the executor.
   * @param proc the new procedure to execute.
   * @return the procedure id, that can be used to monitor the operation
   */
  public long submitProcedure(final Procedure proc) {
    return submitProcedure(proc, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  /**
   * Add a new root-procedure to the executor.
   * @param proc the new procedure to execute.
   * @param nonceGroup
   * @param nonce
   * @return the procedure id, that can be used to monitor the operation
   */
  public long submitProcedure(final Procedure proc, final long nonceGroup, final long nonce) {
    Preconditions.checkArgument(lastProcId.get() >= 0);
    Preconditions.checkArgument(isRunning(), "executor not running");

    // Prepare procedure
    prepareProcedure(proc);

    // Check whether the proc exists.  If exist, just return the proc id.
    // This is to prevent the same proc to submit multiple times (it could happen
    // when client could not talk to server and resubmit the same request).
    if (nonce != HConstants.NO_NONCE) {
      final NonceKey noncekey = new NonceKey(nonceGroup, nonce);
      proc.setNonceKey(noncekey);

      Long oldProcId = nonceKeysToProcIdsMap.putIfAbsent(noncekey, proc.getProcId());
      if (oldProcId != null) {
        // Found the proc
        return oldProcId.longValue();
      }
    }

    // Commit the transaction
    store.insert(proc, null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Procedure " + proc + " added to the store.");
    }

    // Add the procedure to the executor
    return pushProcedure(proc);
  }

  /**
   * Add a set of new root-procedure to the executor.
   * @param procs the new procedures to execute.
   */
  public void submitProcedures(final Procedure[] procs) {
    Preconditions.checkArgument(lastProcId.get() >= 0);
    Preconditions.checkArgument(isRunning(), "executor not running");

    // Prepare procedure
    for (int i = 0; i < procs.length; ++i) {
      prepareProcedure(procs[i]);
    }

    // Commit the transaction
    store.insert(procs);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Procedures added to the store: " + Arrays.toString(procs));
    }

    // Add the procedure to the executor
    for (int i = 0; i < procs.length; ++i) {
      pushProcedure(procs[i]);
    }
  }

  private void prepareProcedure(final Procedure proc) {
    Preconditions.checkArgument(proc.getState() == ProcedureState.INITIALIZING);
    Preconditions.checkArgument(isRunning(), "executor not running");
    Preconditions.checkArgument(!proc.hasParent(), "unexpected parent", proc);
    if (this.checkOwnerSet) {
      Preconditions.checkArgument(proc.hasOwner(), "missing owner");
    }

    // Initialize the Procedure ID
    final long currentProcId = nextProcId();
    proc.setProcId(currentProcId);
  }

  private long pushProcedure(final Procedure proc) {
    final long currentProcId = proc.getProcId();

    // Create the rollback stack for the procedure
    RootProcedureState stack = new RootProcedureState();
    rollbackStack.put(currentProcId, stack);

    // Submit the new subprocedures
    assert !procedures.containsKey(currentProcId);
    procedures.put(currentProcId, proc);
    sendProcedureAddedNotification(currentProcId);
    scheduler.addBack(proc);
    return currentProcId;
  }

  /**
   * Send an abort notification the specified procedure.
   * Depending on the procedure implementation the abort can be considered or ignored.
   * @param procId the procedure to abort
   * @return true if the procedure exist and has received the abort, otherwise false.
   */
  public boolean abort(final long procId) {
    return abort(procId, true);
  }

  /**
   * Send an abort notification the specified procedure.
   * Depending on the procedure implementation the abort can be considered or ignored.
   * @param procId the procedure to abort
   * @param mayInterruptIfRunning if the proc completed at least one step, should it be aborted?
   * @return true if the procedure exist and has received the abort, otherwise false.
   */
  public boolean abort(final long procId, final boolean mayInterruptIfRunning) {
    final Procedure proc = procedures.get(procId);
    if (proc != null) {
      if (!mayInterruptIfRunning && proc.wasExecuted()) {
        return false;
      }
      return proc.abort(getEnvironment());
    }
    return false;
  }

  // ==========================================================================
  //  Executor query helpers
  // ==========================================================================
  public Procedure getProcedure(final long procId) {
    return procedures.get(procId);
  }

  public ProcedureInfo getResult(final long procId) {
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
    return !procedures.containsKey(procId);
  }

  /**
   * Return true if the procedure is started.
   * @param procId the ID of the procedure to check
   * @return true if the procedure execution is started, otherwise false.
   */
  public boolean isStarted(final long procId) {
    final Procedure proc = procedures.get(procId);
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
    final ProcedureInfo result = completed.get(procId);
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

  public Pair<ProcedureInfo, Procedure> getResultOrProcedure(final long procId) {
    ProcedureInfo result = completed.get(procId);
    Procedure proc = null;
    if (result == null) {
      proc = procedures.get(procId);
      if (proc == null) {
        result = completed.get(procId);
      }
    }
    return new Pair(result, proc);
  }

  /**
   * Check if the user is this procedure's owner
   * @param procId the target procedure
   * @param user the user
   * @return true if the user is the owner of the procedure,
   *   false otherwise or the owner is unknown.
   */
  public boolean isProcedureOwner(final long procId, final User user) {
    if (user == null) return false;

    final Procedure proc = procedures.get(procId);
    if (proc != null) {
      return proc.getOwner().equals(user.getShortName());
    }

    final ProcedureInfo procInfo = completed.get(procId);
    if (procInfo == null) {
      // Procedure either does not exist or has already completed and got cleaned up.
      // At this time, we cannot check the owner of the procedure
      return false;
    }
    return ProcedureInfo.isProcedureOwner(procInfo, user);
  }

  /**
   * List procedures.
   * @return the procedures in a list
   */
  public List<ProcedureInfo> listProcedures() {
    final List<ProcedureInfo> procedureLists =
        new ArrayList<ProcedureInfo>(procedures.size() + completed.size());
    for (Map.Entry<Long, Procedure> p: procedures.entrySet()) {
      procedureLists.add(ProcedureUtil.convertToProcedureInfo(p.getValue()));
    }
    for (Map.Entry<Long, ProcedureInfo> e: completed.entrySet()) {
      // Note: The procedure could show up twice in the list with different state, as
      // it could complete after we walk through procedures list and insert into
      // procedureList - it is ok, as we will use the information in the ProcedureInfo
      // to figure it out; to prevent this would increase the complexity of the logic.
      procedureLists.add(e.getValue());
    }
    return procedureLists;
  }

  // ==========================================================================
  //  Listeners helpers
  // ==========================================================================
  public void registerListener(ProcedureExecutorListener listener) {
    this.listeners.add(listener);
  }

  public boolean unregisterListener(ProcedureExecutorListener listener) {
    return this.listeners.remove(listener);
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

  // ==========================================================================
  //  Procedure IDs helpers
  // ==========================================================================
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
    assert procId >= 0 : "Invalid procId " + procId;
    return procId;
  }

  @VisibleForTesting
  protected long getLastProcId() {
    return lastProcId.get();
  }

  private Long getRootProcedureId(Procedure proc) {
    return Procedure.getRootProcedureId(procedures, proc);
  }

  // ==========================================================================
  //  Executions
  // ==========================================================================
  private void executeProcedure(final Procedure proc) {
    final Long rootProcId = getRootProcedureId(proc);
    if (rootProcId == null) {
      // The 'proc' was ready to run but the root procedure was rolledback
      executeRollback(proc);
      return;
    }

    final RootProcedureState procStack = rollbackStack.get(rootProcId);
    if (procStack == null) return;

    do {
      // Try to acquire the execution
      if (!procStack.acquire(proc)) {
        if (procStack.setRollback()) {
          // we have the 'rollback-lock' we can start rollingback
          if (!executeRollback(rootProcId, procStack)) {
            procStack.unsetRollback();
            scheduler.yield(proc);
          }
        } else {
          // if we can't rollback means that some child is still running.
          // the rollback will be executed after all the children are done.
          // If the procedure was never executed, remove and mark it as rolledback.
          if (!proc.wasExecuted()) {
            if (!executeRollback(proc)) {
              scheduler.yield(proc);
            }
          }
        }
        break;
      }

      // Execute the procedure
      assert proc.getState() == ProcedureState.RUNNABLE : proc;
      if (acquireLock(proc)) {
        execProcedure(procStack, proc);
        releaseLock(proc, false);
      } else {
        scheduler.yield(proc);
      }
      procStack.release(proc);

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && !isRunning()) {
        break;
      }

      if (proc.isSuccess()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Procedure completed in " +
              StringUtils.humanTimeDiff(proc.elapsedTime()) + ": " + proc);
        }
        // Finalize the procedure state
        if (proc.getProcId() == rootProcId) {
          procedureFinished(proc);
        } else {
          execCompletionCleanup(proc);
        }
        break;
      }
    } while (procStack.isFailed());
  }

  private boolean acquireLock(final Procedure proc) {
    final TEnvironment env = getEnvironment();
    // hasLock() is used in conjunction with holdLock().
    // This allows us to not rewrite or carry around the hasLock() flag
    // for every procedure. the hasLock() have meaning only if holdLock() is true.
    if (proc.holdLock(env) && proc.hasLock(env)) {
      return true;
    }
    return proc.doAcquireLock(env);
  }

  private void releaseLock(final Procedure proc, final boolean force) {
    final TEnvironment env = getEnvironment();
    // for how the framework works, we know that we will always have the lock
    // when we call releaseLock(), so we can avoid calling proc.hasLock()
    if (force || !proc.holdLock(env)) {
      proc.doReleaseLock(env);
    }
  }

  /**
   * Execute the rollback of the full procedure stack.
   * Once the procedure is rolledback, the root-procedure will be visible as
   * finished to user, and the result will be the fatal exception.
   */
  private boolean executeRollback(final long rootProcId, final RootProcedureState procStack) {
    final Procedure rootProc = procedures.get(rootProcId);
    RemoteProcedureException exception = rootProc.getException();
    if (exception == null) {
      exception = procStack.getException();
      rootProc.setFailure(exception);
      store.update(rootProc);
    }

    final List<Procedure> subprocStack = procStack.getSubproceduresStack();
    assert subprocStack != null : "Called rollback with no steps executed rootProc=" + rootProc;

    int stackTail = subprocStack.size();
    boolean reuseLock = false;
    while (stackTail --> 0) {
      final Procedure proc = subprocStack.get(stackTail);

      if (!reuseLock && !acquireLock(proc)) {
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
        releaseLock(proc, false);
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

      if (proc != rootProc) {
        execCompletionCleanup(proc);
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
        final long[] childProcIds = rollbackStack.get(proc.getProcId()).getSubprocedureIds();
        if (childProcIds != null) {
          store.delete(proc, childProcIds);
        } else {
          store.update(proc);
        }
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
    boolean isSuspended = false;
    boolean reExecute = false;
    Procedure[] subprocs = null;
    do {
      reExecute = false;
      try {
        subprocs = procedure.doExecute(getEnvironment());
        if (subprocs != null && subprocs.length == 0) {
          subprocs = null;
        }
      } catch (ProcedureSuspendedException e) {
        isSuspended = true;
      } catch (ProcedureYieldException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Yield procedure: " + procedure + ": " + e.getMessage());
        }
        scheduler.yield(procedure);
        return;
      } catch (InterruptedException e) {
        handleInterruptedException(procedure, e);
        scheduler.yield(procedure);
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
            subprocs = initializeChildren(procStack, procedure, subprocs);
          }
        } else if (procedure.getState() == ProcedureState.WAITING_TIMEOUT) {
          timeoutExecutor.add(procedure);
        } else if (!isSuspended) {
          // No subtask, so we are done
          procedure.setState(ProcedureState.FINISHED);
        }
      }

      // Add the procedure to the stack
      procStack.addRollbackStep(procedure);

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && testing.shouldKillBeforeStoreUpdate(isSuspended)) {
        LOG.debug("TESTING: Kill before store update: " + procedure);
        stop();
        return;
      }

      // Commit the transaction
      updateStoreOnExec(procStack, procedure, subprocs);

      // if the store is not running we are aborting
      if (!store.isRunning()) return;

      // if the procedure is kind enough to pass the slot to someone else, yield
      if (procedure.isRunnable() && !isSuspended &&
          procedure.isYieldAfterExecutionStep(getEnvironment())) {
        scheduler.yield(procedure);
        return;
      }

      assert (reExecute && subprocs == null) || !reExecute;
    } while (reExecute);

    // Submit the new subprocedures
    if (subprocs != null && !procedure.isFailed()) {
      submitChildrenProcedures(subprocs);
    }

    // if the procedure is complete and has a parent, count down the children latch
    if (procedure.isFinished() && procedure.hasParent()) {
      countDownChildren(procStack, procedure);
    }
  }

  private Procedure[] initializeChildren(final RootProcedureState procStack,
      final Procedure procedure, final Procedure[] subprocs) {
    assert subprocs != null : "expected subprocedures";
    final long rootProcId = getRootProcedureId(procedure);
    for (int i = 0; i < subprocs.length; ++i) {
      final Procedure subproc = subprocs[i];
      if (subproc == null) {
        String msg = "subproc[" + i + "] is null, aborting the procedure";
        procedure.setFailure(new RemoteProcedureException(msg,
          new IllegalArgumentIOException(msg)));
        return null;
      }

      assert subproc.getState() == ProcedureState.INITIALIZING : subproc;
      subproc.setParentProcId(procedure.getProcId());
      subproc.setRootProcId(rootProcId);
      subproc.setProcId(nextProcId());
      procStack.addSubProcedure(subproc);
    }

    if (!procedure.isFailed()) {
      procedure.setChildrenLatch(subprocs.length);
      switch (procedure.getState()) {
        case RUNNABLE:
          procedure.setState(ProcedureState.WAITING);
          break;
        case WAITING_TIMEOUT:
          timeoutExecutor.add(procedure);
          break;
        default:
          break;
      }
    }
    return subprocs;
  }

  private void submitChildrenProcedures(final Procedure[] subprocs) {
    for (int i = 0; i < subprocs.length; ++i) {
      final Procedure subproc = subprocs[i];
      assert !procedures.containsKey(subproc.getProcId());
      procedures.put(subproc.getProcId(), subproc);
      scheduler.addFront(subproc);
    }
  }

  private void countDownChildren(final RootProcedureState procStack, final Procedure procedure) {
    final Procedure parent = procedures.get(procedure.getParentProcId());
    if (parent == null) {
      assert procStack.isRollingback();
      return;
    }

    // If this procedure is the last child awake the parent procedure
    final boolean isTraceEnabled = LOG.isTraceEnabled();
    if (isTraceEnabled) {
      LOG.trace(parent + " child is done: " + procedure);
    }

    if (parent.childrenCountDown() && parent.getState() == ProcedureState.WAITING) {
      parent.setState(ProcedureState.RUNNABLE);
      store.update(parent);
      scheduler.addFront(parent);
      if (isTraceEnabled) {
        LOG.trace(parent + " all the children finished their work, resume.");
      }
      return;
    }
  }

  private void updateStoreOnExec(final RootProcedureState procStack,
      final Procedure procedure, final Procedure[] subprocs) {
    if (subprocs != null && !procedure.isFailed()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Store add " + procedure + " children " + Arrays.toString(subprocs));
      }
      store.insert(procedure, subprocs);
    } else {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Store update " + procedure);
      }
      if (procedure.isFinished() && !procedure.hasParent()) {
        // remove child procedures
        final long[] childProcIds = procStack.getSubprocedureIds();
        if (childProcIds != null) {
          store.delete(procedure, childProcIds);
          for (int i = 0; i < childProcIds.length; ++i) {
            procedures.remove(childProcIds[i]);
          }
        } else {
          store.update(procedure);
        }
      } else {
        store.update(procedure);
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

  private void execCompletionCleanup(final Procedure proc) {
    final TEnvironment env = getEnvironment();
    if (proc.holdLock(env) && proc.hasLock(env)) {
      releaseLock(proc, true);
    }
    try {
      proc.completionCleanup(env);
    } catch (Throwable e) {
      // Catch NullPointerExceptions or similar errors...
      LOG.error("CODE-BUG: uncatched runtime exception for procedure: " + proc, e);
    }
  }

  private void procedureFinished(final Procedure proc) {
    // call the procedure completion cleanup handler
    execCompletionCleanup(proc);

    // update the executor internal state maps
    final ProcedureInfo procInfo = ProcedureUtil.convertToProcedureInfo(proc, proc.getNonceKey());
    if (!proc.shouldWaitClientAck(getEnvironment())) {
      procInfo.setClientAckTime(0);
    }

    completed.put(procInfo.getProcId(), procInfo);
    rollbackStack.remove(proc.getProcId());
    procedures.remove(proc.getProcId());

    // call the runnableSet completion cleanup handler
    try {
      scheduler.completionCleanup(proc);
    } catch (Throwable e) {
      // Catch NullPointerExceptions or similar errors...
      LOG.error("CODE-BUG: uncatched runtime exception for completion cleanup: " + proc, e);
    }

    // Notify the listeners
    sendProcedureFinishedNotification(proc.getProcId());
  }

  // ==========================================================================
  //  Worker Thread
  // ==========================================================================
  private final class WorkerThread extends StoppableThread {
    private final AtomicLong executionStartTime = new AtomicLong(Long.MAX_VALUE);

    public WorkerThread(final ThreadGroup group) {
      super(group, "ProcedureExecutorWorker-" + workerId.incrementAndGet());
    }

    @Override
    public void sendStopSignal() {
      scheduler.signalAll();
    }

    @Override
    public void run() {
      final boolean isTraceEnabled = LOG.isTraceEnabled();
      long lastUpdate = EnvironmentEdgeManager.currentTime();
      while (isRunning() && keepAlive(lastUpdate)) {
        final Procedure procedure = scheduler.poll(keepAliveTime, TimeUnit.MILLISECONDS);
        if (procedure == null) continue;

        activeExecutorCount.incrementAndGet();
        executionStartTime.set(EnvironmentEdgeManager.currentTime());
        try {
          if (isTraceEnabled) {
            LOG.trace("Trying to start the execution of " + procedure);
          }
          executeProcedure(procedure);
        } finally {
          activeExecutorCount.decrementAndGet();
          lastUpdate = EnvironmentEdgeManager.currentTime();
          executionStartTime.set(Long.MAX_VALUE);
        }
      }
      LOG.debug("worker thread terminated " + this);
      workerThreads.remove(this);
    }

    /**
     * @return the time since the current procedure is running
     */
    public long getCurrentRunTime() {
      return EnvironmentEdgeManager.currentTime() - executionStartTime.get();
    }

    private boolean keepAlive(final long lastUpdate) {
      if (workerThreads.size() <= corePoolSize) return true;
      return (EnvironmentEdgeManager.currentTime() - lastUpdate) < keepAliveTime;
    }
  }

  // ==========================================================================
  //  Timeout Thread
  // ==========================================================================
  private final class TimeoutExecutorThread extends StoppableThread {
    private final DelayQueue<DelayedWithTimeout> queue = new DelayQueue<DelayedWithTimeout>();

    public TimeoutExecutorThread(final ThreadGroup group) {
      super(group, "ProcedureTimeoutExecutor");
    }

    @Override
    public void sendStopSignal() {
      queue.add(DelayedUtil.DELAYED_POISON);
    }

    @Override
    public void run() {
      final boolean isTraceEnabled = LOG.isTraceEnabled();
      while (isRunning()) {
        final DelayedWithTimeout task = DelayedUtil.takeWithoutInterrupt(queue);
        if (task == null || task == DelayedUtil.DELAYED_POISON) {
          // the executor may be shutting down,
          // and the task is just the shutdown request
          continue;
        }

        if (isTraceEnabled) {
          LOG.trace("Trying to start the execution of " + task);
        }

        // execute the task
        if (task instanceof InlineChore) {
          execInlineChore((InlineChore)task);
        } else if (task instanceof DelayedProcedure) {
          execDelayedProcedure((DelayedProcedure)task);
        } else {
          LOG.error("CODE-BUG unknown timeout task type " + task);
        }
      }
    }

    public void add(final InlineChore chore) {
      chore.refreshTimeout();
      queue.add(chore);
    }

    public void add(final Procedure procedure) {
      assert procedure.getState() == ProcedureState.WAITING_TIMEOUT;
      queue.add(new DelayedProcedure(procedure));
    }

    public boolean remove(final Procedure procedure) {
      return queue.remove(new DelayedProcedure(procedure));
    }

    private void execInlineChore(final InlineChore chore) {
      chore.run();
      add(chore);
    }

    private void execDelayedProcedure(final DelayedProcedure delayed) {
      // TODO: treat this as a normal procedure, add it to the scheduler and
      // let one of the workers handle it.
      // Today we consider ProcedureInMemoryChore as InlineChores
      final Procedure procedure = delayed.getObject();
      if (procedure instanceof ProcedureInMemoryChore) {
        executeInMemoryChore((ProcedureInMemoryChore)procedure);
        // if the procedure is in a waiting state again, put it back in the queue
        procedure.updateTimestamp();
        if (procedure.isWaiting()) {
          delayed.setTimeoutTimestamp(procedure.getTimeoutTimestamp());
          queue.add(delayed);
        }
      } else {
        executeTimedoutProcedure(procedure);
      }
    }

    private void executeInMemoryChore(final ProcedureInMemoryChore chore) {
      if (!chore.isWaiting()) return;

      // The ProcedureInMemoryChore is a special case, and it acts as a chore.
      // instead of bringing the Chore class in, we reuse this timeout thread for
      // this special case.
      try {
        chore.periodicExecute(getEnvironment());
      } catch (Throwable e) {
        LOG.error("Ignoring " + chore + " exception: " + e.getMessage(), e);
      }
    }

    private void executeTimedoutProcedure(final Procedure proc) {
      // The procedure received a timeout. if the procedure itself does not handle it,
      // call abort() and add the procedure back in the queue for rollback.
      if (proc.setTimeoutFailure(getEnvironment())) {
        long rootProcId = Procedure.getRootProcedureId(procedures, proc);
        RootProcedureState procStack = rollbackStack.get(rootProcId);
        procStack.abort();
        store.update(proc);
        scheduler.addFront(proc);
      }
    }
  }

  private static final class DelayedProcedure
      extends DelayedUtil.DelayedContainerWithTimestamp<Procedure> {
    public DelayedProcedure(final Procedure procedure) {
      super(procedure, procedure.getTimeoutTimestamp());
    }
  }

  private static abstract class StoppableThread extends Thread {
    public StoppableThread(final ThreadGroup group, final String name) {
      super(group, name);
    }

    public abstract void sendStopSignal();

    public void awaitTermination() {
      try {
        final long startTime = EnvironmentEdgeManager.currentTime();
        for (int i = 0; isAlive(); ++i) {
          sendStopSignal();
          join(250);
          if (i > 0 && (i % 8) == 0) {
            LOG.warn("waiting termination of thread " + getName() + ", " +
              StringUtils.humanTimeDiff(EnvironmentEdgeManager.currentTime() - startTime));
          }
        }
      } catch (InterruptedException e) {
        LOG.warn(getName() + " join wait got interrupted", e);
      }
    }
  }

  // ==========================================================================
  //  Inline Chores (executors internal chores)
  // ==========================================================================
  private static abstract class InlineChore extends DelayedUtil.DelayedObject implements Runnable {
    private long timeout;

    public abstract int getTimeoutInterval();

    protected void refreshTimeout() {
      this.timeout = EnvironmentEdgeManager.currentTime() + getTimeoutInterval();
    }

    @Override
    public long getTimeoutTimestamp() {
      return timeout;
    }
  }

  // ----------------------------------------------------------------------------
  // TODO-MAYBE: Should we provide a InlineChore to notify the store with the
  // full set of procedures pending and completed to write a compacted
  // version of the log (in case is a log)?
  // In theory no, procedures are have a short life, so at some point the store
  // will have the tracker saying everything is in the last log.
  // ----------------------------------------------------------------------------

  private final class WorkerMonitor extends InlineChore {
    public static final String WORKER_MONITOR_INTERVAL_CONF_KEY =
        "hbase.procedure.worker.monitor.interval.msec";
    private static final int DEFAULT_WORKER_MONITOR_INTERVAL = 5000; // 5sec

    public static final String WORKER_STUCK_THRESHOLD_CONF_KEY =
        "hbase.procedure.worker.stuck.threshold.msec";
    private static final int DEFAULT_WORKER_STUCK_THRESHOLD = 10000; // 10sec

    public static final String WORKER_ADD_STUCK_PERCENTAGE_CONF_KEY =
        "hbase.procedure.worker.add.stuck.percentage";
    private static final float DEFAULT_WORKER_ADD_STUCK_PERCENTAGE = 0.5f; // 50% stuck

    private float addWorkerStuckPercentage = DEFAULT_WORKER_ADD_STUCK_PERCENTAGE;
    private int timeoutInterval = DEFAULT_WORKER_MONITOR_INTERVAL;
    private int stuckThreshold = DEFAULT_WORKER_STUCK_THRESHOLD;

    public WorkerMonitor() {
      refreshConfig();
    }

    @Override
    public void run() {
      final int stuckCount = checkForStuckWorkers();
      checkThreadCount(stuckCount);

      // refresh interval (poor man dynamic conf update)
      refreshConfig();
    }

    private int checkForStuckWorkers() {
      // check if any of the worker is stuck
      int stuckCount = 0;
      for (WorkerThread worker: workerThreads) {
        if (worker.getCurrentRunTime() < stuckThreshold) {
          continue;
        }

        // WARN the worker is stuck
        stuckCount++;
        LOG.warn("found worker stuck " + worker +
            " run time " + StringUtils.humanTimeDiff(worker.getCurrentRunTime()));
      }
      return stuckCount;
    }

    private void checkThreadCount(final int stuckCount) {
      // nothing to do if there are no runnable tasks
      if (stuckCount < 1 || !scheduler.hasRunnables()) return;

      // add a new thread if the worker stuck percentage exceed the threshold limit
      // and every handler is active.
      final float stuckPerc = ((float)stuckCount) / workerThreads.size();
      if (stuckPerc >= addWorkerStuckPercentage &&
          activeExecutorCount.get() == workerThreads.size()) {
        final WorkerThread worker = new WorkerThread(threadGroup);
        workerThreads.add(worker);
        worker.start();
        LOG.debug("added a new worker thread " + worker);
      }
    }

    private void refreshConfig() {
      addWorkerStuckPercentage = conf.getFloat(WORKER_ADD_STUCK_PERCENTAGE_CONF_KEY,
          DEFAULT_WORKER_ADD_STUCK_PERCENTAGE);
      timeoutInterval = conf.getInt(WORKER_MONITOR_INTERVAL_CONF_KEY,
        DEFAULT_WORKER_MONITOR_INTERVAL);
      stuckThreshold = conf.getInt(WORKER_STUCK_THRESHOLD_CONF_KEY,
        DEFAULT_WORKER_STUCK_THRESHOLD);
    }

    @Override
    public int getTimeoutInterval() {
      return timeoutInterval;
    }
  }
}
