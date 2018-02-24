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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.procedure2.Procedure.LockState;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

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
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureExecutor.class);

  public static final String CHECK_OWNER_SET_CONF_KEY = "hbase.procedure.check.owner.set";
  private static final boolean DEFAULT_CHECK_OWNER_SET = false;

  public static final String WORKER_KEEP_ALIVE_TIME_CONF_KEY =
      "hbase.procedure.worker.keep.alive.time.msec";
  private static final long DEFAULT_WORKER_KEEP_ALIVE_TIME = TimeUnit.MINUTES.toMillis(1);

  Testing testing = null;
  public static class Testing {
    protected boolean killIfSuspended = false;
    protected boolean killBeforeStoreUpdate = false;
    protected boolean toggleKillBeforeStoreUpdate = false;

    protected boolean shouldKillBeforeStoreUpdate() {
      final boolean kill = this.killBeforeStoreUpdate;
      if (this.toggleKillBeforeStoreUpdate) {
        this.killBeforeStoreUpdate = !kill;
        LOG.warn("Toggle KILL before store update to: " + this.killBeforeStoreUpdate);
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

  private static class CompletedProcedureRetainer {
    private final Procedure<?> procedure;
    private long clientAckTime;

    public CompletedProcedureRetainer(Procedure<?> procedure) {
      this.procedure = procedure;
      clientAckTime = -1;
    }

    public Procedure<?> getProcedure() {
      return procedure;
    }

    public boolean hasClientAckTime() {
      return clientAckTime != -1;
    }

    public long getClientAckTime() {
      return clientAckTime;
    }

    public void setClientAckTime(long clientAckTime) {
      this.clientAckTime = clientAckTime;
    }

    public boolean isExpired(long now, long evictTtl, long evictAckTtl) {
      return (hasClientAckTime() && (now - getClientAckTime()) >= evictAckTtl) ||
        (now - procedure.getLastUpdate()) >= evictTtl;
    }
  }

  /**
   * Internal cleaner that removes the completed procedure results after a TTL.
   * NOTE: This is a special case handled in timeoutLoop().
   *
   * <p>Since the client code looks more or less like:
   * <pre>
   *   procId = master.doOperation()
   *   while (master.getProcResult(procId) == ProcInProgress);
   * </pre>
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
    private static final Logger LOG = LoggerFactory.getLogger(CompletedProcedureCleaner.class);

    private static final String CLEANER_INTERVAL_CONF_KEY = "hbase.procedure.cleaner.interval";
    private static final int DEFAULT_CLEANER_INTERVAL = 30 * 1000; // 30sec

    private static final String EVICT_TTL_CONF_KEY = "hbase.procedure.cleaner.evict.ttl";
    private static final int DEFAULT_EVICT_TTL = 15 * 60000; // 15min

    private static final String EVICT_ACKED_TTL_CONF_KEY ="hbase.procedure.cleaner.acked.evict.ttl";
    private static final int DEFAULT_ACKED_EVICT_TTL = 5 * 60000; // 5min

    private static final String BATCH_SIZE_CONF_KEY = "hbase.procedure.cleaner.evict.batch.size";
    private static final int DEFAULT_BATCH_SIZE = 32;

    private final Map<Long, CompletedProcedureRetainer> completed;
    private final Map<NonceKey, Long> nonceKeysToProcIdsMap;
    private final ProcedureStore store;
    private Configuration conf;

    public CompletedProcedureCleaner(final Configuration conf, final ProcedureStore store,
        final Map<Long, CompletedProcedureRetainer> completedMap,
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
      final Iterator<Map.Entry<Long, CompletedProcedureRetainer>> it = completed.entrySet().iterator();
      while (it.hasNext() && store.isRunning()) {
        final Map.Entry<Long, CompletedProcedureRetainer> entry = it.next();
        final CompletedProcedureRetainer retainer = entry.getValue();
        final Procedure<?> proc = retainer.getProcedure();

        // TODO: Select TTL based on Procedure type
        if (retainer.isExpired(now, evictTtl, evictAckTtl)) {
          // Failed procedures aren't persisted in WAL.
          if (!(proc instanceof FailedProcedure)) {
            batchIds[batchCount++] = entry.getKey();
            if (batchCount == batchIds.length) {
              store.delete(batchIds, 0, batchCount);
              batchCount = 0;
            }
          }
          final NonceKey nonceKey = proc.getNonceKey();
          if (nonceKey != null) {
            nonceKeysToProcIdsMap.remove(nonceKey);
          }
          it.remove();
          LOG.trace("Evict completed {}", proc);
        }
      }
      if (batchCount > 0) {
        store.delete(batchIds, 0, batchCount);
      }
    }
  }

  /**
   * Map the the procId returned by submitProcedure(), the Root-ProcID, to the Procedure.
   * Once a Root-Procedure completes (success or failure), the result will be added to this map.
   * The user of ProcedureExecutor should call getResult(procId) to get the result.
   */
  private final ConcurrentHashMap<Long, CompletedProcedureRetainer> completed = new ConcurrentHashMap<>();

  /**
   * Map the the procId returned by submitProcedure(), the Root-ProcID, to the RootProcedureState.
   * The RootProcedureState contains the execution stack of the Root-Procedure,
   * It is added to the map by submitProcedure() and removed on procedure completion.
   */
  private final ConcurrentHashMap<Long, RootProcedureState> rollbackStack = new ConcurrentHashMap<>();

  /**
   * Helper map to lookup the live procedures by ID.
   * This map contains every procedure. root-procedures and subprocedures.
   */
  private final ConcurrentHashMap<Long, Procedure> procedures = new ConcurrentHashMap<>();

  /**
   * Helper map to lookup whether the procedure already issued from the same client.
   * This map contains every root procedure.
   */
  private final ConcurrentHashMap<NonceKey, Long> nonceKeysToProcIdsMap = new ConcurrentHashMap<>();

  private final CopyOnWriteArrayList<ProcedureExecutorListener> listeners = new CopyOnWriteArrayList<>();

  private Configuration conf;
  private ThreadGroup threadGroup;
  private CopyOnWriteArrayList<WorkerThread> workerThreads;
  private TimeoutExecutorThread timeoutExecutor;
  private int corePoolSize;
  private int maxPoolSize;

  private volatile long keepAliveTime;

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
          Procedure<?> proc = procIter.next();
          LOG.error("Corrupt " + proc);
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
    final boolean debugEnabled = LOG.isDebugEnabled();

    // 1. Build the rollback stack
    int runnablesCount = 0;
    while (procIter.hasNext()) {
      boolean finished = procIter.isNextFinished();
      Procedure proc = procIter.next();
      NonceKey nonceKey = proc.getNonceKey();
      long procId = proc.getProcId();

      if (finished) {
        completed.put(proc.getProcId(), new CompletedProcedureRetainer(proc));
        if (debugEnabled) {
          LOG.debug("Completed " + proc);
        }
      } else {
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
      if (procIter.isNextFinished()) {
        procIter.skipNext();
        continue;
      }

      Procedure proc = procIter.next();
      assert !(proc.isFinished() && !proc.hasParent()) : "unexpected completed proc=" + proc;

      if (debugEnabled) {
        LOG.debug(String.format("Loading %s", proc));
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
            waitingSet = new HashSet<>();
          }
          waitingSet.add(proc);
          break;
        case FAILED:
          // add the proc to the scheduler to perform the rollback
          scheduler.addBack(proc);
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
        LOG.error("Corrupted " + proc);
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
    if (!running.compareAndSet(false, true)) {
      LOG.warn("Already running");
      return;
    }

    // We have numThreads executor + one timer thread used for timing out
    // procedures and triggering periodic procedures.
    this.corePoolSize = numThreads;
    this.maxPoolSize = 10 * numThreads;
    LOG.info("Starting {} core workers (bigger of cpus/4 or 16) with max (burst) worker count={}",
        corePoolSize, maxPoolSize);

    // Create the Thread Group for the executors
    threadGroup = new ThreadGroup("PEWorkerGroup");

    // Create the timeout executor
    timeoutExecutor = new TimeoutExecutorThread(this, threadGroup);

    // Create the workers
    workerId.set(0);
    workerThreads = new CopyOnWriteArrayList<>();
    for (int i = 0; i < corePoolSize; ++i) {
      workerThreads.add(new WorkerThread(threadGroup));
    }

    long st, et;

    // Acquire the store lease.
    st = EnvironmentEdgeManager.currentTime();
    store.recoverLease();
    et = EnvironmentEdgeManager.currentTime();
    LOG.info("Recovered {} lease in {}", store.getClass().getSimpleName(),
      StringUtils.humanTimeDiff(et - st));

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
    LOG.info("Loaded {} in {}", store.getClass().getSimpleName(),
      StringUtils.humanTimeDiff(et - st));

    // Start the executors. Here we must have the lastProcId set.
    LOG.trace("Start workers {}", workerThreads.size());
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

    LOG.info("Stopping");
    scheduler.stop();
    timeoutExecutor.sendStopSignal();
  }

  @VisibleForTesting
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
      LOG.error("ThreadGroup " + threadGroup + " contains running threads; " + e.getMessage());
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

  ProcedureScheduler getScheduler() {
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
    chore.setState(ProcedureState.SUCCESS);
    return timeoutExecutor.remove(chore);
  }

  // ==========================================================================
  //  Nonce Procedure helpers
  // ==========================================================================
  /**
   * Create a NoneKey from the specified nonceGroup and nonce.
   * @param nonceGroup
   * @param nonce
   * @return the generated NonceKey
   */
  public NonceKey createNonceKey(final long nonceGroup, final long nonce) {
    return (nonce == HConstants.NO_NONCE) ? null : new NonceKey(nonceGroup, nonce);
  }

  /**
   * Register a nonce for a procedure that is going to be submitted.
   * A procId will be reserved and on submitProcedure(),
   * the procedure with the specified nonce will take the reserved ProcId.
   * If someone already reserved the nonce, this method will return the procId reserved,
   * otherwise an invalid procId will be returned. and the caller should procede
   * and submit the procedure.
   *
   * @param nonceKey A unique identifier for this operation from the client or process.
   * @return the procId associated with the nonce, if any otherwise an invalid procId.
   */
  public long registerNonce(final NonceKey nonceKey) {
    if (nonceKey == null) return -1;

    // check if we have already a Reserved ID for the nonce
    Long oldProcId = nonceKeysToProcIdsMap.get(nonceKey);
    if (oldProcId == null) {
      // reserve a new Procedure ID, this will be associated with the nonce
      // and the procedure submitted with the specified nonce will use this ID.
      final long newProcId = nextProcId();
      oldProcId = nonceKeysToProcIdsMap.putIfAbsent(nonceKey, newProcId);
      if (oldProcId == null) return -1;
    }

    // we found a registered nonce, but the procedure may not have been submitted yet.
    // since the client expect the procedure to be submitted, spin here until it is.
    final boolean traceEnabled = LOG.isTraceEnabled();
    while (isRunning() &&
           !(procedures.containsKey(oldProcId) || completed.containsKey(oldProcId)) &&
           nonceKeysToProcIdsMap.containsKey(nonceKey)) {
      if (traceEnabled) {
        LOG.trace("Waiting for pid=" + oldProcId.longValue() + " to be submitted");
      }
      Threads.sleep(100);
    }
    return oldProcId.longValue();
  }

  /**
   * Remove the NonceKey if the procedure was not submitted to the executor.
   * @param nonceKey A unique identifier for this operation from the client or process.
   */
  public void unregisterNonceIfProcedureWasNotSubmitted(final NonceKey nonceKey) {
    if (nonceKey == null) return;

    final Long procId = nonceKeysToProcIdsMap.get(nonceKey);
    if (procId == null) return;

    // if the procedure was not submitted, remove the nonce
    if (!(procedures.containsKey(procId) || completed.containsKey(procId))) {
      nonceKeysToProcIdsMap.remove(nonceKey);
    }
  }

  public static class FailedProcedure<TEnvironment> extends Procedure<TEnvironment> {
    private String procName;

    public FailedProcedure() {
    }

    public FailedProcedure(long procId, String procName, User owner,
        NonceKey nonceKey, IOException exception) {
      this.procName = procName;
      setProcId(procId);
      setState(ProcedureState.ROLLEDBACK);
      setOwner(owner);
      setNonceKey(nonceKey);
      long currentTime = EnvironmentEdgeManager.currentTime();
      setSubmittedTime(currentTime);
      setLastUpdate(currentTime);
      setFailure(Objects.toString(exception.getMessage(), ""), exception);
    }

    @Override
    public String getProcName() {
      return procName;
    }

    @Override
    protected Procedure<TEnvironment>[] execute(TEnvironment env)
        throws ProcedureYieldException, ProcedureSuspendedException,
        InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void rollback(TEnvironment env)
        throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(TEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
    }
  }

  /**
   * If the failure failed before submitting it, we may want to give back the
   * same error to the requests with the same nonceKey.
   *
   * @param nonceKey A unique identifier for this operation from the client or process
   * @param procName name of the procedure, used to inform the user
   * @param procOwner name of the owner of the procedure, used to inform the user
   * @param exception the failure to report to the user
   */
  public void setFailureResultForNonce(final NonceKey nonceKey, final String procName,
      final User procOwner, final IOException exception) {
    if (nonceKey == null) return;

    final Long procId = nonceKeysToProcIdsMap.get(nonceKey);
    if (procId == null || completed.containsKey(procId)) return;

    Procedure<?> proc = new FailedProcedure(procId.longValue(),
        procName, procOwner, nonceKey, exception);

    completed.putIfAbsent(procId, new CompletedProcedureRetainer(proc));
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
    return submitProcedure(proc, null);
  }

  /**
   * Add a new root-procedure to the executor.
   * @param proc the new procedure to execute.
   * @param nonceKey the registered unique identifier for this operation from the client or process.
   * @return the procedure id, that can be used to monitor the operation
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification = "FindBugs is blind to the check-for-null")
  public long submitProcedure(final Procedure proc, final NonceKey nonceKey) {
    Preconditions.checkArgument(lastProcId.get() >= 0);
    Preconditions.checkArgument(isRunning(), "executor not running");

    prepareProcedure(proc);

    final Long currentProcId;
    if (nonceKey != null) {
      currentProcId = nonceKeysToProcIdsMap.get(nonceKey);
      Preconditions.checkArgument(currentProcId != null,
        "Expected nonceKey=" + nonceKey + " to be reserved, use registerNonce(); proc=" + proc);
    } else {
      currentProcId = nextProcId();
    }

    // Initialize the procedure
    proc.setNonceKey(nonceKey);
    proc.setProcId(currentProcId.longValue());

    // Commit the transaction
    store.insert(proc, null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stored " + proc);
    }

    // Add the procedure to the executor
    return pushProcedure(proc);
  }

  /**
   * Add a set of new root-procedure to the executor.
   * @param procs the new procedures to execute.
   */
  // TODO: Do we need to take nonces here?
  public void submitProcedures(final Procedure[] procs) {
    Preconditions.checkArgument(lastProcId.get() >= 0);
    Preconditions.checkArgument(isRunning(), "executor not running");
    if (procs == null || procs.length <= 0) {
      return;
    }

    // Prepare procedure
    for (int i = 0; i < procs.length; ++i) {
      prepareProcedure(procs[i]).setProcId(nextProcId());
    }

    // Commit the transaction
    store.insert(procs);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stored " + Arrays.toString(procs));
    }

    // Add the procedure to the executor
    for (int i = 0; i < procs.length; ++i) {
      pushProcedure(procs[i]);
    }
  }

  private Procedure prepareProcedure(final Procedure proc) {
    Preconditions.checkArgument(proc.getState() == ProcedureState.INITIALIZING);
    Preconditions.checkArgument(isRunning(), "executor not running");
    Preconditions.checkArgument(!proc.hasParent(), "unexpected parent", proc);
    if (this.checkOwnerSet) {
      Preconditions.checkArgument(proc.hasOwner(), "missing owner");
    }
    return proc;
  }

  private long pushProcedure(final Procedure proc) {
    final long currentProcId = proc.getProcId();

    // Update metrics on start of a procedure
    proc.updateMetricsOnSubmit(getEnvironment());

    // Create the rollback stack for the procedure
    RootProcedureState stack = new RootProcedureState();
    rollbackStack.put(currentProcId, stack);

    // Submit the new subprocedures
    assert !procedures.containsKey(currentProcId);
    procedures.put(currentProcId, proc);
    sendProcedureAddedNotification(currentProcId);
    scheduler.addBack(proc);
    return proc.getProcId();
  }

  /**
   * Send an abort notification the specified procedure.
   * Depending on the procedure implementation the abort can be considered or ignored.
   * @param procId the procedure to abort
   * @return true if the procedure exists and has received the abort, otherwise false.
   */
  public boolean abort(final long procId) {
    return abort(procId, true);
  }

  /**
   * Send an abort notification to the specified procedure.
   * Depending on the procedure implementation, the abort can be considered or ignored.
   * @param procId the procedure to abort
   * @param mayInterruptIfRunning if the proc completed at least one step, should it be aborted?
   * @return true if the procedure exists and has received the abort, otherwise false.
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

  public <T extends Procedure> T getProcedure(final Class<T> clazz, final long procId) {
    final Procedure proc = getProcedure(procId);
    if (clazz.isInstance(proc)) {
      return (T)proc;
    }
    return null;
  }

  public Procedure getResult(final long procId) {
    CompletedProcedureRetainer retainer = completed.get(procId);
    if (retainer == null) {
      return null;
    } else {
      return retainer.getProcedure();
    }
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
    CompletedProcedureRetainer retainer = completed.get(procId);
    if (retainer == null) {
      assert !procedures.containsKey(procId) : "pid=" + procId + " is still running";
      if (LOG.isDebugEnabled()) {
        LOG.debug("pid=" + procId + " already removed by the cleaner.");
      }
      return;
    }

    // The CompletedProcedureCleaner will take care of deletion, once the TTL is expired.
    retainer.setClientAckTime(EnvironmentEdgeManager.currentTime());
  }

  public Procedure getResultOrProcedure(final long procId) {
    CompletedProcedureRetainer retainer = completed.get(procId);
    if (retainer == null) {
      return procedures.get(procId);
    } else {
      return retainer.getProcedure();
    }
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

    final Procedure runningProc = procedures.get(procId);
    if (runningProc != null) {
      return runningProc.getOwner().equals(user.getShortName());
    }

    final CompletedProcedureRetainer retainer = completed.get(procId);
    if (retainer != null) {
      return retainer.getProcedure().getOwner().equals(user.getShortName());
    }

    // Procedure either does not exist or has already completed and got cleaned up.
    // At this time, we cannot check the owner of the procedure
    return false;
  }

  /**
   * Get procedures.
   * @return the procedures in a list
   */
  public List<Procedure<?>> getProcedures() {
    final List<Procedure<?>> procedureLists = new ArrayList<>(procedures.size() + completed.size());
    for (Procedure<?> procedure : procedures.values()) {
      procedureLists.add(procedure);
    }
    // Note: The procedure could show up twice in the list with different state, as
    // it could complete after we walk through procedures list and insert into
    // procedureList - it is ok, as we will use the information in the Procedure
    // to figure it out; to prevent this would increase the complexity of the logic.
    for (CompletedProcedureRetainer retainer: completed.values()) {
      procedureLists.add(retainer.getProcedure());
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
          LOG.error("Listener " + listener + " had an error: " + e.getMessage(), e);
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
          LOG.error("Listener " + listener + " had an error: " + e.getMessage(), e);
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
          LOG.error("Listener " + listener + " had an error: " + e.getMessage(), e);
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

  @VisibleForTesting
  public Set<Long> getActiveProcIds() {
    return procedures.keySet();
  }

  Long getRootProcedureId(Procedure proc) {
    return Procedure.getRootProcedureId(procedures, proc);
  }

  // ==========================================================================
  //  Executions
  // ==========================================================================
  private void executeProcedure(final Procedure proc) {
    final Long rootProcId = getRootProcedureId(proc);
    if (rootProcId == null) {
      // The 'proc' was ready to run but the root procedure was rolledback
      LOG.warn("Rollback because parent is done/rolledback proc=" + proc);
      executeRollback(proc);
      return;
    }

    final RootProcedureState procStack = rollbackStack.get(rootProcId);
    if (procStack == null) {
      LOG.warn("RootProcedureState is null for " + proc.getProcId());
      return;
    }
    do {
      // Try to acquire the execution
      if (!procStack.acquire(proc)) {
        if (procStack.setRollback()) {
          // we have the 'rollback-lock' we can start rollingback
          switch (executeRollback(rootProcId, procStack)) {
            case LOCK_ACQUIRED:
                break;
            case LOCK_YIELD_WAIT:
              procStack.unsetRollback();
              scheduler.yield(proc);
              break;
            case LOCK_EVENT_WAIT:
              LOG.info("LOCK_EVENT_WAIT rollback..." + proc);
              procStack.unsetRollback();
              break;
            default:
              throw new UnsupportedOperationException();
          }
        } else {
          // if we can't rollback means that some child is still running.
          // the rollback will be executed after all the children are done.
          // If the procedure was never executed, remove and mark it as rolledback.
          if (!proc.wasExecuted()) {
            switch (executeRollback(proc)) {
              case LOCK_ACQUIRED:
                break;
              case LOCK_YIELD_WAIT:
                scheduler.yield(proc);
                break;
              case LOCK_EVENT_WAIT:
                LOG.info("LOCK_EVENT_WAIT can't rollback child running?..." + proc);
                break;
              default:
                throw new UnsupportedOperationException();
            }
          }
        }
        break;
      }

      // Execute the procedure
      assert proc.getState() == ProcedureState.RUNNABLE : proc;
      // Note that lock is NOT about concurrency but rather about ensuring
      // ownership of a procedure of an entity such as a region or table
      LockState lockState = acquireLock(proc);
      switch (lockState) {
        case LOCK_ACQUIRED:
          execProcedure(procStack, proc);
          releaseLock(proc, false);
          break;
        case LOCK_YIELD_WAIT:
          LOG.info(lockState + " " + proc);
          scheduler.yield(proc);
          break;
        case LOCK_EVENT_WAIT:
          // Someone will wake us up when the lock is available
          LOG.debug(lockState + " " + proc);
          break;
        default:
          throw new UnsupportedOperationException();
      }
      procStack.release(proc);

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && !isRunning()) {
        break;
      }

      if (proc.isSuccess()) {
        // update metrics on finishing the procedure
        proc.updateMetricsOnFinish(getEnvironment(), proc.elapsedTime(), true);
        LOG.info("Finished " + proc + " in " + StringUtils.humanTimeDiff(proc.elapsedTime()));
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

  private LockState acquireLock(final Procedure proc) {
    final TEnvironment env = getEnvironment();
    // hasLock() is used in conjunction with holdLock().
    // This allows us to not rewrite or carry around the hasLock() flag
    // for every procedure. the hasLock() have meaning only if holdLock() is true.
    if (proc.holdLock(env) && proc.hasLock(env)) {
      return LockState.LOCK_ACQUIRED;
    }
    return proc.doAcquireLock(env);
  }

  private void releaseLock(final Procedure proc, final boolean force) {
    final TEnvironment env = getEnvironment();
    // For how the framework works, we know that we will always have the lock
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
  private LockState executeRollback(final long rootProcId, final RootProcedureState procStack) {
    final Procedure rootProc = procedures.get(rootProcId);
    RemoteProcedureException exception = rootProc.getException();
    // TODO: This needs doc. The root proc doesn't have an exception. Maybe we are
    // rolling back because the subprocedure does. Clarify.
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

      LockState lockState;
      if (!reuseLock && (lockState = acquireLock(proc)) != LockState.LOCK_ACQUIRED) {
        // can't take a lock on the procedure, add the root-proc back on the
        // queue waiting for the lock availability
        return lockState;
      }

      lockState = executeRollback(proc);
      boolean abortRollback = lockState != LockState.LOCK_ACQUIRED;
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
        return lockState;
      }

      subprocStack.remove(stackTail);

      // if the procedure is kind enough to pass the slot to someone else, yield
      if (proc.isYieldAfterExecutionStep(getEnvironment())) {
        return LockState.LOCK_YIELD_WAIT;
      }

      if (proc != rootProc) {
        execCompletionCleanup(proc);
      }
    }

    // Finalize the procedure state
    LOG.info("Rolled back " + rootProc +
             " exec-time=" + StringUtils.humanTimeDiff(rootProc.elapsedTime()));
    procedureFinished(rootProc);
    return LockState.LOCK_ACQUIRED;
  }

  /**
   * Execute the rollback of the procedure step.
   * It updates the store with the new state (stack index)
   * or will remove completly the procedure in case it is a child.
   */
  private LockState executeRollback(final Procedure proc) {
    try {
      proc.doRollback(getEnvironment());
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Roll back attempt failed for " + proc, e);
      }
      return LockState.LOCK_YIELD_WAIT;
    } catch (InterruptedException e) {
      handleInterruptedException(proc, e);
      return LockState.LOCK_YIELD_WAIT;
    } catch (Throwable e) {
      // Catch NullPointerExceptions or similar errors...
      LOG.error(HBaseMarkers.FATAL, "CODE-BUG: Uncaught runtime exception for " + proc, e);
    }

    // allows to kill the executor before something is stored to the wal.
    // useful to test the procedure recovery.
    if (testing != null && testing.shouldKillBeforeStoreUpdate()) {
      LOG.debug("TESTING: Kill before store update");
      stop();
      return LockState.LOCK_YIELD_WAIT;
    }

    if (proc.removeStackIndex()) {
      proc.setState(ProcedureState.ROLLEDBACK);

      // update metrics on finishing the procedure (fail)
      proc.updateMetricsOnFinish(getEnvironment(), proc.elapsedTime(), false);

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

    return LockState.LOCK_ACQUIRED;
  }

  /**
   * Executes <code>procedure</code>
   * <ul>
   *  <li>Calls the doExecute() of the procedure
   *  <li>If the procedure execution didn't fail (i.e. valid user input)
   *  <ul>
   *    <li>...and returned subprocedures
   *    <ul><li>The subprocedures are initialized.
   *      <li>The subprocedures are added to the store
   *      <li>The subprocedures are added to the runnable queue
   *      <li>The procedure is now in a WAITING state, waiting for the subprocedures to complete
   *    </ul>
   *    </li>
   *   <li>...if there are no subprocedure
   *    <ul><li>the procedure completed successfully
   *      <li>if there is a parent (WAITING)
   *      <li>the parent state will be set to RUNNABLE
   *    </ul>
   *   </li>
   *  </ul>
   *  </li>
   *  <li>In case of failure
   *  <ul>
   *    <li>The store is updated with the new state</li>
   *    <li>The executor (caller of this method) will start the rollback of the procedure</li>
   *  </ul>
   *  </li>
   *  </ul>
   */
  private void execProcedure(final RootProcedureState procStack,
      final Procedure<TEnvironment> procedure) {
    Preconditions.checkArgument(procedure.getState() == ProcedureState.RUNNABLE);

    // Procedures can suspend themselves. They skip out by throwing a ProcedureSuspendedException.
    // The exception is caught below and then we hurry to the exit without disturbing state. The
    // idea is that the processing of this procedure will be unsuspended later by an external event
    // such the report of a region open. TODO: Currently, its possible for two worker threads
    // to be working on the same procedure concurrently (locking in procedures is NOT about
    // concurrency but about tying an entity to a procedure; i.e. a region to a particular
    // procedure instance). This can make for issues if both threads are changing state.
    // See env.getProcedureScheduler().wakeEvent(regionNode.getProcedureEvent());
    // in RegionTransitionProcedure#reportTransition for example of Procedure putting
    // itself back on the scheduler making it possible for two threads running against
    // the one Procedure. Might be ok if they are both doing different, idempotent sections.
    boolean suspended = false;

    // Whether to 're-' -execute; run through the loop again.
    boolean reExecute = false;

    Procedure<TEnvironment>[] subprocs = null;
    do {
      reExecute = false;
      try {
        subprocs = procedure.doExecute(getEnvironment());
        if (subprocs != null && subprocs.length == 0) {
          subprocs = null;
        }
      } catch (ProcedureSuspendedException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Suspend " + procedure);
        }
        suspended = true;
      } catch (ProcedureYieldException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Yield " + procedure + ": " + e.getMessage(), e);
        }
        scheduler.yield(procedure);
        return;
      } catch (InterruptedException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Yield interrupt " + procedure + ": " + e.getMessage(), e);
        }
        handleInterruptedException(procedure, e);
        scheduler.yield(procedure);
        return;
      } catch (Throwable e) {
        // Catch NullPointerExceptions or similar errors...
        String msg = "CODE-BUG: Uncaught runtime exception: " + procedure;
        LOG.error(msg, e);
        procedure.setFailure(new RemoteProcedureException(msg, e));
      }

      if (!procedure.isFailed()) {
        if (subprocs != null) {
          if (subprocs.length == 1 && subprocs[0] == procedure) {
            // Procedure returned itself. Quick-shortcut for a state machine-like procedure;
            // i.e. we go around this loop again rather than go back out on the scheduler queue.
            subprocs = null;
            reExecute = true;
            if (LOG.isTraceEnabled()) {
              LOG.trace("Short-circuit to next step on pid=" + procedure.getProcId());
            }
          } else {
            // Yield the current procedure, and make the subprocedure runnable
            // subprocs may come back 'null'.
            subprocs = initializeChildren(procStack, procedure, subprocs);
            LOG.info("Initialized subprocedures=" +
              (subprocs == null? null:
                Stream.of(subprocs).map(e -> "{" + e.toString() + "}").
                collect(Collectors.toList()).toString()));
          }
        } else if (procedure.getState() == ProcedureState.WAITING_TIMEOUT) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Added to timeoutExecutor " + procedure);
          }
          timeoutExecutor.add(procedure);
        } else if (!suspended) {
          // No subtask, so we are done
          procedure.setState(ProcedureState.SUCCESS);
        }
      }

      // Add the procedure to the stack
      procStack.addRollbackStep(procedure);

      // allows to kill the executor before something is stored to the wal.
      // useful to test the procedure recovery.
      if (testing != null && testing.shouldKillBeforeStoreUpdate(suspended)) {
        LOG.debug("TESTING: Kill before store update: " + procedure);
        stop();
        return;
      }

      // TODO: The code here doesn't check if store is running before persisting to the store as
      // it relies on the method call below to throw RuntimeException to wind up the stack and
      // executor thread to stop. The statement following the method call below seems to check if
      // store is not running, to prevent scheduling children procedures, re-execution or yield
      // of this procedure. This may need more scrutiny and subsequent cleanup in future
      //
      // Commit the transaction even if a suspend (state may have changed). Note this append
      // can take a bunch of time to complete.
      updateStoreOnExec(procStack, procedure, subprocs);

      // if the store is not running we are aborting
      if (!store.isRunning()) return;
      // if the procedure is kind enough to pass the slot to someone else, yield
      if (procedure.isRunnable() && !suspended &&
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

    // if the procedure is complete and has a parent, count down the children latch.
    // If 'suspended', do nothing to change state -- let other threads handle unsuspend event.
    if (!suspended && procedure.isFinished() && procedure.hasParent()) {
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
      subproc.updateMetricsOnSubmit(getEnvironment());
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
    if (parent.tryRunnable()) {
      // If we succeeded in making the parent runnable -- i.e. all of its
      // children have completed, move parent to front of the queue.
      store.update(parent);
      scheduler.addFront(parent);
      LOG.info("Finished subprocedure(s) of " + parent + "; resume parent processing.");
      return;
    }
  }

  private void updateStoreOnExec(final RootProcedureState procStack,
      final Procedure procedure, final Procedure[] subprocs) {
    if (subprocs != null && !procedure.isFailed()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Stored " + procedure + ", children " + Arrays.toString(subprocs));
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
      LOG.trace("Interrupt during " + proc + ". suspend and retry it later.", e);
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

    CompletedProcedureRetainer retainer = new CompletedProcedureRetainer(proc);

    // update the executor internal state maps
    if (!proc.shouldWaitClientAck(getEnvironment())) {
      retainer.setClientAckTime(0);
    }

    completed.put(proc.getProcId(), retainer);
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

  RootProcedureState getProcStack(long rootProcId) {
    return rollbackStack.get(rootProcId);
  }

  // ==========================================================================
  //  Worker Thread
  // ==========================================================================
  private class WorkerThread extends StoppableThread {
    private final AtomicLong executionStartTime = new AtomicLong(Long.MAX_VALUE);
    private volatile Procedure<?> activeProcedure;

    public WorkerThread(ThreadGroup group) {
      this(group, "PEWorker-");
    }

    protected WorkerThread(ThreadGroup group, String prefix) {
      super(group, prefix + workerId.incrementAndGet());
      setDaemon(true);
    }

    @Override
    public void sendStopSignal() {
      scheduler.signalAll();
    }

    @Override
    public void run() {
      long lastUpdate = EnvironmentEdgeManager.currentTime();
      try {
        while (isRunning() && keepAlive(lastUpdate)) {
          Procedure<?> proc = scheduler.poll(keepAliveTime, TimeUnit.MILLISECONDS);
          if (proc == null) {
            continue;
          }
          this.activeProcedure = proc;
          int activeCount = activeExecutorCount.incrementAndGet();
          int runningCount = store.setRunningProcedureCount(activeCount);
          LOG.trace("Execute pid={} runningCount={}, activeCount={}", proc.getProcId(),
            runningCount, activeCount);
          executionStartTime.set(EnvironmentEdgeManager.currentTime());
          try {
            executeProcedure(proc);
          } catch (AssertionError e) {
            LOG.info("ASSERT pid=" + proc.getProcId(), e);
            throw e;
          } finally {
            activeCount = activeExecutorCount.decrementAndGet();
            runningCount = store.setRunningProcedureCount(activeCount);
            LOG.trace("Halt pid={} runningCount={}, activeCount={}", proc.getProcId(),
              runningCount, activeCount);
            this.activeProcedure = null;
            lastUpdate = EnvironmentEdgeManager.currentTime();
            executionStartTime.set(Long.MAX_VALUE);
          }
        }
      } catch (Throwable t) {
        LOG.warn("Worker terminating UNNATURALLY {}", this.activeProcedure, t);
      } finally {
        LOG.trace("Worker terminated.");
      }
      workerThreads.remove(this);
    }

    @Override
    public String toString() {
      Procedure<?> p = this.activeProcedure;
      return getName() + "(pid=" + (p == null? Procedure.NO_PROC_ID: p.getProcId() + ")");
    }

    /**
     * @return the time since the current procedure is running
     */
    public long getCurrentRunTime() {
      return EnvironmentEdgeManager.currentTime() - executionStartTime.get();
    }

    // core worker never timeout
    protected boolean keepAlive(long lastUpdate) {
      return true;
    }
  }

  // A worker thread which can be added when core workers are stuck. Will timeout after
  // keepAliveTime if there is no procedure to run.
  private final class KeepAliveWorkerThread extends WorkerThread {

    public KeepAliveWorkerThread(ThreadGroup group) {
      super(group, "KeepAlivePEWorker-");
    }

    @Override
    protected boolean keepAlive(long lastUpdate) {
      return EnvironmentEdgeManager.currentTime() - lastUpdate < keepAliveTime;
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
      for (WorkerThread worker : workerThreads) {
        if (worker.getCurrentRunTime() < stuckThreshold) {
          continue;
        }

        // WARN the worker is stuck
        stuckCount++;
        LOG.warn("Worker stuck {} run time {}", worker,
          StringUtils.humanTimeDiff(worker.getCurrentRunTime()));
      }
      return stuckCount;
    }

    private void checkThreadCount(final int stuckCount) {
      // nothing to do if there are no runnable tasks
      if (stuckCount < 1 || !scheduler.hasRunnables()) {
        return;
      }

      // add a new thread if the worker stuck percentage exceed the threshold limit
      // and every handler is active.
      final float stuckPerc = ((float) stuckCount) / workerThreads.size();
      // let's add new worker thread more aggressively, as they will timeout finally if there is no
      // work to do.
      if (stuckPerc >= addWorkerStuckPercentage && workerThreads.size() < maxPoolSize) {
        final KeepAliveWorkerThread worker = new KeepAliveWorkerThread(threadGroup);
        workerThreads.add(worker);
        worker.start();
        LOG.debug("Added new worker thread {}", worker);
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
