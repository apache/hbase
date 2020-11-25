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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureStoreListener;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

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
public class ProcedureExecutor<TEnvironment> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureExecutor.class);

  public static final String CHECK_OWNER_SET_CONF_KEY = "hbase.procedure.check.owner.set";
  private static final boolean DEFAULT_CHECK_OWNER_SET = false;

  public static final String WORKER_KEEP_ALIVE_TIME_CONF_KEY =
      "hbase.procedure.worker.keep.alive.time.msec";
  private static final long DEFAULT_WORKER_KEEP_ALIVE_TIME = TimeUnit.MINUTES.toMillis(1);

  public static final String EVICT_TTL_CONF_KEY = "hbase.procedure.cleaner.evict.ttl";
  static final int DEFAULT_EVICT_TTL = 15 * 60000; // 15min

  public static final String EVICT_ACKED_TTL_CONF_KEY ="hbase.procedure.cleaner.acked.evict.ttl";
  static final int DEFAULT_ACKED_EVICT_TTL = 5 * 60000; // 5min

  /**
   * {@link #testing} is non-null when ProcedureExecutor is being tested. Tests will try to
   * break PE having it fail at various junctures. When non-null, testing is set to an instance of
   * the below internal {@link Testing} class with flags set for the particular test.
   */
  volatile Testing testing = null;

  /**
   * Class with parameters describing how to fail/die when in testing-context.
   */
  public static class Testing {
    protected volatile boolean killIfHasParent = true;
    protected volatile boolean killIfSuspended = false;

    /**
     * Kill the PE BEFORE we store state to the WAL. Good for figuring out if a Procedure is
     * persisting all the state it needs to recover after a crash.
     */
    protected volatile boolean killBeforeStoreUpdate = false;
    protected volatile boolean toggleKillBeforeStoreUpdate = false;

    /**
     * Set when we want to fail AFTER state has been stored into the WAL. Rarely used. HBASE-20978
     * is about a case where memory-state was being set after store to WAL where a crash could
     * cause us to get stuck. This flag allows killing at what was a vulnerable time.
     */
    protected volatile boolean killAfterStoreUpdate = false;
    protected volatile boolean toggleKillAfterStoreUpdate = false;

    protected boolean shouldKillBeforeStoreUpdate() {
      final boolean kill = this.killBeforeStoreUpdate;
      if (this.toggleKillBeforeStoreUpdate) {
        this.killBeforeStoreUpdate = !kill;
        LOG.warn("Toggle KILL before store update to: " + this.killBeforeStoreUpdate);
      }
      return kill;
    }

    protected boolean shouldKillBeforeStoreUpdate(boolean isSuspended, boolean hasParent) {
      if (isSuspended && !killIfSuspended) {
        return false;
      }
      if (hasParent && !killIfHasParent) {
        return false;
      }
      return shouldKillBeforeStoreUpdate();
    }

    protected boolean shouldKillAfterStoreUpdate() {
      final boolean kill = this.killAfterStoreUpdate;
      if (this.toggleKillAfterStoreUpdate) {
        this.killAfterStoreUpdate = !kill;
        LOG.warn("Toggle KILL after store update to: " + this.killAfterStoreUpdate);
      }
      return kill;
    }

    protected boolean shouldKillAfterStoreUpdate(final boolean isSuspended) {
      return (isSuspended && !killIfSuspended) ? false : shouldKillAfterStoreUpdate();
    }
  }

  public interface ProcedureExecutorListener {
    void procedureLoaded(long procId);
    void procedureAdded(long procId);
    void procedureFinished(long procId);
  }

  /**
   * Map the the procId returned by submitProcedure(), the Root-ProcID, to the Procedure.
   * Once a Root-Procedure completes (success or failure), the result will be added to this map.
   * The user of ProcedureExecutor should call getResult(procId) to get the result.
   */
  private final ConcurrentHashMap<Long, CompletedProcedureRetainer<TEnvironment>> completed =
    new ConcurrentHashMap<>();

  /**
   * Map the the procId returned by submitProcedure(), the Root-ProcID, to the RootProcedureState.
   * The RootProcedureState contains the execution stack of the Root-Procedure,
   * It is added to the map by submitProcedure() and removed on procedure completion.
   */
  private final ConcurrentHashMap<Long, RootProcedureState<TEnvironment>> rollbackStack =
    new ConcurrentHashMap<>();

  /**
   * Helper map to lookup the live procedures by ID.
   * This map contains every procedure. root-procedures and subprocedures.
   */
  private final ConcurrentHashMap<Long, Procedure<TEnvironment>> procedures =
    new ConcurrentHashMap<>();

  /**
   * Helper map to lookup whether the procedure already issued from the same client. This map
   * contains every root procedure.
   */
  private final ConcurrentHashMap<NonceKey, Long> nonceKeysToProcIdsMap = new ConcurrentHashMap<>();

  private final CopyOnWriteArrayList<ProcedureExecutorListener> listeners =
    new CopyOnWriteArrayList<>();

  private Configuration conf;

  /**
   * Created in the {@link #init(int, boolean)} method. Destroyed in {@link #join()} (FIX! Doing
   * resource handling rather than observing in a #join is unexpected).
   * Overridden when we do the ProcedureTestingUtility.testRecoveryAndDoubleExecution trickery
   * (Should be ok).
   */
  private ThreadGroup threadGroup;

  /**
   * Created in the {@link #init(int, boolean)}  method. Terminated in {@link #join()} (FIX! Doing
   * resource handling rather than observing in a #join is unexpected).
   * Overridden when we do the ProcedureTestingUtility.testRecoveryAndDoubleExecution trickery
   * (Should be ok).
   */
  private CopyOnWriteArrayList<WorkerThread> workerThreads;

  /**
   * Created in the {@link #init(int, boolean)} method. Terminated in {@link #join()} (FIX! Doing
   * resource handling rather than observing in a #join is unexpected).
   * Overridden when we do the ProcedureTestingUtility.testRecoveryAndDoubleExecution trickery
   * (Should be ok).
   */
  private TimeoutExecutorThread<TEnvironment> timeoutExecutor;

  /**
   * WorkerMonitor check for stuck workers and new worker thread when necessary, for example if
   * there is no worker to assign meta, it will new worker thread for it, so it is very important.
   * TimeoutExecutor execute many tasks like DeadServerMetricRegionChore RegionInTransitionChore
   * and so on, some tasks may execute for a long time so will block other tasks like
   * WorkerMonitor, so use a dedicated thread for executing WorkerMonitor.
   */
  private TimeoutExecutorThread<TEnvironment> workerMonitorExecutor;

  private int corePoolSize;
  private int maxPoolSize;

  private volatile long keepAliveTime;

  /**
   * Scheduler/Queue that contains runnable procedures.
   */
  private final ProcedureScheduler scheduler;

  private final Executor forceUpdateExecutor = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Force-Update-PEWorker-%d").build());

  private final AtomicLong lastProcId = new AtomicLong(-1);
  private final AtomicLong workerId = new AtomicLong(0);
  private final AtomicInteger activeExecutorCount = new AtomicInteger(0);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final TEnvironment environment;
  private final ProcedureStore store;

  private final boolean checkOwnerSet;

  // To prevent concurrent execution of the same procedure.
  // For some rare cases, especially if the procedure uses ProcedureEvent, it is possible that the
  // procedure is woken up before we finish the suspend which causes the same procedures to be
  // executed in parallel. This does lead to some problems, see HBASE-20939&HBASE-20949, and is also
  // a bit confusing to the developers. So here we introduce this lock to prevent the concurrent
  // execution of the same procedure.
  private final IdLock procExecutionLock = new IdLock();

  public ProcedureExecutor(final Configuration conf, final TEnvironment environment,
      final ProcedureStore store) {
    this(conf, environment, store, new SimpleProcedureScheduler());
  }

  private boolean isRootFinished(Procedure<?> proc) {
    Procedure<?> rootProc = procedures.get(proc.getRootProcId());
    return rootProc == null || rootProc.isFinished();
  }

  private void forceUpdateProcedure(long procId) throws IOException {
    IdLock.Entry lockEntry = procExecutionLock.getLockEntry(procId);
    try {
      Procedure<TEnvironment> proc = procedures.get(procId);
      if (proc != null) {
        if (proc.isFinished() && proc.hasParent() && isRootFinished(proc)) {
          LOG.debug("Procedure {} has already been finished and parent is succeeded," +
            " skip force updating", proc);
          return;
        }
      } else {
        CompletedProcedureRetainer<TEnvironment> retainer = completed.get(procId);
        if (retainer == null || retainer.getProcedure() instanceof FailedProcedure) {
          LOG.debug("No pending procedure with id = {}, skip force updating.", procId);
          return;
        }
        long evictTtl = conf.getInt(EVICT_TTL_CONF_KEY, DEFAULT_EVICT_TTL);
        long evictAckTtl = conf.getInt(EVICT_ACKED_TTL_CONF_KEY, DEFAULT_ACKED_EVICT_TTL);
        if (retainer.isExpired(System.currentTimeMillis(), evictTtl, evictAckTtl)) {
          LOG.debug("Procedure {} has already been finished and expired, skip force updating",
            procId);
          return;
        }
        proc = retainer.getProcedure();
      }
      LOG.debug("Force update procedure {}", proc);
      store.update(proc);
    } finally {
      procExecutionLock.releaseLockEntry(lockEntry);
    }
  }

  public ProcedureExecutor(final Configuration conf, final TEnvironment environment,
      final ProcedureStore store, final ProcedureScheduler scheduler) {
    this.environment = environment;
    this.scheduler = scheduler;
    this.store = store;
    this.conf = conf;
    this.checkOwnerSet = conf.getBoolean(CHECK_OWNER_SET_CONF_KEY, DEFAULT_CHECK_OWNER_SET);
    refreshConfiguration(conf);
    store.registerListener(new ProcedureStoreListener() {

      @Override
      public void forceUpdate(long[] procIds) {
        Arrays.stream(procIds).forEach(procId -> forceUpdateExecutor.execute(() -> {
          try {
            forceUpdateProcedure(procId);
          } catch (IOException e) {
            LOG.warn("Failed to force update procedure with pid={}", procId);
          }
        }));
      }
    });
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

  private void restoreLock(Procedure<TEnvironment> proc, Set<Long> restored) {
    proc.restoreLock(getEnvironment());
    restored.add(proc.getProcId());
  }

  private void restoreLocks(Deque<Procedure<TEnvironment>> stack, Set<Long> restored) {
    while (!stack.isEmpty()) {
      restoreLock(stack.pop(), restored);
    }
  }

  // Restore the locks for all the procedures.
  // Notice that we need to restore the locks starting from the root proc, otherwise there will be
  // problem that a sub procedure may hold the exclusive lock first and then we are stuck when
  // calling the acquireLock method for the parent procedure.
  // The algorithm is straight-forward:
  // 1. Use a set to record the procedures which locks have already been restored.
  // 2. Use a stack to store the hierarchy of the procedures
  // 3. For all the procedure, we will first try to find its parent and push it into the stack,
  // unless
  // a. We have no parent, i.e, we are the root procedure
  // b. The lock has already been restored(by checking the set introduced in #1)
  // then we start to pop the stack and call acquireLock for each procedure.
  // Notice that this should be done for all procedures, not only the ones in runnableList.
  private void restoreLocks() {
    Set<Long> restored = new HashSet<>();
    Deque<Procedure<TEnvironment>> stack = new ArrayDeque<>();
    procedures.values().forEach(proc -> {
      for (;;) {
        if (restored.contains(proc.getProcId())) {
          restoreLocks(stack, restored);
          return;
        }
        if (!proc.hasParent()) {
          restoreLock(proc, restored);
          restoreLocks(stack, restored);
          return;
        }
        stack.push(proc);
        proc = procedures.get(proc.getParentProcId());
      }
    });
  }

  private void loadProcedures(ProcedureIterator procIter, boolean abortOnCorruption)
      throws IOException {
    // 1. Build the rollback stack
    int runnableCount = 0;
    int failedCount = 0;
    int waitingCount = 0;
    int waitingTimeoutCount = 0;
    while (procIter.hasNext()) {
      boolean finished = procIter.isNextFinished();
      @SuppressWarnings("unchecked")
      Procedure<TEnvironment> proc = procIter.next();
      NonceKey nonceKey = proc.getNonceKey();
      long procId = proc.getProcId();

      if (finished) {
        completed.put(proc.getProcId(), new CompletedProcedureRetainer<>(proc));
        LOG.debug("Completed {}", proc);
      } else {
        if (!proc.hasParent()) {
          assert !proc.isFinished() : "unexpected finished procedure";
          rollbackStack.put(proc.getProcId(), new RootProcedureState<>());
        }

        // add the procedure to the map
        proc.beforeReplay(getEnvironment());
        procedures.put(proc.getProcId(), proc);
        switch (proc.getState()) {
          case RUNNABLE:
            runnableCount++;
            break;
          case FAILED:
            failedCount++;
            break;
          case WAITING:
            waitingCount++;
            break;
          case WAITING_TIMEOUT:
            waitingTimeoutCount++;
            break;
          default:
            break;
        }
      }

      if (nonceKey != null) {
        nonceKeysToProcIdsMap.put(nonceKey, procId); // add the nonce to the map
      }
    }

    // 2. Initialize the stacks: In the old implementation, for procedures in FAILED state, we will
    // push it into the ProcedureScheduler directly to execute the rollback. But this does not work
    // after we introduce the restore lock stage. For now, when we acquire a xlock, we will remove
    // the queue from runQueue in scheduler, and then when a procedure which has lock access, for
    // example, a sub procedure of the procedure which has the xlock, is pushed into the scheduler,
    // we will add the queue back to let the workers poll from it. The assumption here is that, the
    // procedure which has the xlock should have been polled out already, so when loading we can not
    // add the procedure to scheduler first and then call acquireLock, since the procedure is still
    // in the queue, and since we will remove the queue from runQueue, then no one can poll it out,
    // then there is a dead lock
    List<Procedure<TEnvironment>> runnableList = new ArrayList<>(runnableCount);
    List<Procedure<TEnvironment>> failedList = new ArrayList<>(failedCount);
    List<Procedure<TEnvironment>> waitingList = new ArrayList<>(waitingCount);
    List<Procedure<TEnvironment>> waitingTimeoutList = new ArrayList<>(waitingTimeoutCount);
    procIter.reset();
    while (procIter.hasNext()) {
      if (procIter.isNextFinished()) {
        procIter.skipNext();
        continue;
      }

      @SuppressWarnings("unchecked")
      Procedure<TEnvironment> proc = procIter.next();
      assert !(proc.isFinished() && !proc.hasParent()) : "unexpected completed proc=" + proc;
      LOG.debug("Loading {}", proc);
      Long rootProcId = getRootProcedureId(proc);
      // The orphan procedures will be passed to handleCorrupted, so add an assert here
      assert rootProcId != null;

      if (proc.hasParent()) {
        Procedure<TEnvironment> parent = procedures.get(proc.getParentProcId());
        if (parent != null && !proc.isFinished()) {
          parent.incChildrenLatch();
        }
      }

      RootProcedureState<TEnvironment> procStack = rollbackStack.get(rootProcId);
      procStack.loadStack(proc);

      proc.setRootProcId(rootProcId);
      switch (proc.getState()) {
        case RUNNABLE:
          runnableList.add(proc);
          break;
        case WAITING:
          waitingList.add(proc);
          break;
        case WAITING_TIMEOUT:
          waitingTimeoutList.add(proc);
          break;
        case FAILED:
          failedList.add(proc);
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

    // 3. Check the waiting procedures to see if some of them can be added to runnable.
    waitingList.forEach(proc -> {
      if (!proc.hasChildren()) {
        // Normally, WAITING procedures should be waken by its children. But, there is a case that,
        // all the children are successful and before they can wake up their parent procedure, the
        // master was killed. So, during recovering the procedures from ProcedureWal, its children
        // are not loaded because of their SUCCESS state. So we need to continue to run this WAITING
        // procedure. But before executing, we need to set its state to RUNNABLE, otherwise, a
        // exception will throw:
        // Preconditions.checkArgument(procedure.getState() == ProcedureState.RUNNABLE,
        // "NOT RUNNABLE! " + procedure.toString());
        proc.setState(ProcedureState.RUNNABLE);
        runnableList.add(proc);
      } else {
        proc.afterReplay(getEnvironment());
      }
    });
    // 4. restore locks
    restoreLocks();

    // 5. Push the procedures to the timeout executor
    waitingTimeoutList.forEach(proc -> {
      proc.afterReplay(getEnvironment());
      timeoutExecutor.add(proc);
    });

    // 6. Push the procedure to the scheduler
    failedList.forEach(scheduler::addBack);
    runnableList.forEach(p -> {
      p.afterReplay(getEnvironment());
      if (!p.hasParent()) {
        sendProcedureLoadedNotification(p.getProcId());
      }
      scheduler.addBack(p);
    });
    // After all procedures put into the queue, signal the worker threads.
    // Otherwise, there is a race condition. See HBASE-21364.
    scheduler.signalAll();
  }

  /**
   * Initialize the procedure executor, but do not start workers. We will start them later.
   * <p/>
   * It calls ProcedureStore.recoverLease() and ProcedureStore.load() to recover the lease, and
   * ensure a single executor, and start the procedure replay to resume and recover the previous
   * pending and in-progress procedures.
   * @param numThreads number of threads available for procedure execution.
   * @param abortOnCorruption true if you want to abort your service in case a corrupted procedure
   *          is found on replay. otherwise false.
   */
  public void init(int numThreads, boolean abortOnCorruption) throws IOException {
    // We have numThreads executor + one timer thread used for timing out
    // procedures and triggering periodic procedures.
    this.corePoolSize = numThreads;
    this.maxPoolSize = 10 * numThreads;
    LOG.info("Starting {} core workers (bigger of cpus/4 or 16) with max (burst) worker count={}",
        corePoolSize, maxPoolSize);

    this.threadGroup = new ThreadGroup("PEWorkerGroup");
    this.timeoutExecutor = new TimeoutExecutorThread<>(this, threadGroup, "ProcExecTimeout");
    this.workerMonitorExecutor = new TimeoutExecutorThread<>(this, threadGroup, "WorkerMonitor");

    // Create the workers
    workerId.set(0);
    workerThreads = new CopyOnWriteArrayList<>();
    for (int i = 0; i < corePoolSize; ++i) {
      workerThreads.add(new WorkerThread(threadGroup));
    }

    long st, et;

    // Acquire the store lease.
    st = System.nanoTime();
    store.recoverLease();
    et = System.nanoTime();
    LOG.info("Recovered {} lease in {}", store.getClass().getSimpleName(),
      StringUtils.humanTimeDiff(TimeUnit.NANOSECONDS.toMillis(et - st)));

    // start the procedure scheduler
    scheduler.start();

    // TODO: Split in two steps.
    // TODO: Handle corrupted procedures (currently just a warn)
    // The first one will make sure that we have the latest id,
    // so we can start the threads and accept new procedures.
    // The second step will do the actual load of old procedures.
    st = System.nanoTime();
    load(abortOnCorruption);
    et = System.nanoTime();
    LOG.info("Loaded {} in {}", store.getClass().getSimpleName(),
      StringUtils.humanTimeDiff(TimeUnit.NANOSECONDS.toMillis(et - st)));
  }

  /**
   * Start the workers.
   */
  public void startWorkers() throws IOException {
    if (!running.compareAndSet(false, true)) {
      LOG.warn("Already running");
      return;
    }
    // Start the executors. Here we must have the lastProcId set.
    LOG.trace("Start workers {}", workerThreads.size());
    timeoutExecutor.start();
    workerMonitorExecutor.start();
    for (WorkerThread worker: workerThreads) {
      worker.start();
    }

    // Internal chores
    workerMonitorExecutor.add(new WorkerMonitor());

    // Add completed cleaner chore
    addChore(new CompletedProcedureCleaner<>(conf, store, procExecutionLock, completed,
      nonceKeysToProcIdsMap));
  }

  public void stop() {
    if (!running.getAndSet(false)) {
      return;
    }

    LOG.info("Stopping");
    scheduler.stop();
    timeoutExecutor.sendStopSignal();
    workerMonitorExecutor.sendStopSignal();
  }

  public void join() {
    assert !isRunning() : "expected not running";

    // stop the timeout executor
    timeoutExecutor.awaitTermination();
    // stop the work monitor executor
    workerMonitorExecutor.awaitTermination();

    // stop the worker threads
    for (WorkerThread worker: workerThreads) {
      worker.awaitTermination();
    }

    // Destroy the Thread Group for the executors
    // TODO: Fix. #join is not place to destroy resources.
    try {
      threadGroup.destroy();
    } catch (IllegalThreadStateException e) {
      LOG.error("ThreadGroup {} contains running threads; {}: See STDOUT",
          this.threadGroup, e.getMessage());
      // This dumps list of threads on STDOUT.
      this.threadGroup.list();
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
  public void addChore(@Nullable ProcedureInMemoryChore<TEnvironment> chore) {
    if (chore == null) {
      return;
    }
    chore.setState(ProcedureState.WAITING_TIMEOUT);
    timeoutExecutor.add(chore);
  }

  /**
   * Remove a chore procedure from the executor
   * @param chore the chore to remove
   * @return whether the chore is removed, or it will be removed later
   */
  public boolean removeChore(@Nullable ProcedureInMemoryChore<TEnvironment> chore) {
    if (chore == null) {
      return true;
    }
    chore.setState(ProcedureState.SUCCESS);
    return timeoutExecutor.remove(chore);
  }

  // ==========================================================================
  //  Nonce Procedure helpers
  // ==========================================================================
  /**
   * Create a NonceKey from the specified nonceGroup and nonce.
   * @param nonceGroup the group to use for the {@link NonceKey}
   * @param nonce the nonce to use in the {@link NonceKey}
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
    if (nonceKey == null) {
      return -1;
    }

    // check if we have already a Reserved ID for the nonce
    Long oldProcId = nonceKeysToProcIdsMap.get(nonceKey);
    if (oldProcId == null) {
      // reserve a new Procedure ID, this will be associated with the nonce
      // and the procedure submitted with the specified nonce will use this ID.
      final long newProcId = nextProcId();
      oldProcId = nonceKeysToProcIdsMap.putIfAbsent(nonceKey, newProcId);
      if (oldProcId == null) {
        return -1;
      }
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
    if (nonceKey == null) {
      return;
    }

    final Long procId = nonceKeysToProcIdsMap.get(nonceKey);
    if (procId == null) {
      return;
    }

    // if the procedure was not submitted, remove the nonce
    if (!(procedures.containsKey(procId) || completed.containsKey(procId))) {
      nonceKeysToProcIdsMap.remove(nonceKey);
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
  public void setFailureResultForNonce(NonceKey nonceKey, String procName, User procOwner,
      IOException exception) {
    if (nonceKey == null) {
      return;
    }

    Long procId = nonceKeysToProcIdsMap.get(nonceKey);
    if (procId == null || completed.containsKey(procId)) {
      return;
    }

    Procedure<TEnvironment> proc =
      new FailedProcedure<>(procId.longValue(), procName, procOwner, nonceKey, exception);

    completed.putIfAbsent(procId, new CompletedProcedureRetainer<>(proc));
  }

  // ==========================================================================
  //  Submit/Abort Procedure
  // ==========================================================================
  /**
   * Add a new root-procedure to the executor.
   * @param proc the new procedure to execute.
   * @return the procedure id, that can be used to monitor the operation
   */
  public long submitProcedure(Procedure<TEnvironment> proc) {
    return submitProcedure(proc, null);
  }

  /**
   * Bypass a procedure. If the procedure is set to bypass, all the logic in
   * execute/rollback will be ignored and it will return success, whatever.
   * It is used to recover buggy stuck procedures, releasing the lock resources
   * and letting other procedures run. Bypassing one procedure (and its ancestors will
   * be bypassed automatically) may leave the cluster in a middle state, e.g. region
   * not assigned, or some hdfs files left behind. After getting rid of those stuck procedures,
   * the operators may have to do some clean up on hdfs or schedule some assign procedures
   * to let region online. DO AT YOUR OWN RISK.
   * <p>
   * A procedure can be bypassed only if
   * 1. The procedure is in state of RUNNABLE, WAITING, WAITING_TIMEOUT
   * or it is a root procedure without any child.
   * 2. No other worker thread is executing it
   * 3. No child procedure has been submitted
   *
   * <p>
   * If all the requirements are meet, the procedure and its ancestors will be
   * bypassed and persisted to WAL.
   *
   * <p>
   * If the procedure is in WAITING state, will set it to RUNNABLE add it to run queue.
   * TODO: What about WAITING_TIMEOUT?
   * @param pids the procedure id
   * @param lockWait time to wait lock
   * @param force if force set to true, we will bypass the procedure even if it is executing.
   *              This is for procedures which can't break out during executing(due to bug, mostly)
   *              In this case, bypassing the procedure is not enough, since it is already stuck
   *              there. We need to restart the master after bypassing, and letting the problematic
   *              procedure to execute wth bypass=true, so in that condition, the procedure can be
   *              successfully bypassed.
   * @param recursive We will do an expensive search for children of each pid. EXPENSIVE!
   * @return true if bypass success
   * @throws IOException IOException
   */
  public List<Boolean> bypassProcedure(List<Long> pids, long lockWait, boolean force,
      boolean recursive)
      throws IOException {
    List<Boolean> result = new ArrayList<Boolean>(pids.size());
    for(long pid: pids) {
      result.add(bypassProcedure(pid, lockWait, force, recursive));
    }
    return result;
  }

  boolean bypassProcedure(long pid, long lockWait, boolean override, boolean recursive)
      throws IOException {
    Preconditions.checkArgument(lockWait > 0, "lockWait should be positive");
    final Procedure<TEnvironment> procedure = getProcedure(pid);
    if (procedure == null) {
      LOG.debug("Procedure pid={} does not exist, skipping bypass", pid);
      return false;
    }

    LOG.debug("Begin bypass {} with lockWait={}, override={}, recursive={}",
        procedure, lockWait, override, recursive);
    IdLock.Entry lockEntry = procExecutionLock.tryLockEntry(procedure.getProcId(), lockWait);
    if (lockEntry == null && !override) {
      LOG.debug("Waited {} ms, but {} is still running, skipping bypass with force={}",
          lockWait, procedure, override);
      return false;
    } else if (lockEntry == null) {
      LOG.debug("Waited {} ms, but {} is still running, begin bypass with force={}",
          lockWait, procedure, override);
    }
    try {
      // check whether the procedure is already finished
      if (procedure.isFinished()) {
        LOG.debug("{} is already finished, skipping bypass", procedure);
        return false;
      }

      if (procedure.hasChildren()) {
        if (recursive) {
          // EXPENSIVE. Checks each live procedure of which there could be many!!!
          // Is there another way to get children of a procedure?
          LOG.info("Recursive bypass on children of pid={}", procedure.getProcId());
          this.procedures.forEachValue(1 /*Single-threaded*/,
            // Transformer
            v -> v.getParentProcId() == procedure.getProcId()? v: null,
            // Consumer
            v -> {
              try {
                bypassProcedure(v.getProcId(), lockWait, override, recursive);
              } catch (IOException e) {
                LOG.warn("Recursive bypass of pid={}", v.getProcId(), e);
              }
            });
        } else {
          LOG.debug("{} has children, skipping bypass", procedure);
          return false;
        }
      }

      // If the procedure has no parent or no child, we are safe to bypass it in whatever state
      if (procedure.hasParent() && procedure.getState() != ProcedureState.RUNNABLE
          && procedure.getState() != ProcedureState.WAITING
          && procedure.getState() != ProcedureState.WAITING_TIMEOUT) {
        LOG.debug("Bypassing procedures in RUNNABLE, WAITING and WAITING_TIMEOUT states "
                + "(with no parent), {}",
            procedure);
        // Question: how is the bypass done here?
        return false;
      }

      // Now, the procedure is not finished, and no one can execute it since we take the lock now
      // And we can be sure that its ancestor is not running too, since their child has not
      // finished yet
      Procedure<TEnvironment> current = procedure;
      while (current != null) {
        LOG.debug("Bypassing {}", current);
        current.bypass(getEnvironment());
        store.update(current);
        long parentID = current.getParentProcId();
        current = getProcedure(parentID);
      }

      //wake up waiting procedure, already checked there is no child
      if (procedure.getState() == ProcedureState.WAITING) {
        procedure.setState(ProcedureState.RUNNABLE);
        store.update(procedure);
      }

      // If state of procedure is WAITING_TIMEOUT, we can directly submit it to the scheduler.
      // Instead we should remove it from timeout Executor queue and tranfer its state to RUNNABLE
      if (procedure.getState() == ProcedureState.WAITING_TIMEOUT) {
        LOG.debug("transform procedure {} from WAITING_TIMEOUT to RUNNABLE", procedure);
        if (timeoutExecutor.remove(procedure)) {
          LOG.debug("removed procedure {} from timeoutExecutor", procedure);
          timeoutExecutor.executeTimedoutProcedure(procedure);
        }
      } else if (lockEntry != null) {
        scheduler.addFront(procedure);
        LOG.debug("Bypassing {} and its ancestors successfully, adding to queue", procedure);
      } else {
        // If we don't have the lock, we can't re-submit the queue,
        // since it is already executing. To get rid of the stuck situation, we
        // need to restart the master. With the procedure set to bypass, the procedureExecutor
        // will bypass it and won't get stuck again.
        LOG.debug("Bypassing {} and its ancestors successfully, but since it is already running, "
            + "skipping add to queue",
          procedure);
      }
      return true;

    } finally {
      if (lockEntry != null) {
        procExecutionLock.releaseLockEntry(lockEntry);
      }
    }
  }

  /**
   * Add a new root-procedure to the executor.
   * @param proc the new procedure to execute.
   * @param nonceKey the registered unique identifier for this operation from the client or process.
   * @return the procedure id, that can be used to monitor the operation
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification = "FindBugs is blind to the check-for-null")
  public long submitProcedure(Procedure<TEnvironment> proc, NonceKey nonceKey) {
    Preconditions.checkArgument(lastProcId.get() >= 0);

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
    LOG.debug("Stored {}", proc);

    // Add the procedure to the executor
    return pushProcedure(proc);
  }

  /**
   * Add a set of new root-procedure to the executor.
   * @param procs the new procedures to execute.
   */
  // TODO: Do we need to take nonces here?
  public void submitProcedures(Procedure<TEnvironment>[] procs) {
    Preconditions.checkArgument(lastProcId.get() >= 0);
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

  private Procedure<TEnvironment> prepareProcedure(Procedure<TEnvironment> proc) {
    Preconditions.checkArgument(proc.getState() == ProcedureState.INITIALIZING);
    Preconditions.checkArgument(!proc.hasParent(), "unexpected parent", proc);
    if (this.checkOwnerSet) {
      Preconditions.checkArgument(proc.hasOwner(), "missing owner");
    }
    return proc;
  }

  private long pushProcedure(Procedure<TEnvironment> proc) {
    final long currentProcId = proc.getProcId();

    // Update metrics on start of a procedure
    proc.updateMetricsOnSubmit(getEnvironment());

    // Create the rollback stack for the procedure
    RootProcedureState<TEnvironment> stack = new RootProcedureState<>();
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
  public boolean abort(long procId) {
    return abort(procId, true);
  }

  /**
   * Send an abort notification to the specified procedure.
   * Depending on the procedure implementation, the abort can be considered or ignored.
   * @param procId the procedure to abort
   * @param mayInterruptIfRunning if the proc completed at least one step, should it be aborted?
   * @return true if the procedure exists and has received the abort, otherwise false.
   */
  public boolean abort(long procId, boolean mayInterruptIfRunning) {
    Procedure<TEnvironment> proc = procedures.get(procId);
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
  public Procedure<TEnvironment> getProcedure(final long procId) {
    return procedures.get(procId);
  }

  public <T extends Procedure<TEnvironment>> T getProcedure(Class<T> clazz, long procId) {
    Procedure<TEnvironment> proc = getProcedure(procId);
    if (clazz.isInstance(proc)) {
      return clazz.cast(proc);
    }
    return null;
  }

  public Procedure<TEnvironment> getResult(long procId) {
    CompletedProcedureRetainer<TEnvironment> retainer = completed.get(procId);
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
  public boolean isStarted(long procId) {
    Procedure<?> proc = procedures.get(procId);
    if (proc == null) {
      return completed.get(procId) != null;
    }
    return proc.wasExecuted();
  }

  /**
   * Mark the specified completed procedure, as ready to remove.
   * @param procId the ID of the procedure to remove
   */
  public void removeResult(long procId) {
    CompletedProcedureRetainer<TEnvironment> retainer = completed.get(procId);
    if (retainer == null) {
      assert !procedures.containsKey(procId) : "pid=" + procId + " is still running";
      LOG.debug("pid={} already removed by the cleaner.", procId);
      return;
    }

    // The CompletedProcedureCleaner will take care of deletion, once the TTL is expired.
    retainer.setClientAckTime(EnvironmentEdgeManager.currentTime());
  }

  public Procedure<TEnvironment> getResultOrProcedure(long procId) {
    CompletedProcedureRetainer<TEnvironment> retainer = completed.get(procId);
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
  public boolean isProcedureOwner(long procId, User user) {
    if (user == null) {
      return false;
    }
    final Procedure<TEnvironment> runningProc = procedures.get(procId);
    if (runningProc != null) {
      return runningProc.getOwner().equals(user.getShortName());
    }

    final CompletedProcedureRetainer<TEnvironment> retainer = completed.get(procId);
    if (retainer != null) {
      return retainer.getProcedure().getOwner().equals(user.getShortName());
    }

    // Procedure either does not exist or has already completed and got cleaned up.
    // At this time, we cannot check the owner of the procedure
    return false;
  }

  /**
   * Should only be used when starting up, where the procedure workers have not been started.
   * <p/>
   * If the procedure works has been started, the return values maybe changed when you are
   * processing it so usually this is not safe. Use {@link #getProcedures()} below for most cases as
   * it will do a copy, and also include the finished procedures.
   */
  public Collection<Procedure<TEnvironment>> getActiveProceduresNoCopy() {
    return procedures.values();
  }

  /**
   * Get procedures.
   * @return the procedures in a list
   */
  public List<Procedure<TEnvironment>> getProcedures() {
    List<Procedure<TEnvironment>> procedureList =
      new ArrayList<>(procedures.size() + completed.size());
    procedureList.addAll(procedures.values());
    // Note: The procedure could show up twice in the list with different state, as
    // it could complete after we walk through procedures list and insert into
    // procedureList - it is ok, as we will use the information in the Procedure
    // to figure it out; to prevent this would increase the complexity of the logic.
    completed.values().stream().map(CompletedProcedureRetainer::getProcedure)
      .forEach(procedureList::add);
    return procedureList;
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
        if (procId >= 0) {
          break;
        }
      }
      while (procedures.containsKey(procId)) {
        procId = lastProcId.incrementAndGet();
      }
    }
    assert procId >= 0 : "Invalid procId " + procId;
    return procId;
  }

  protected long getLastProcId() {
    return lastProcId.get();
  }

  public Set<Long> getActiveProcIds() {
    return procedures.keySet();
  }

  Long getRootProcedureId(Procedure<TEnvironment> proc) {
    return Procedure.getRootProcedureId(procedures, proc);
  }

  // ==========================================================================
  //  Executions
  // ==========================================================================
  private void executeProcedure(Procedure<TEnvironment> proc) {
    if (proc.isFinished()) {
      LOG.debug("{} is already finished, skipping execution", proc);
      return;
    }
    final Long rootProcId = getRootProcedureId(proc);
    if (rootProcId == null) {
      // The 'proc' was ready to run but the root procedure was rolledback
      LOG.warn("Rollback because parent is done/rolledback proc=" + proc);
      executeRollback(proc);
      return;
    }

    RootProcedureState<TEnvironment> procStack = rollbackStack.get(rootProcId);
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

  private LockState acquireLock(Procedure<TEnvironment> proc) {
    TEnvironment env = getEnvironment();
    // if holdLock is true, then maybe we already have the lock, so just return LOCK_ACQUIRED if
    // hasLock is true.
    if (proc.hasLock()) {
      return LockState.LOCK_ACQUIRED;
    }
    return proc.doAcquireLock(env, store);
  }

  private void releaseLock(Procedure<TEnvironment> proc, boolean force) {
    TEnvironment env = getEnvironment();
    // For how the framework works, we know that we will always have the lock
    // when we call releaseLock(), so we can avoid calling proc.hasLock()
    if (force || !proc.holdLock(env) || proc.isFinished()) {
      proc.doReleaseLock(env, store);
    }
  }

  /**
   * Execute the rollback of the full procedure stack. Once the procedure is rolledback, the
   * root-procedure will be visible as finished to user, and the result will be the fatal exception.
   */
  private LockState executeRollback(long rootProcId, RootProcedureState<TEnvironment> procStack) {
    Procedure<TEnvironment> rootProc = procedures.get(rootProcId);
    RemoteProcedureException exception = rootProc.getException();
    // TODO: This needs doc. The root proc doesn't have an exception. Maybe we are
    // rolling back because the subprocedure does. Clarify.
    if (exception == null) {
      exception = procStack.getException();
      rootProc.setFailure(exception);
      store.update(rootProc);
    }

    List<Procedure<TEnvironment>> subprocStack = procStack.getSubproceduresStack();
    assert subprocStack != null : "Called rollback with no steps executed rootProc=" + rootProc;

    int stackTail = subprocStack.size();
    while (stackTail-- > 0) {
      Procedure<TEnvironment> proc = subprocStack.get(stackTail);
      IdLock.Entry lockEntry = null;
      // Hold the execution lock if it is not held by us. The IdLock is not reentrant so we need
      // this check, as the worker will hold the lock before executing a procedure. This is the only
      // place where we may hold two procedure execution locks, and there is a fence in the
      // RootProcedureState where we can make sure that only one worker can execute the rollback of
      // a RootProcedureState, so there is no dead lock problem. And the lock here is necessary to
      // prevent race between us and the force update thread.
      if (!procExecutionLock.isHeldByCurrentThread(proc.getProcId())) {
        try {
          lockEntry = procExecutionLock.getLockEntry(proc.getProcId());
        } catch (IOException e) {
          // can only happen if interrupted, so not a big deal to propagate it
          throw new UncheckedIOException(e);
        }
      }
      try {
        // For the sub procedures which are successfully finished, we do not rollback them.
        // Typically, if we want to rollback a procedure, we first need to rollback it, and then
        // recursively rollback its ancestors. The state changes which are done by sub procedures
        // should be handled by parent procedures when rolling back. For example, when rolling back
        // a MergeTableProcedure, we will schedule new procedures to bring the offline regions
        // online, instead of rolling back the original procedures which offlined the regions(in
        // fact these procedures can not be rolled back...).
        if (proc.isSuccess()) {
          // Just do the cleanup work, without actually executing the rollback
          subprocStack.remove(stackTail);
          cleanupAfterRollbackOneStep(proc);
          continue;
        }
        LockState lockState = acquireLock(proc);
        if (lockState != LockState.LOCK_ACQUIRED) {
          // can't take a lock on the procedure, add the root-proc back on the
          // queue waiting for the lock availability
          return lockState;
        }

        lockState = executeRollback(proc);
        releaseLock(proc, false);
        boolean abortRollback = lockState != LockState.LOCK_ACQUIRED;
        abortRollback |= !isRunning() || !store.isRunning();

        // allows to kill the executor before something is stored to the wal.
        // useful to test the procedure recovery.
        if (abortRollback) {
          return lockState;
        }

        subprocStack.remove(stackTail);

        // if the procedure is kind enough to pass the slot to someone else, yield
        // if the proc is already finished, do not yield
        if (!proc.isFinished() && proc.isYieldAfterExecutionStep(getEnvironment())) {
          return LockState.LOCK_YIELD_WAIT;
        }

        if (proc != rootProc) {
          execCompletionCleanup(proc);
        }
      } finally {
        if (lockEntry != null) {
          procExecutionLock.releaseLockEntry(lockEntry);
        }
      }
    }

    // Finalize the procedure state
    LOG.info("Rolled back {} exec-time={}", rootProc,
      StringUtils.humanTimeDiff(rootProc.elapsedTime()));
    procedureFinished(rootProc);
    return LockState.LOCK_ACQUIRED;
  }

  private void cleanupAfterRollbackOneStep(Procedure<TEnvironment> proc) {
    if (proc.removeStackIndex()) {
      if (!proc.isSuccess()) {
        proc.setState(ProcedureState.ROLLEDBACK);
      }

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
  }

  /**
   * Execute the rollback of the procedure step.
   * It updates the store with the new state (stack index)
   * or will remove completly the procedure in case it is a child.
   */
  private LockState executeRollback(Procedure<TEnvironment> proc) {
    try {
      proc.doRollback(getEnvironment());
    } catch (IOException e) {
      LOG.debug("Roll back attempt failed for {}", proc, e);
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
      String msg = "TESTING: Kill before store update";
      LOG.debug(msg);
      stop();
      throw new RuntimeException(msg);
    }

    cleanupAfterRollbackOneStep(proc);

    return LockState.LOCK_ACQUIRED;
  }

  private void yieldProcedure(Procedure<TEnvironment> proc) {
    releaseLock(proc, false);
    scheduler.yield(proc);
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
  private void execProcedure(RootProcedureState<TEnvironment> procStack,
      Procedure<TEnvironment> procedure) {
    Preconditions.checkArgument(procedure.getState() == ProcedureState.RUNNABLE,
        "NOT RUNNABLE! " + procedure.toString());

    // Procedures can suspend themselves. They skip out by throwing a ProcedureSuspendedException.
    // The exception is caught below and then we hurry to the exit without disturbing state. The
    // idea is that the processing of this procedure will be unsuspended later by an external event
    // such the report of a region open.
    boolean suspended = false;

    // Whether to 're-' -execute; run through the loop again.
    boolean reExecute = false;

    Procedure<TEnvironment>[] subprocs = null;
    do {
      reExecute = false;
      procedure.resetPersistence();
      try {
        subprocs = procedure.doExecute(getEnvironment());
        if (subprocs != null && subprocs.length == 0) {
          subprocs = null;
        }
      } catch (ProcedureSuspendedException e) {
        LOG.trace("Suspend {}", procedure);
        suspended = true;
      } catch (ProcedureYieldException e) {
        LOG.trace("Yield {}", procedure, e);
        yieldProcedure(procedure);
        return;
      } catch (InterruptedException e) {
        LOG.trace("Yield interrupt {}", procedure, e);
        handleInterruptedException(procedure, e);
        yieldProcedure(procedure);
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
            LOG.trace("Short-circuit to next step on pid={}", procedure.getProcId());
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
          LOG.trace("Added to timeoutExecutor {}", procedure);
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
      if (testing != null &&
        testing.shouldKillBeforeStoreUpdate(suspended, procedure.hasParent())) {
        kill("TESTING: Kill BEFORE store update: " + procedure);
      }

      // TODO: The code here doesn't check if store is running before persisting to the store as
      // it relies on the method call below to throw RuntimeException to wind up the stack and
      // executor thread to stop. The statement following the method call below seems to check if
      // store is not running, to prevent scheduling children procedures, re-execution or yield
      // of this procedure. This may need more scrutiny and subsequent cleanup in future
      //
      // Commit the transaction even if a suspend (state may have changed). Note this append
      // can take a bunch of time to complete.
      if (procedure.needPersistence()) {
        updateStoreOnExec(procStack, procedure, subprocs);
      }

      // if the store is not running we are aborting
      if (!store.isRunning()) {
        return;
      }
      // if the procedure is kind enough to pass the slot to someone else, yield
      if (procedure.isRunnable() && !suspended &&
          procedure.isYieldAfterExecutionStep(getEnvironment())) {
        yieldProcedure(procedure);
        return;
      }

      assert (reExecute && subprocs == null) || !reExecute;
    } while (reExecute);

    // Allows to kill the executor after something is stored to the WAL but before the below
    // state settings are done -- in particular the one on the end where we make parent
    // RUNNABLE again when its children are done; see countDownChildren.
    if (testing != null && testing.shouldKillAfterStoreUpdate(suspended)) {
      kill("TESTING: Kill AFTER store update: " + procedure);
    }

    // Submit the new subprocedures
    if (subprocs != null && !procedure.isFailed()) {
      submitChildrenProcedures(subprocs);
    }

    // we need to log the release lock operation before waking up the parent procedure, as there
    // could be race that the parent procedure may call updateStoreOnExec ahead of us and remove all
    // the sub procedures from store and cause problems...
    releaseLock(procedure, false);

    // if the procedure is complete and has a parent, count down the children latch.
    // If 'suspended', do nothing to change state -- let other threads handle unsuspend event.
    if (!suspended && procedure.isFinished() && procedure.hasParent()) {
      countDownChildren(procStack, procedure);
    }
  }

  private void kill(String msg) {
    LOG.debug(msg);
    stop();
    throw new RuntimeException(msg);
  }

  private Procedure<TEnvironment>[] initializeChildren(RootProcedureState<TEnvironment> procStack,
      Procedure<TEnvironment> procedure, Procedure<TEnvironment>[] subprocs) {
    assert subprocs != null : "expected subprocedures";
    final long rootProcId = getRootProcedureId(procedure);
    for (int i = 0; i < subprocs.length; ++i) {
      Procedure<TEnvironment> subproc = subprocs[i];
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

  private void submitChildrenProcedures(Procedure<TEnvironment>[] subprocs) {
    for (int i = 0; i < subprocs.length; ++i) {
      Procedure<TEnvironment> subproc = subprocs[i];
      subproc.updateMetricsOnSubmit(getEnvironment());
      assert !procedures.containsKey(subproc.getProcId());
      procedures.put(subproc.getProcId(), subproc);
      scheduler.addFront(subproc);
    }
  }

  private void countDownChildren(RootProcedureState<TEnvironment> procStack,
      Procedure<TEnvironment> procedure) {
    Procedure<TEnvironment> parent = procedures.get(procedure.getParentProcId());
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
      LOG.info("Finished subprocedure pid={}, resume processing ppid={}",
        procedure.getProcId(), parent.getProcId());
      return;
    }
  }

  private void updateStoreOnExec(RootProcedureState<TEnvironment> procStack,
      Procedure<TEnvironment> procedure, Procedure<TEnvironment>[] subprocs) {
    if (subprocs != null && !procedure.isFailed()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Stored " + procedure + ", children " + Arrays.toString(subprocs));
      }
      store.insert(procedure, subprocs);
    } else {
      LOG.trace("Store update {}", procedure);
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

  private void handleInterruptedException(Procedure<TEnvironment> proc, InterruptedException e) {
    LOG.trace("Interrupt during {}. suspend and retry it later.", proc, e);
    // NOTE: We don't call Thread.currentThread().interrupt()
    // because otherwise all the subsequent calls e.g. Thread.sleep() will throw
    // the InterruptedException. If the master is going down, we will be notified
    // and the executor/store will be stopped.
    // (The interrupted procedure will be retried on the next run)
  }

  private void execCompletionCleanup(Procedure<TEnvironment> proc) {
    final TEnvironment env = getEnvironment();
    if (proc.hasLock()) {
      LOG.warn("Usually this should not happen, we will release the lock before if the procedure" +
        " is finished, even if the holdLock is true, arrive here means we have some holes where" +
        " we do not release the lock. And the releaseLock below may fail since the procedure may" +
        " have already been deleted from the procedure store.");
      releaseLock(proc, true);
    }
    try {
      proc.completionCleanup(env);
    } catch (Throwable e) {
      // Catch NullPointerExceptions or similar errors...
      LOG.error("CODE-BUG: uncatched runtime exception for procedure: " + proc, e);
    }
  }

  private void procedureFinished(Procedure<TEnvironment> proc) {
    // call the procedure completion cleanup handler
    execCompletionCleanup(proc);

    CompletedProcedureRetainer<TEnvironment> retainer = new CompletedProcedureRetainer<>(proc);

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
      LOG.error("CODE-BUG: uncatched runtime exception for completion cleanup: {}", proc, e);
    }

    // Notify the listeners
    sendProcedureFinishedNotification(proc.getProcId());
  }

  RootProcedureState<TEnvironment> getProcStack(long rootProcId) {
    return rollbackStack.get(rootProcId);
  }

  ProcedureScheduler getProcedureScheduler() {
    return scheduler;
  }

  int getCompletedSize() {
    return completed.size();
  }

  public IdLock getProcExecutionLock() {
    return procExecutionLock;
  }

  // ==========================================================================
  //  Worker Thread
  // ==========================================================================
  private class WorkerThread extends StoppableThread {
    private final AtomicLong executionStartTime = new AtomicLong(Long.MAX_VALUE);
    private volatile Procedure<TEnvironment> activeProcedure;

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
          @SuppressWarnings("unchecked")
          Procedure<TEnvironment> proc = scheduler.poll(keepAliveTime, TimeUnit.MILLISECONDS);
          if (proc == null) {
            continue;
          }
          this.activeProcedure = proc;
          int activeCount = activeExecutorCount.incrementAndGet();
          int runningCount = store.setRunningProcedureCount(activeCount);
          LOG.trace("Execute pid={} runningCount={}, activeCount={}", proc.getProcId(),
            runningCount, activeCount);
          executionStartTime.set(EnvironmentEdgeManager.currentTime());
          IdLock.Entry lockEntry = procExecutionLock.getLockEntry(proc.getProcId());
          try {
            executeProcedure(proc);
          } catch (AssertionError e) {
            LOG.info("ASSERT pid=" + proc.getProcId(), e);
            throw e;
          } finally {
            procExecutionLock.releaseLockEntry(lockEntry);
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
        LOG.warn("Worker stuck {}, run time {}", worker,
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
