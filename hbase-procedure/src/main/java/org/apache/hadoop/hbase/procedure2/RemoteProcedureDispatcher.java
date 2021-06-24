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
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.procedure2.util.DelayedUtil;
import org.apache.hadoop.hbase.procedure2.util.DelayedUtil.DelayedContainerWithTimestamp;
import org.apache.hadoop.hbase.procedure2.util.DelayedUtil.DelayedWithTimeout;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;

/**
 * A procedure dispatcher that aggregates and sends after elapsed time or after we hit
 * count threshold. Creates its own threadpool to run RPCs with timeout.
 * <ul>
 * <li>Each server queue has a dispatch buffer</li>
 * <li>Once the dispatch buffer reaches a threshold-size/time we send<li>
 * </ul>
 * <p>Call {@link #start()} and then {@link #submitTask(Runnable)}. When done,
 * call {@link #stop()}.
 */
@InterfaceAudience.Private
public abstract class RemoteProcedureDispatcher<TEnv, TRemote extends Comparable<TRemote>> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteProcedureDispatcher.class);

  public static final String THREAD_POOL_SIZE_CONF_KEY =
      "hbase.procedure.remote.dispatcher.threadpool.size";
  private static final int DEFAULT_THREAD_POOL_SIZE = 128;

  public static final String DISPATCH_DELAY_CONF_KEY =
      "hbase.procedure.remote.dispatcher.delay.msec";
  private static final int DEFAULT_DISPATCH_DELAY = 150;

  public static final String DISPATCH_MAX_QUEUE_SIZE_CONF_KEY =
      "hbase.procedure.remote.dispatcher.max.queue.size";
  private static final int DEFAULT_MAX_QUEUE_SIZE = 32;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final ConcurrentHashMap<TRemote, BufferNode> nodeMap =
      new ConcurrentHashMap<TRemote, BufferNode>();

  private final int operationDelay;
  private final int queueMaxSize;
  private final int corePoolSize;

  private TimeoutExecutorThread timeoutExecutor;
  private ThreadPoolExecutor threadPool;

  protected RemoteProcedureDispatcher(Configuration conf) {
    this.corePoolSize = conf.getInt(THREAD_POOL_SIZE_CONF_KEY, DEFAULT_THREAD_POOL_SIZE);
    this.operationDelay = conf.getInt(DISPATCH_DELAY_CONF_KEY, DEFAULT_DISPATCH_DELAY);
    this.queueMaxSize = conf.getInt(DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, DEFAULT_MAX_QUEUE_SIZE);
  }

  public boolean start() {
    if (running.getAndSet(true)) {
      LOG.warn("Already running");
      return false;
    }

    LOG.info("Instantiated, coreThreads={} (allowCoreThreadTimeOut=true), queueMaxSize={}, " +
        "operationDelay={}", this.corePoolSize, this.queueMaxSize, this.operationDelay);

    // Create the timeout executor
    timeoutExecutor = new TimeoutExecutorThread();
    timeoutExecutor.start();

    // Create the thread pool that will execute RPCs
    threadPool = Threads.getBoundedCachedThreadPool(corePoolSize, 60L, TimeUnit.SECONDS,
      new ThreadFactoryBuilder().setNameFormat(this.getClass().getSimpleName() + "-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(getUncaughtExceptionHandler()).build());
    return true;
  }

  protected void setTimeoutExecutorUncaughtExceptionHandler(UncaughtExceptionHandler eh) {
    timeoutExecutor.setUncaughtExceptionHandler(eh);
  }

  public boolean stop() {
    if (!running.getAndSet(false)) {
      return false;
    }

    LOG.info("Stopping procedure remote dispatcher");

    // send stop signals
    timeoutExecutor.sendStopSignal();
    threadPool.shutdownNow();
    return true;
  }

  public void join() {
    assert !running.get() : "expected not running";

    // wait the timeout executor
    timeoutExecutor.awaitTermination();
    timeoutExecutor = null;

    // wait for the thread pool to terminate
    threadPool.shutdownNow();
    try {
      while (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
        LOG.warn("Waiting for thread-pool to terminate");
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for thread-pool termination", e);
    }
  }

  protected abstract UncaughtExceptionHandler getUncaughtExceptionHandler();

  // ============================================================================================
  //  Node Helpers
  // ============================================================================================
  /**
   * Add a node that will be able to execute remote procedures
   * @param key the node identifier
   */
  public void addNode(final TRemote key) {
    assert key != null: "Tried to add a node with a null key";
    final BufferNode newNode = new BufferNode(key);
    nodeMap.putIfAbsent(key, newNode);
  }

  /**
   * Add a remote rpc.
   * @param key the node identifier
   */
  public void addOperationToNode(final TRemote key, RemoteProcedure rp)
          throws NullTargetServerDispatchException, NoServerDispatchException,
          NoNodeDispatchException {
    if (key == null) {
      throw new NullTargetServerDispatchException(rp.toString());
    }
    BufferNode node = nodeMap.get(key);
    if (node == null) {
      // If null here, it means node has been removed because it crashed. This happens when server
      // is expired in ServerManager. ServerCrashProcedure may or may not have run.
      throw new NoServerDispatchException(key.toString() + "; " + rp.toString());
    }
    node.add(rp);
    // Check our node still in the map; could have been removed by #removeNode.
    if (!nodeMap.containsValue(node)) {
      throw new NoNodeDispatchException(key.toString() + "; " + rp.toString());
    }
  }

  public void removeCompletedOperation(final TRemote key, RemoteProcedure rp) {
    BufferNode node = nodeMap.get(key);
    if (node == null) {
      LOG.warn("since no node for this key {}, we can't removed the finished remote procedure",
        key);
      return;
    }
    node.operationCompleted(rp);
  }

  /**
   * Remove a remote node
   * @param key the node identifier
   */
  public boolean removeNode(final TRemote key) {
    final BufferNode node = nodeMap.remove(key);
    if (node == null) {
      return false;
    }

    node.abortOperationsInQueue();
    return true;
  }

  // ============================================================================================
  //  Task Helpers
  // ============================================================================================
  protected final void submitTask(Runnable task) {
    threadPool.execute(task);
  }

  protected final void submitTask(Runnable task, long delay, TimeUnit unit) {
    timeoutExecutor.add(new DelayedTask(task, delay, unit));
  }

  protected abstract void remoteDispatch(TRemote key, Set<RemoteProcedure> operations);
  protected abstract void abortPendingOperations(TRemote key, Set<RemoteProcedure> operations);

  /**
   * Data structure with reference to remote operation.
   */
  public static abstract class RemoteOperation {
    private final RemoteProcedure remoteProcedure;

    protected RemoteOperation(final RemoteProcedure remoteProcedure) {
      this.remoteProcedure = remoteProcedure;
    }

    public RemoteProcedure getRemoteProcedure() {
      return remoteProcedure;
    }
  }

  /**
   * Remote procedure reference.
   */
  public interface RemoteProcedure<TEnv, TRemote> {
    /**
     * For building the remote operation.
     * May be empty if no need to send remote call. Usually, this means the RemoteProcedure has been
     * finished already. This is possible, as we may have already sent the procedure to RS but then
     * the rpc connection is broken so the executeProcedures call fails, but the RS does receive the
     * procedure and execute it and then report back, before we retry again.
     */
    Optional<RemoteOperation> remoteCallBuild(TEnv env, TRemote remote);

    /**
     * Called when the executeProcedure call is failed.
     */
    void remoteCallFailed(TEnv env, TRemote remote, IOException exception);

    /**
     * Called when RS tells the remote procedure is succeeded through the
     * {@code reportProcedureDone} method.
     */
    void remoteOperationCompleted(TEnv env);

    /**
     * Called when RS tells the remote procedure is failed through the {@code reportProcedureDone}
     * method.
     */
    void remoteOperationFailed(TEnv env, RemoteProcedureException error);

    /**
     * Whether store this remote procedure in dispatched queue
     * only OpenRegionProcedure and CloseRegionProcedure return false since they are
     * not fully controlled by dispatcher
     */
    default boolean storeInDispatchedQueue() {
      return true;
    }
  }

  /**
   * Account of what procedures are running on remote node.
   */
  public interface RemoteNode<TEnv, TRemote> {
    TRemote getKey();

    void add(RemoteProcedure<TEnv, TRemote> operation);

    void dispatch();
  }

  protected ArrayListMultimap<Class<?>, RemoteOperation> buildAndGroupRequestByType(final TEnv env,
      final TRemote remote, final Set<RemoteProcedure> remoteProcedures) {
    final ArrayListMultimap<Class<?>, RemoteOperation> requestByType = ArrayListMultimap.create();
    for (RemoteProcedure proc : remoteProcedures) {
      Optional<RemoteOperation> operation = proc.remoteCallBuild(env, remote);
      operation.ifPresent(op -> requestByType.put(op.getClass(), op));
    }
    return requestByType;
  }

  protected <T extends RemoteOperation> List<T> fetchType(
      final ArrayListMultimap<Class<?>, RemoteOperation> requestByType, final Class<T> type) {
    return (List<T>)requestByType.removeAll(type);
  }

  // ============================================================================================
  //  Timeout Helpers
  // ============================================================================================
  private final class TimeoutExecutorThread extends Thread {
    private final DelayQueue<DelayedWithTimeout> queue = new DelayQueue<DelayedWithTimeout>();

    public TimeoutExecutorThread() {
      super("ProcedureDispatcherTimeoutThread");
    }

    @Override
    public void run() {
      while (running.get()) {
        final DelayedWithTimeout task = DelayedUtil.takeWithoutInterrupt(queue,
          20, TimeUnit.SECONDS);
        if (task == null || task == DelayedUtil.DELAYED_POISON) {
          if (task == null && queue.size() > 0) {
            LOG.error("DelayQueue for RemoteProcedureDispatcher is not empty when timed waiting"
              + " elapsed. If this is repeated consistently, it means no element is getting expired"
              + " from the queue and it might freeze the system. Queue: {}", queue);
          }
          // the executor may be shutting down, and the task is just the shutdown request
          continue;
        }
        if (task instanceof DelayedTask) {
          threadPool.execute(((DelayedTask) task).getObject());
        } else {
          ((BufferNode) task).dispatch();
        }
      }
    }

    public void add(final DelayedWithTimeout delayed) {
      queue.add(delayed);
    }

    public void remove(final DelayedWithTimeout delayed) {
      queue.remove(delayed);
    }

    public void sendStopSignal() {
      queue.add(DelayedUtil.DELAYED_POISON);
    }

    public void awaitTermination() {
      try {
        final long startTime = EnvironmentEdgeManager.currentTime();
        for (int i = 0; isAlive(); ++i) {
          sendStopSignal();
          join(250);
          if (i > 0 && (i % 8) == 0) {
            LOG.warn("Waiting termination of thread " + getName() + ", " +
              StringUtils.humanTimeDiff(EnvironmentEdgeManager.currentTime() - startTime));
          }
        }
      } catch (InterruptedException e) {
        LOG.warn(getName() + " join wait got interrupted", e);
      }
    }
  }

  // ============================================================================================
  //  Internals Helpers
  // ============================================================================================

  /**
   * Node that contains a set of RemoteProcedures
   */
  protected final class BufferNode extends DelayedContainerWithTimestamp<TRemote>
      implements RemoteNode<TEnv, TRemote> {
    private Set<RemoteProcedure> operations;
    private final Set<RemoteProcedure> dispatchedOperations = new HashSet<>();

    protected BufferNode(final TRemote key) {
      super(key, 0);
    }

    @Override
    public TRemote getKey() {
      return getObject();
    }

    @Override
    public synchronized void add(final RemoteProcedure operation) {
      if (this.operations == null) {
        this.operations = new HashSet<>();
        setTimeout(EnvironmentEdgeManager.currentTime() + operationDelay);
        timeoutExecutor.add(this);
      }
      this.operations.add(operation);
      if (this.operations.size() > queueMaxSize) {
        timeoutExecutor.remove(this);
        dispatch();
      }
    }

    @Override
    public synchronized void dispatch() {
      if (operations != null) {
        remoteDispatch(getKey(), operations);
        operations.stream().filter(operation -> operation.storeInDispatchedQueue())
            .forEach(operation -> dispatchedOperations.add(operation));
        this.operations = null;
      }
    }

    public synchronized void abortOperationsInQueue() {
      if (operations != null) {
        abortPendingOperations(getKey(), operations);
        this.operations = null;
      }
      abortPendingOperations(getKey(), dispatchedOperations);
      this.dispatchedOperations.clear();
    }

    public synchronized void operationCompleted(final RemoteProcedure remoteProcedure){
      this.dispatchedOperations.remove(remoteProcedure);
    }

    @Override
    public String toString() {
      return super.toString() + ", operations=" + this.operations;
    }
  }

  /**
   * Delayed object that holds a FutureTask.
   * <p/>
   * used to submit something later to the thread-pool.
   */
  private static final class DelayedTask extends DelayedContainerWithTimestamp<Runnable> {
    public DelayedTask(Runnable task, long delay, TimeUnit unit) {
      super(task, EnvironmentEdgeManager.currentTime() + unit.toMillis(delay));
    }
  };
}
