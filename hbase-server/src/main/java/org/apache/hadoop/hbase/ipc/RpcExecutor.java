/*
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

package org.apache.hadoop.hbase.ipc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.BoundedPriorityBlockingQueue;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;

/**
 * Runs the CallRunners passed here via {@link #dispatch(CallRunner)}. Subclass and add particular
 * scheduling behavior.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public abstract class RpcExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(RpcExecutor.class);

  protected static final int DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT = 250;
  public static final String CALL_QUEUE_HANDLER_FACTOR_CONF_KEY =
    "hbase.ipc.server.callqueue.handler.factor";

  /** max delay in msec used to bound the de-prioritized requests */
  public static final String QUEUE_MAX_CALL_DELAY_CONF_KEY =
    "hbase.ipc.server.queue.max.call.delay";

  /**
   * The default, 'fifo', has the least friction but is dumb. If set to 'deadline', uses a priority
   * queue and de-prioritizes long-running scans. Sorting by priority comes at a cost, reduced
   * throughput.
   */
  public static final String CALL_QUEUE_TYPE_CODEL_CONF_VALUE = "codel";
  public static final String CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE = "deadline";
  public static final String CALL_QUEUE_TYPE_FIFO_CONF_VALUE = "fifo";
  public static final String CALL_QUEUE_TYPE_PLUGGABLE_CONF_VALUE = "pluggable";
  public static final String CALL_QUEUE_TYPE_CONF_KEY = "hbase.ipc.server.callqueue.type";
  public static final String CALL_QUEUE_TYPE_CONF_DEFAULT = CALL_QUEUE_TYPE_FIFO_CONF_VALUE;

  public static final String CALL_QUEUE_QUEUE_BALANCER_CLASS =
    "hbase.ipc.server.callqueue.balancer.class";
  public static final Class<?> CALL_QUEUE_QUEUE_BALANCER_CLASS_DEFAULT = RandomQueueBalancer.class;


  // These 3 are only used by Codel executor
  public static final String CALL_QUEUE_CODEL_TARGET_DELAY =
    "hbase.ipc.server.callqueue.codel.target.delay";
  public static final String CALL_QUEUE_CODEL_INTERVAL =
    "hbase.ipc.server.callqueue.codel.interval";
  public static final String CALL_QUEUE_CODEL_LIFO_THRESHOLD =
    "hbase.ipc.server.callqueue.codel.lifo.threshold";

  public static final int CALL_QUEUE_CODEL_DEFAULT_TARGET_DELAY = 100;
  public static final int CALL_QUEUE_CODEL_DEFAULT_INTERVAL = 100;
  public static final double CALL_QUEUE_CODEL_DEFAULT_LIFO_THRESHOLD = 0.8;

  public static final String PLUGGABLE_CALL_QUEUE_CLASS_NAME =
    "hbase.ipc.server.callqueue.pluggable.queue.class.name";
  public static final String PLUGGABLE_CALL_QUEUE_WITH_FAST_PATH_ENABLED =
    "hbase.ipc.server.callqueue.pluggable.queue.fast.path.enabled";

  private final LongAdder numGeneralCallsDropped = new LongAdder();
  private final LongAdder numLifoModeSwitches = new LongAdder();

  protected final int numCallQueues;
  protected final List<BlockingQueue<CallRunner>> queues;
  private final Class<? extends BlockingQueue> queueClass;
  private final Object[] queueInitArgs;

  protected volatile int currentQueueLimit;

  private final AtomicInteger activeHandlerCount = new AtomicInteger(0);
  private final List<RpcHandler> handlers;
  private final int handlerCount;
  private final AtomicInteger failedHandlerCount = new AtomicInteger(0);

  private String name;

  private final Configuration conf;
  private final Abortable abortable;

  public RpcExecutor(final String name, final int handlerCount, final int maxQueueLength,
      final PriorityFunction priority, final Configuration conf, final Abortable abortable) {
    this(name, handlerCount, conf.get(CALL_QUEUE_TYPE_CONF_KEY,
      CALL_QUEUE_TYPE_CONF_DEFAULT), maxQueueLength, priority, conf, abortable);
  }

  public RpcExecutor(final String name, final int handlerCount, final String callQueueType,
      final int maxQueueLength, final PriorityFunction priority, final Configuration conf,
      final Abortable abortable) {
    this.name = Strings.nullToEmpty(name);
    this.conf = conf;
    this.abortable = abortable;

    float callQueuesHandlersFactor = this.conf.getFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 0.1f);
    if (Float.compare(callQueuesHandlersFactor, 1.0f) > 0 ||
        Float.compare(0.0f, callQueuesHandlersFactor) > 0) {
      LOG.warn(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY +
        " is *ILLEGAL*, it should be in range [0.0, 1.0]");
      // For callQueuesHandlersFactor > 1.0, we just set it 1.0f.
      if (Float.compare(callQueuesHandlersFactor, 1.0f) > 0) {
        LOG.warn("Set " + CALL_QUEUE_HANDLER_FACTOR_CONF_KEY + " 1.0f");
        callQueuesHandlersFactor = 1.0f;
      } else {
        // But for callQueuesHandlersFactor < 0.0, following method #computeNumCallQueues
        // will compute max(1, -x) => 1 which has same effect of default value.
        LOG.warn("Set " + CALL_QUEUE_HANDLER_FACTOR_CONF_KEY + " default value 0.0f");
      }
    }
    this.numCallQueues = computeNumCallQueues(handlerCount, callQueuesHandlersFactor);
    this.queues = new ArrayList<>(this.numCallQueues);

    this.handlerCount = Math.max(handlerCount, this.numCallQueues);
    this.handlers = new ArrayList<>(this.handlerCount);

    if (isDeadlineQueueType(callQueueType)) {
      this.name += ".Deadline";
      this.queueInitArgs = new Object[] { maxQueueLength,
        new CallPriorityComparator(conf, priority) };
      this.queueClass = BoundedPriorityBlockingQueue.class;
    } else if (isCodelQueueType(callQueueType)) {
      this.name += ".Codel";
      int codelTargetDelay = conf.getInt(CALL_QUEUE_CODEL_TARGET_DELAY,
        CALL_QUEUE_CODEL_DEFAULT_TARGET_DELAY);
      int codelInterval = conf.getInt(CALL_QUEUE_CODEL_INTERVAL, CALL_QUEUE_CODEL_DEFAULT_INTERVAL);
      double codelLifoThreshold = conf.getDouble(CALL_QUEUE_CODEL_LIFO_THRESHOLD,
        CALL_QUEUE_CODEL_DEFAULT_LIFO_THRESHOLD);
      this.queueInitArgs = new Object[] { maxQueueLength, codelTargetDelay, codelInterval,
        codelLifoThreshold, numGeneralCallsDropped, numLifoModeSwitches };
      this.queueClass = AdaptiveLifoCoDelCallQueue.class;
    } else if (isPluggableQueueType(callQueueType)) {
      Optional<Class<? extends BlockingQueue<CallRunner>>> pluggableQueueClass =
        getPluggableQueueClass();

      if (!pluggableQueueClass.isPresent()) {
        throw new PluggableRpcQueueNotFound("Pluggable call queue failed to load and selected call"
          + " queue type required");
      } else {
        this.queueInitArgs = new Object[] { maxQueueLength, priority, conf };
        this.queueClass = pluggableQueueClass.get();
      }
    } else {
      this.name += ".Fifo";
      this.queueInitArgs = new Object[] { maxQueueLength };
      this.queueClass = LinkedBlockingQueue.class;
    }

    LOG.info("Instantiated {} with queueClass={}; " +
        "numCallQueues={}, maxQueueLength={}, handlerCount={}",
        this.name, this.queueClass, this.numCallQueues, maxQueueLength, this.handlerCount);
  }

  protected int computeNumCallQueues(final int handlerCount, final float callQueuesHandlersFactor) {
    return Math.max(1, Math.round(handlerCount * callQueuesHandlersFactor));
  }

  /**
   * Return the {@link Descriptors.MethodDescriptor#getName()} from {@code callRunner} or "Unknown".
   */
  private static String getMethodName(final CallRunner callRunner) {
    return Optional.ofNullable(callRunner)
      .map(CallRunner::getRpcCall)
      .map(RpcCall::getMethod)
      .map(Descriptors.MethodDescriptor::getName)
      .orElse("Unknown");
  }

  /**
   * Return the {@link RpcCall#getSize()} from {@code callRunner} or 0L.
   */
  private static long getRpcCallSize(final CallRunner callRunner) {
    return Optional.ofNullable(callRunner)
      .map(CallRunner::getRpcCall)
      .map(RpcCall::getSize)
      .orElse(0L);
  }

  public Map<String, Long> getCallQueueCountsSummary() {
    return queues.stream()
      .flatMap(Collection::stream)
      .map(RpcExecutor::getMethodName)
      .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
  }

  public Map<String, Long> getCallQueueSizeSummary() {
    return queues.stream()
      .flatMap(Collection::stream)
      .map(callRunner -> new Pair<>(getMethodName(callRunner), getRpcCallSize(callRunner)))
      .collect(Collectors.groupingBy(Pair::getFirst, Collectors.summingLong(Pair::getSecond)));
  }

  protected void initializeQueues(final int numQueues) {
    if (queueInitArgs.length > 0) {
      currentQueueLimit = (int) queueInitArgs[0];
      queueInitArgs[0] = Math.max((int) queueInitArgs[0], DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT);
    }
    for (int i = 0; i < numQueues; ++i) {
      queues.add(ReflectionUtils.newInstance(queueClass, queueInitArgs));
    }
  }

  public void start(final int port) {
    startHandlers(port);
  }

  public void stop() {
    for (RpcHandler handler : handlers) {
      handler.stopRunning();
      handler.interrupt();
    }
  }

  /** Add the request to the executor queue */
  public abstract boolean dispatch(final CallRunner callTask);

  /** Returns the list of request queues */
  protected List<BlockingQueue<CallRunner>> getQueues() {
    return queues;
  }

  protected void startHandlers(final int port) {
    List<BlockingQueue<CallRunner>> callQueues = getQueues();
    startHandlers(null, handlerCount, callQueues, 0, callQueues.size(), port, activeHandlerCount);
  }

  /**
   * Override if providing alternate Handler implementation.
   */
  protected RpcHandler getHandler(final String name, final double handlerFailureThreshhold,
      final int handlerCount, final BlockingQueue<CallRunner> q,
      final AtomicInteger activeHandlerCount, final AtomicInteger failedHandlerCount,
      final Abortable abortable) {
    return new RpcHandler(name, handlerFailureThreshhold, handlerCount, q, activeHandlerCount,
      failedHandlerCount, abortable);
  }

  /**
   * Start up our handlers.
   */
  protected void startHandlers(final String nameSuffix, final int numHandlers,
      final List<BlockingQueue<CallRunner>> callQueues, final int qindex, final int qsize,
      final int port, final AtomicInteger activeHandlerCount) {
    final String threadPrefix = name + Strings.nullToEmpty(nameSuffix);
    double handlerFailureThreshhold = conf == null ? 1.0 : conf.getDouble(
      HConstants.REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT,
      HConstants.DEFAULT_REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT);
    for (int i = 0; i < numHandlers; i++) {
      final int index = qindex + (i % qsize);
      String name = "RpcServer." + threadPrefix + ".handler=" + handlers.size() + ",queue=" + index
          + ",port=" + port;
      RpcHandler handler = getHandler(name, handlerFailureThreshhold, handlerCount,
        callQueues.get(index), activeHandlerCount, failedHandlerCount, abortable);
      handler.start();
      handlers.add(handler);
    }
    LOG.debug("Started handlerCount={} with threadPrefix={}, numCallQueues={}, port={}",
        handlers.size(), threadPrefix, qsize, port);
  }

  /**
   * All requests go to the first queue, at index 0
   */
  private static final QueueBalancer ONE_QUEUE = val -> 0;

  public static QueueBalancer getBalancer(
    final String executorName,
    final Configuration conf,
    final List<BlockingQueue<CallRunner>> queues
  ) {
    Preconditions.checkArgument(queues.size() > 0, "Queue size is <= 0, must be at least 1");
    if (queues.size() == 1) {
      return ONE_QUEUE;
    } else {
      Class<?> balancerClass = conf.getClass(
        CALL_QUEUE_QUEUE_BALANCER_CLASS, CALL_QUEUE_QUEUE_BALANCER_CLASS_DEFAULT);
      return (QueueBalancer) ReflectionUtils.newInstance(balancerClass, conf, executorName, queues);
    }
  }

  /**
   * Comparator used by the "normal callQueue" if DEADLINE_CALL_QUEUE_CONF_KEY is set to true. It
   * uses the calculated "deadline" e.g. to deprioritize long-running job If multiple requests have
   * the same deadline BoundedPriorityBlockingQueue will order them in FIFO (first-in-first-out)
   * manner.
   */
  private static class CallPriorityComparator implements Comparator<CallRunner> {
    private final static int DEFAULT_MAX_CALL_DELAY = 5000;

    private final PriorityFunction priority;
    private final int maxDelay;

    public CallPriorityComparator(final Configuration conf, final PriorityFunction priority) {
      this.priority = priority;
      this.maxDelay = conf.getInt(QUEUE_MAX_CALL_DELAY_CONF_KEY, DEFAULT_MAX_CALL_DELAY);
    }

    @Override
    public int compare(CallRunner a, CallRunner b) {
      RpcCall callA = a.getRpcCall();
      RpcCall callB = b.getRpcCall();
      long deadlineA = priority.getDeadline(callA.getHeader(), callA.getParam());
      long deadlineB = priority.getDeadline(callB.getHeader(), callB.getParam());
      deadlineA = callA.getReceiveTime() + Math.min(deadlineA, maxDelay);
      deadlineB = callB.getReceiveTime() + Math.min(deadlineB, maxDelay);
      return Long.compare(deadlineA, deadlineB);
    }
  }

  public static boolean isDeadlineQueueType(final String callQueueType) {
    return callQueueType.equals(CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE);
  }

  public static boolean isCodelQueueType(final String callQueueType) {
    return callQueueType.equals(CALL_QUEUE_TYPE_CODEL_CONF_VALUE);
  }

  public static boolean isFifoQueueType(final String callQueueType) {
    return callQueueType.equals(CALL_QUEUE_TYPE_FIFO_CONF_VALUE);
  }

  public static boolean isPluggableQueueType(String callQueueType) {
    return callQueueType.equals(CALL_QUEUE_TYPE_PLUGGABLE_CONF_VALUE);
  }

  public static boolean isPluggableQueueWithFastPath(String callQueueType, Configuration conf) {
    return isPluggableQueueType(callQueueType) &&
      conf.getBoolean(PLUGGABLE_CALL_QUEUE_WITH_FAST_PATH_ENABLED, false);
  }

  private Optional<Class<? extends BlockingQueue<CallRunner>>> getPluggableQueueClass() {
    String queueClassName = conf.get(PLUGGABLE_CALL_QUEUE_CLASS_NAME);

    if (queueClassName == null) {
      LOG.error("Pluggable queue class config at " + PLUGGABLE_CALL_QUEUE_CLASS_NAME +
        " was not found");
      return Optional.empty();
    }

    try {
      Class<?> clazz = Class.forName(queueClassName);

      if (BlockingQueue.class.isAssignableFrom(clazz)) {
        return Optional.of((Class<? extends BlockingQueue<CallRunner>>) clazz);
      } else {
        LOG.error("Pluggable Queue class " + queueClassName +
          " does not extend BlockingQueue<CallRunner>");
        return Optional.empty();
      }
    } catch (ClassNotFoundException exception) {
      LOG.error("Could not find " + queueClassName + " on the classpath to load.");
      return Optional.empty();
    }
  }

  public long getNumGeneralCallsDropped() {
    return numGeneralCallsDropped.longValue();
  }

  public long getNumLifoModeSwitches() {
    return numLifoModeSwitches.longValue();
  }

  public int getActiveHandlerCount() {
    return activeHandlerCount.get();
  }

  public int getActiveWriteHandlerCount() {
    return 0;
  }

  public int getActiveReadHandlerCount() {
    return 0;
  }

  public int getActiveScanHandlerCount() {
    return 0;
  }

  /** Returns the length of the pending queue */
  public int getQueueLength() {
    int length = 0;
    for (final BlockingQueue<CallRunner> queue: queues) {
      length += queue.size();
    }
    return length;
  }

  public int getReadQueueLength() {
    return 0;
  }

  public int getScanQueueLength() {
    return 0;
  }

  public int getWriteQueueLength() {
    return 0;
  }

  public String getName() {
    return this.name;
  }

  /**
   * Update current soft limit for executor's call queues
   * @param conf updated configuration
   */
  public void resizeQueues(Configuration conf) {
    String configKey = RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH;
    if (name != null) {
      if (name.toLowerCase(Locale.ROOT).contains("priority")) {
        configKey = RpcScheduler.IPC_SERVER_PRIORITY_MAX_CALLQUEUE_LENGTH;
      } else if (name.toLowerCase(Locale.ROOT).contains("replication")) {
        configKey = RpcScheduler.IPC_SERVER_REPLICATION_MAX_CALLQUEUE_LENGTH;
      }
    }
    final int queueLimit = currentQueueLimit;
    currentQueueLimit = conf.getInt(configKey, queueLimit);
  }

  public void onConfigurationChange(Configuration conf) {
    // update CoDel Scheduler tunables
    int codelTargetDelay = conf.getInt(CALL_QUEUE_CODEL_TARGET_DELAY,
      CALL_QUEUE_CODEL_DEFAULT_TARGET_DELAY);
    int codelInterval = conf.getInt(CALL_QUEUE_CODEL_INTERVAL, CALL_QUEUE_CODEL_DEFAULT_INTERVAL);
    double codelLifoThreshold = conf.getDouble(CALL_QUEUE_CODEL_LIFO_THRESHOLD,
      CALL_QUEUE_CODEL_DEFAULT_LIFO_THRESHOLD);

    for (BlockingQueue<CallRunner> queue : queues) {
      if (queue instanceof AdaptiveLifoCoDelCallQueue) {
        ((AdaptiveLifoCoDelCallQueue) queue).updateTunables(codelTargetDelay, codelInterval,
          codelLifoThreshold);
      } else if (queue instanceof ConfigurationObserver) {
        ((ConfigurationObserver)queue).onConfigurationChange(conf);
      }
    }
  }
}
