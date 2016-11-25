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

package org.apache.hadoop.hbase.ipc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.util.BoundedPriorityBlockingQueue;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Runs the CallRunners passed here via {@link #dispatch(CallRunner)}. Subclass and add particular
 * scheduling behavior.
 */
@InterfaceAudience.Private
public abstract class RpcExecutor {
  private static final Log LOG = LogFactory.getLog(RpcExecutor.class);

  protected static final int DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT = 250;
  public static final String CALL_QUEUE_HANDLER_FACTOR_CONF_KEY = "hbase.ipc.server.callqueue.handler.factor";

  /** max delay in msec used to bound the deprioritized requests */
  public static final String QUEUE_MAX_CALL_DELAY_CONF_KEY = "hbase.ipc.server.queue.max.call.delay";

  /**
   * The default, 'fifo', has the least friction but is dumb. If set to 'deadline', uses a priority
   * queue and deprioritizes long-running scans. Sorting by priority comes at a cost, reduced
   * throughput.
   */
  public static final String CALL_QUEUE_TYPE_CODEL_CONF_VALUE = "codel";
  public static final String CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE = "deadline";
  public static final String CALL_QUEUE_TYPE_FIFO_CONF_VALUE = "fifo";
  public static final String CALL_QUEUE_TYPE_CONF_KEY = "hbase.ipc.server.callqueue.type";
  public static final String CALL_QUEUE_TYPE_CONF_DEFAULT = CALL_QUEUE_TYPE_FIFO_CONF_VALUE;

  // These 3 are only used by Codel executor
  public static final String CALL_QUEUE_CODEL_TARGET_DELAY = "hbase.ipc.server.callqueue.codel.target.delay";
  public static final String CALL_QUEUE_CODEL_INTERVAL = "hbase.ipc.server.callqueue.codel.interval";
  public static final String CALL_QUEUE_CODEL_LIFO_THRESHOLD = "hbase.ipc.server.callqueue.codel.lifo.threshold";

  public static final int CALL_QUEUE_CODEL_DEFAULT_TARGET_DELAY = 100;
  public static final int CALL_QUEUE_CODEL_DEFAULT_INTERVAL = 100;
  public static final double CALL_QUEUE_CODEL_DEFAULT_LIFO_THRESHOLD = 0.8;

  private AtomicLong numGeneralCallsDropped = new AtomicLong();
  private AtomicLong numLifoModeSwitches = new AtomicLong();

  protected final int numCallQueues;
  protected final List<BlockingQueue<CallRunner>> queues;
  private final Class<? extends BlockingQueue> queueClass;
  private final Object[] queueInitArgs;

  private final PriorityFunction priority;

  protected volatile int currentQueueLimit;

  private final AtomicInteger activeHandlerCount = new AtomicInteger(0);
  private final List<Handler> handlers;
  private final int handlerCount;
  private final AtomicInteger failedHandlerCount = new AtomicInteger(0);

  private String name;
  private boolean running;

  private Configuration conf = null;
  private Abortable abortable = null;

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

    float callQueuesHandlersFactor = this.conf.getFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 0);
    this.numCallQueues = computeNumCallQueues(handlerCount, callQueuesHandlersFactor);
    this.queues = new ArrayList<>(this.numCallQueues);

    this.handlerCount = Math.max(handlerCount, this.numCallQueues);
    this.handlers = new ArrayList<>(this.handlerCount);

    this.priority = priority;

    if (isDeadlineQueueType(callQueueType)) {
      this.name += ".Deadline";
      this.queueInitArgs = new Object[] { maxQueueLength,
        new CallPriorityComparator(conf, this.priority) };
      this.queueClass = BoundedPriorityBlockingQueue.class;
    } else if (isCodelQueueType(callQueueType)) {
      this.name += ".Codel";
      int codelTargetDelay = conf.getInt(CALL_QUEUE_CODEL_TARGET_DELAY,
        CALL_QUEUE_CODEL_DEFAULT_TARGET_DELAY);
      int codelInterval = conf.getInt(CALL_QUEUE_CODEL_INTERVAL, CALL_QUEUE_CODEL_DEFAULT_INTERVAL);
      double codelLifoThreshold = conf.getDouble(CALL_QUEUE_CODEL_LIFO_THRESHOLD,
        CALL_QUEUE_CODEL_DEFAULT_LIFO_THRESHOLD);
      queueInitArgs = new Object[] { maxQueueLength, codelTargetDelay, codelInterval,
          codelLifoThreshold, numGeneralCallsDropped, numLifoModeSwitches };
      queueClass = AdaptiveLifoCoDelCallQueue.class;
    } else {
      this.name += ".Fifo";
      queueInitArgs = new Object[] { maxQueueLength };
      queueClass = LinkedBlockingQueue.class;
    }

    LOG.info("RpcExecutor " + " name " + " using " + callQueueType
        + " as call queue; numCallQueues=" + numCallQueues + "; maxQueueLength=" + maxQueueLength
        + "; handlerCount=" + handlerCount);
  }

  protected int computeNumCallQueues(final int handlerCount, final float callQueuesHandlersFactor) {
    return Math.max(1, (int) Math.round(handlerCount * callQueuesHandlersFactor));
  }

  protected void initializeQueues(final int numQueues) {
    if (queueInitArgs.length > 0) {
      currentQueueLimit = (int) queueInitArgs[0];
      queueInitArgs[0] = Math.max((int) queueInitArgs[0], DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT);
    }
    for (int i = 0; i < numQueues; ++i) {
      queues
          .add((BlockingQueue<CallRunner>) ReflectionUtils.newInstance(queueClass, queueInitArgs));
    }
  }

  public void start(final int port) {
    running = true;
    startHandlers(port);
  }

  public void stop() {
    running = false;
    for (Thread handler : handlers) {
      handler.interrupt();
    }
  }

  /** Add the request to the executor queue */
  public abstract boolean dispatch(final CallRunner callTask) throws InterruptedException;

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
  protected Handler getHandler(final String name, final double handlerFailureThreshhold,
      final BlockingQueue<CallRunner> q, final AtomicInteger activeHandlerCount) {
    return new Handler(name, handlerFailureThreshhold, q, activeHandlerCount);
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
      Handler handler = getHandler(name, handlerFailureThreshhold, callQueues.get(index),
        activeHandlerCount);
      handler.start();
      LOG.debug("Started " + name);
      handlers.add(handler);
    }
  }

  /**
   * Handler thread run the {@link CallRunner#run()} in.
   */
  protected class Handler extends Thread {
    /**
     * Q to find CallRunners to run in.
     */
    final BlockingQueue<CallRunner> q;

    final double handlerFailureThreshhold;

    // metrics (shared with other handlers)
    final AtomicInteger activeHandlerCount;

    Handler(final String name, final double handlerFailureThreshhold,
        final BlockingQueue<CallRunner> q, final AtomicInteger activeHandlerCount) {
      super(name);
      setDaemon(true);
      this.q = q;
      this.handlerFailureThreshhold = handlerFailureThreshhold;
      this.activeHandlerCount = activeHandlerCount;
    }

    /**
     * @return A {@link CallRunner}
     * @throws InterruptedException
     */
    protected CallRunner getCallRunner() throws InterruptedException {
      return this.q.take();
    }

    @Override
    public void run() {
      boolean interrupted = false;
      try {
        while (running) {
          try {
            run(getCallRunner());
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } catch (Exception e) {
        LOG.warn(e);
        throw e;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private void run(CallRunner cr) {
      MonitoredRPCHandler status = RpcServer.getStatus();
      cr.setStatus(status);
      try {
        this.activeHandlerCount.incrementAndGet();
        cr.run();
      } catch (Throwable e) {
        if (e instanceof Error) {
          int failedCount = failedHandlerCount.incrementAndGet();
          if (this.handlerFailureThreshhold >= 0
              && failedCount > handlerCount * this.handlerFailureThreshhold) {
            String message = "Number of failed RpcServer handler runs exceeded threshhold "
                + this.handlerFailureThreshhold + "; reason: " + StringUtils.stringifyException(e);
            if (abortable != null) {
              abortable.abort(message, e);
            } else {
              LOG.error("Error but can't abort because abortable is null: "
                  + StringUtils.stringifyException(e));
              throw e;
            }
          } else {
            LOG.warn("Handler errors " + StringUtils.stringifyException(e));
          }
        } else {
          LOG.warn("Handler  exception " + StringUtils.stringifyException(e));
        }
      } finally {
        this.activeHandlerCount.decrementAndGet();
      }
    }
  }

  public static abstract class QueueBalancer {
    /**
     * @return the index of the next queue to which a request should be inserted
     */
    public abstract int getNextQueue();
  }

  public static QueueBalancer getBalancer(int queueSize) {
    Preconditions.checkArgument(queueSize > 0, "Queue size is <= 0, must be at least 1");
    if (queueSize == 1) {
      return ONE_QUEUE;
    } else {
      return new RandomQueueBalancer(queueSize);
    }
  }

  /**
   * All requests go to the first queue, at index 0
   */
  private static QueueBalancer ONE_QUEUE = new QueueBalancer() {
    @Override
    public int getNextQueue() {
      return 0;
    }
  };

  /**
   * Queue balancer that just randomly selects a queue in the range [0, num queues).
   */
  private static class RandomQueueBalancer extends QueueBalancer {
    private final int queueSize;

    public RandomQueueBalancer(int queueSize) {
      this.queueSize = queueSize;
    }

    public int getNextQueue() {
      return ThreadLocalRandom.current().nextInt(queueSize);
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
      RpcServer.Call callA = a.getCall();
      RpcServer.Call callB = b.getCall();
      long deadlineA = priority.getDeadline(callA.getHeader(), callA.param);
      long deadlineB = priority.getDeadline(callB.getHeader(), callB.param);
      deadlineA = callA.timestamp + Math.min(deadlineA, maxDelay);
      deadlineB = callB.timestamp + Math.min(deadlineB, maxDelay);
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

  public long getNumGeneralCallsDropped() {
    return numGeneralCallsDropped.get();
  }

  public long getNumLifoModeSwitches() {
    return numLifoModeSwitches.get();
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
    if (name != null && name.toLowerCase(Locale.ROOT).contains("priority")) {
      configKey = RpcScheduler.IPC_SERVER_PRIORITY_MAX_CALLQUEUE_LENGTH;
    }
    currentQueueLimit = conf.getInt(configKey, currentQueueLimit);
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
      }
    }
  }
}
