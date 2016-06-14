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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
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

  protected volatile int currentQueueLimit;

  private final AtomicInteger activeHandlerCount = new AtomicInteger(0);
  private final List<Handler> handlers;
  private final int handlerCount;
  private final String name;
  private final AtomicInteger failedHandlerCount = new AtomicInteger(0);

  private boolean running;

  private Configuration conf = null;
  private Abortable abortable = null;

  public RpcExecutor(final String name, final int handlerCount) {
    this.handlers = new ArrayList<Handler>(handlerCount);
    this.handlerCount = handlerCount;
    this.name = Strings.nullToEmpty(name);
  }

  public RpcExecutor(final String name, final int handlerCount, final Configuration conf,
      final Abortable abortable) {
    this(name, handlerCount);
    this.conf = conf;
    this.abortable = abortable;
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

  public int getActiveHandlerCount() {
    return activeHandlerCount.get();
  }

  /** Returns the length of the pending queue */
  public abstract int getQueueLength();

  /** Add the request to the executor queue */
  public abstract boolean dispatch(final CallRunner callTask) throws InterruptedException;

  /** Returns the list of request queues */
  protected abstract List<BlockingQueue<CallRunner>> getQueues();

  protected void startHandlers(final int port) {
    List<BlockingQueue<CallRunner>> callQueues = getQueues();
    startHandlers(null, handlerCount, callQueues, 0, callQueues.size(), port);
  }

  /**
   * Override if providing alternate Handler implementation.
   */
  protected Handler getHandler(final String name, final double handlerFailureThreshhold,
      final BlockingQueue<CallRunner> q) {
    return new Handler(name, handlerFailureThreshhold, q);
  }

  /**
   * Start up our handlers.
   */
  protected void startHandlers(final String nameSuffix, final int numHandlers,
      final List<BlockingQueue<CallRunner>> callQueues,
      final int qindex, final int qsize, final int port) {
    final String threadPrefix = name + Strings.nullToEmpty(nameSuffix);
    double handlerFailureThreshhold =
        conf == null ? 1.0 : conf.getDouble(HConstants.REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT,
          HConstants.DEFAULT_REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT);
    for (int i = 0; i < numHandlers; i++) {
      final int index = qindex + (i % qsize);
      String name = "RpcServer." + threadPrefix + ".handler=" + handlers.size() + ",queue=" +
          index + ",port=" + port;
      Handler handler = getHandler(name, handlerFailureThreshhold, callQueues.get(index));
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

    Handler(final String name, final double handlerFailureThreshhold,
        final BlockingQueue<CallRunner> q) {
      super(name);
      setDaemon(true);
      this.q = q;
      this.handlerFailureThreshhold = handlerFailureThreshhold;
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
        activeHandlerCount.incrementAndGet();
        cr.run();
      } catch (Throwable e) {
        if (e instanceof Error) {
          int failedCount = failedHandlerCount.incrementAndGet();
          if (this.handlerFailureThreshhold >= 0 &&
              failedCount > handlerCount * this.handlerFailureThreshhold) {
            String message = "Number of failed RpcServer handler runs exceeded threshhold " +
              this.handlerFailureThreshhold + "; reason: " + StringUtils.stringifyException(e);
            if (abortable != null) {
              abortable.abort(message, e);
            } else {
              LOG.error("Error but can't abort because abortable is null: " +
                  StringUtils.stringifyException(e));
              throw e;
            }
          } else {
            LOG.warn("Handler errors " + StringUtils.stringifyException(e));
          }
        } else {
          LOG.warn("Handler  exception " + StringUtils.stringifyException(e));
        }
      } finally {
        activeHandlerCount.decrementAndGet();
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
   * Update current soft limit for executor's call queues
   * @param conf updated configuration
   */
  public void resizeQueues(Configuration conf) {
    String configKey = RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH;
    if (name != null && name.toLowerCase().contains("priority")) {
      configKey = RpcScheduler.IPC_SERVER_PRIORITY_MAX_CALLQUEUE_LENGTH;
    }
    currentQueueLimit = conf.getInt(configKey, currentQueueLimit);
  }
}