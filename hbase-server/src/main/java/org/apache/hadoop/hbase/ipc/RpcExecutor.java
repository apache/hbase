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
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RpcExecutor {
  private static final Log LOG = LogFactory.getLog(RpcExecutor.class);

  private final AtomicInteger activeHandlerCount = new AtomicInteger(0);
  private final List<Thread> handlers;
  private final int handlerCount;
  private final String name;
  private final AtomicInteger failedHandlerCount = new AtomicInteger(0);

  private boolean running;

  private Configuration conf = null;
  private Abortable abortable = null;

  public RpcExecutor(final String name, final int handlerCount) {
    this.handlers = new ArrayList<Thread>(handlerCount);
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
  public abstract void dispatch(final CallRunner callTask) throws InterruptedException;

  /** Returns the list of request queues */
  protected abstract List<BlockingQueue<CallRunner>> getQueues();

  protected void startHandlers(final int port) {
    List<BlockingQueue<CallRunner>> callQueues = getQueues();
    startHandlers(null, handlerCount, callQueues, 0, callQueues.size(), port);
  }

  protected void startHandlers(final String nameSuffix, final int numHandlers,
      final List<BlockingQueue<CallRunner>> callQueues,
      final int qindex, final int qsize, final int port) {
    final String threadPrefix = name + Strings.nullToEmpty(nameSuffix);
    for (int i = 0; i < numHandlers; i++) {
      final int index = qindex + (i % qsize);
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          consumerLoop(callQueues.get(index));
        }
      });
      t.setDaemon(true);
      t.setName(threadPrefix + "RpcServer.handler=" + handlers.size() +
          ",queue=" + index + ",port=" + port);
      t.start();
      LOG.debug(threadPrefix + " Start Handler index=" + handlers.size() + " queue=" + index);
      handlers.add(t);
    }
  }

  protected void consumerLoop(final BlockingQueue<CallRunner> myQueue) {
    boolean interrupted = false;
    double handlerFailureThreshhold =
        conf == null ? 1.0 : conf.getDouble(HConstants.REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT,
          HConstants.DEFAULT_REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT);
    try {
      while (running) {
        try {
          CallRunner task = myQueue.take();
          try {
            activeHandlerCount.incrementAndGet();
            task.run();
          } catch (Error e) {
            int failedCount = failedHandlerCount.incrementAndGet();
            if (handlerFailureThreshhold >= 0
                && failedCount > handlerCount * handlerFailureThreshhold) {
              String message =
                  "Number of failed RpcServer handler exceeded threshhold "
                      + handlerFailureThreshhold + "  with failed reason: "
                      + StringUtils.stringifyException(e);
              if (abortable != null) {
                abortable.abort(message, e);
              } else {
                LOG.error("Received " + StringUtils.stringifyException(e)
                  + " but not aborting due to abortable being null");
                throw e;
              }
            } else {
              LOG.warn("RpcServer handler threads encountered errors "
                  + StringUtils.stringifyException(e));
            }
          } finally {
            activeHandlerCount.decrementAndGet();
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
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

    private final ThreadLocal<Random> threadRandom =
      new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
          return new Random();
        }
      };

    public RandomQueueBalancer(int queueSize) {
      this.queueSize = queueSize;
    }

    public int getNextQueue() {
      return threadRandom.get().nextInt(queueSize);
    }
  }
}
