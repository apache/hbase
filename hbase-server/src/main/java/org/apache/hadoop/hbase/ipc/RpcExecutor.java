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

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RpcExecutor {
  private static final Log LOG = LogFactory.getLog(RpcExecutor.class);

  private final AtomicInteger activeHandlerCount = new AtomicInteger(0);
  private final List<Thread> handlers;
  private final int handlerCount;
  private final String name;

  private boolean running;

  public RpcExecutor(final String name, final int handlerCount) {
    this.handlers = new ArrayList<Thread>(handlerCount);
    this.handlerCount = handlerCount;
    this.name = Strings.nullToEmpty(name);
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
    try {
      while (running) {
        try {
          CallRunner task = myQueue.take();
          try {
            activeHandlerCount.incrementAndGet();
            task.run();
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
}
