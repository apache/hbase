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

import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Balanced queue executor with a fastpath. Because this is FIFO, it has no respect for
 * ordering so a fast path skipping the queuing of Calls if an Handler is available, is possible.
 * Just pass the Call direct to waiting Handler thread. Try to keep the hot Handlers bubbling
 * rather than let them go cold and lose context. Idea taken from Apace Kudu (incubating). See
 * https://gerrit.cloudera.org/#/c/2938/7/src/kudu/rpc/service_queue.h
 */
@InterfaceAudience.Private
public class FastPathBalancedQueueRpcExecutor extends BalancedQueueRpcExecutor {
  // Depends on default behavior of BalancedQueueRpcExecutor being FIFO!

  /*
   * Stack of Handlers waiting for work.
   */
  private final Deque<FastPathHandler> fastPathHandlerStack = new ConcurrentLinkedDeque<>();

  public FastPathBalancedQueueRpcExecutor(final String name, final int handlerCount,
      final int maxQueueLength, final PriorityFunction priority, final Configuration conf,
      final Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);

  }

  public FastPathBalancedQueueRpcExecutor(final String name, final int handlerCount,
      final String callQueueType, final int maxQueueLength, final PriorityFunction priority,
      final Configuration conf, final Abortable abortable) {
    super(name, handlerCount, callQueueType, maxQueueLength, priority, conf, abortable);
  }

  @Override
  protected Handler getHandler(String name, double handlerFailureThreshhold,
      BlockingQueue<CallRunner> q, AtomicInteger activeHandlerCount) {
    return new FastPathHandler(name, handlerFailureThreshhold, q, activeHandlerCount,
        fastPathHandlerStack);
  }

  @Override
  public boolean dispatch(CallRunner callTask) throws InterruptedException {
    FastPathHandler handler = popReadyHandler();
    return handler != null? handler.loadCallRunner(callTask): super.dispatch(callTask);
  }

  /**
   * @return Pop a Handler instance if one available ready-to-go or else return null.
   */
  private FastPathHandler popReadyHandler() {
    return this.fastPathHandlerStack.poll();
  }

  class FastPathHandler extends Handler {
    // Below are for fast-path support. Push this Handler on to the fastPathHandlerStack Deque
    // if an empty queue of CallRunners so we are available for direct handoff when one comes in.
    final Deque<FastPathHandler> fastPathHandlerStack;
    // Semaphore to coordinate loading of fastpathed loadedTask and our running it.
    private Semaphore semaphore = new Semaphore(0);
    // The task we get when fast-pathing.
    private CallRunner loadedCallRunner;

    FastPathHandler(String name, double handlerFailureThreshhold, BlockingQueue<CallRunner> q,
        final AtomicInteger activeHandlerCount,
        final Deque<FastPathHandler> fastPathHandlerStack) {
      super(name, handlerFailureThreshhold, q, activeHandlerCount);
      this.fastPathHandlerStack = fastPathHandlerStack;
    }

    protected CallRunner getCallRunner() throws InterruptedException {
      // Get a callrunner if one in the Q.
      CallRunner cr = this.q.poll();
      if (cr == null) {
        // Else, if a fastPathHandlerStack present and no callrunner in Q, register ourselves for
        // the fastpath handoff done via fastPathHandlerStack.
        if (this.fastPathHandlerStack != null) {
          this.fastPathHandlerStack.push(this);
          this.semaphore.acquire();
          cr = this.loadedCallRunner;
          this.loadedCallRunner = null;
        } else {
          // No fastpath available. Block until a task comes available.
          cr = super.getCallRunner();
        }
      }
      return cr;
    }

    /**
     * @param task Task gotten via fastpath.
     * @return True if we successfully loaded our task
     */
    boolean loadCallRunner(final CallRunner cr) {
      this.loadedCallRunner = cr;
      this.semaphore.release();
      return true;
    }
  }
}
