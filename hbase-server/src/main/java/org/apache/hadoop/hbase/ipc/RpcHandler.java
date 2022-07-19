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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread to handle rpc call. Should only be used in {@link RpcExecutor} and its sub-classes.
 */
@InterfaceAudience.Private
public class RpcHandler extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(RpcHandler.class);

  /**
   * Q to find CallRunners to run in.
   */
  final BlockingQueue<CallRunner> q;

  final int handlerCount;
  final double handlerFailureThreshhold;

  // metrics (shared with other handlers)
  final AtomicInteger activeHandlerCount;
  final AtomicInteger failedHandlerCount;

  // The up-level RpcServer.
  final Abortable abortable;

  private boolean running;

  RpcHandler(final String name, final double handlerFailureThreshhold, final int handlerCount,
    final BlockingQueue<CallRunner> q, final AtomicInteger activeHandlerCount,
    final AtomicInteger failedHandlerCount, final Abortable abortable) {
    super(name);
    setDaemon(true);
    this.q = q;
    this.handlerFailureThreshhold = handlerFailureThreshhold;
    this.activeHandlerCount = activeHandlerCount;
    this.failedHandlerCount = failedHandlerCount;
    this.handlerCount = handlerCount;
    this.abortable = abortable;
  }

  /** Returns A {@link CallRunner} n */
  protected CallRunner getCallRunner() throws InterruptedException {
    return this.q.take();
  }

  public void stopRunning() {
    running = false;
  }

  @Override
  public void run() {
    boolean interrupted = false;
    running = true;
    try {
      while (running) {
        try {
          run(getCallRunner());
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (Exception e) {
      LOG.warn(e.toString(), e);
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
        if (
          this.handlerFailureThreshhold >= 0
            && failedCount > handlerCount * this.handlerFailureThreshhold
        ) {
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
