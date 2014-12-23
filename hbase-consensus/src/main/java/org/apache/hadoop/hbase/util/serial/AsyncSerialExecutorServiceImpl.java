package org.apache.hadoop.hbase.util.serial;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Library that can be used when we want to schedule many async events but such that execution of a
 * certain event depends on completing a previous event. One doesn't need to know on which thread
 * the execution will happen - since the next free thread from the pool will be chosen and the
 * constraint will be preserved.
 */
public class AsyncSerialExecutorServiceImpl implements SerialExecutorService {
  static final Log LOG = LogFactory.getLog(AsyncSerialExecutorServiceImpl.class);

  private final Executor executorService;

  public AsyncSerialExecutorServiceImpl(int numThreads, String poolName) {
    this.executorService =
        Executors.newFixedThreadPool(numThreads, new DaemonThreadFactory(poolName));
  }

  public AsyncSerialExecutorServiceImpl(Executor executorService) {
    this.executorService = executorService;
  }

  @Override
  public SerialExecutionStream createStream() {
    return new SerialExecutionStream() {
      /**
       * Future that completes when last scheduled action finishes, representing last
       * future in the stream/list.
       * So, each future needs to have only one listener attached to it.
       * We enforce that by accessing lastExecuted only through getAndSet method,
       * which guarantees each future is both stored and returned by this AtomicReference
       * only once.
       *
       * At start it has already been finished, so first command doesn't have to wait.
       */
      private AtomicReference<ListenableFuture<Void>> lastExecuted = new AtomicReference<>(
          Futures.<Void>immediateFuture(null));

      @Override
      public ListenableFuture<Void> execute(Callable<ListenableFuture<Void>> command) {
        SettableFuture<Void> commandDone = SettableFuture.create();
        ListenableFuture<Void> last = lastExecuted.getAndSet(commandDone);
        last.addListener(new CommandRunAndCompleteCurrent(command, commandDone), executorService);
        return commandDone;
      }
    };
  }

  /**
   * Wrapper around runnable and settable future
   */
  private class CommandRunAndCompleteCurrent implements Runnable {
    private Callable<ListenableFuture<Void>> command;
    private SettableFuture<Void> currentFuture;

    public CommandRunAndCompleteCurrent(Callable<ListenableFuture<Void>> command, SettableFuture<Void> currentFuture) {
      this.command = command;
      this.currentFuture = currentFuture;
    }

    @Override
    public void run() {
      try {
        ListenableFuture<Void> commandFuture = command.call();
        if (commandFuture == null) {
          currentFuture.set(null);
        } else {
          Futures.addCallback(commandFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
              currentFuture.set(null);
            }
            @Override
            public void onFailure(Throwable t) {
              LOG.error("exception in the command future", t);
              currentFuture.set(null);
            }
          }, executorService);
        }
      } catch (Exception e) {
        // Log exception
        LOG.error("exception while executing command", e);
        currentFuture.set(null);
      }
    }
  }
}
