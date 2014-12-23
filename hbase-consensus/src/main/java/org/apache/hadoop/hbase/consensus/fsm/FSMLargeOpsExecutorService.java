package org.apache.hadoop.hbase.consensus.fsm;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * This is an executor service, which has a thread pool to execute IO heavy
 * parts of state transition code (the {@link State#onEntry(Event)} methods).
 */
public class FSMLargeOpsExecutorService {
  public static ListeningExecutorService fsmWriteOpsExecutorService = null;
  public static ListeningExecutorService fsmReadOpsExecutorService = null;

  public static synchronized void initialize(Configuration config) {
    if (fsmWriteOpsExecutorService == null) {
      fsmWriteOpsExecutorService = createWriteOpsExecutorService(config);
    }

    if (fsmReadOpsExecutorService == null) {
      fsmReadOpsExecutorService = createReadOpsExecutorService(config);
    }
  }

  /**
   * This method should only be used for testing
   */
  public static synchronized void initializeForTesting(
    ExecutorService writeOpsService, ExecutorService readOpsService) {
    fsmWriteOpsExecutorService = MoreExecutors.listeningDecorator(writeOpsService);
    fsmReadOpsExecutorService = MoreExecutors.listeningDecorator(readOpsService);
  }

  public static ListeningExecutorService createWriteOpsExecutorService(
    Configuration conf) {
    return MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(
      conf.getInt(HConstants.FSM_WRITEOPS_THREADPOOL_SIZE_KEY,
        HConstants.FSM_WRITEOPS_THREADPOOL_SIZE_DEFAULT),
      new DaemonThreadFactory("fsmWriteOpsExecutor")));
  }

  public static ListeningExecutorService createReadOpsExecutorService(
    Configuration conf) {
    return MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(
      conf.getInt(HConstants.FSM_READOPS_THREADPOOL_SIZE_KEY,
        HConstants.FSM_READOPS_THREADPOOL_SIZE_DEFAULT),
      new DaemonThreadFactory("fsmReadOpsExecutor")));
  }

  private static ListenableFuture<?> submit(ListeningExecutorService executorService, Runnable r) {
    // We should be calling initialize() in the RaftQuorumContext constructor,
    // but just in case we don't initialize the thread pool with the defaults.
    if (executorService == null) {
      initialize(new Configuration());
    }
    return executorService.submit(r);
  }

  public static ListenableFuture<?> submitToWriteOpsThreadPool(Runnable r) {
    return submit(fsmWriteOpsExecutorService, r);
  }

  public static ListenableFuture<?>  submitToReadOpsThreadPool(Runnable r) {
    return submit(fsmReadOpsExecutorService, r);
  }

  private static ListenableFuture<?> submit(ListeningExecutorService executorService, Callable<?> c) {
    // We should be calling initialize() in the RaftQuorumContext constructor,
    // but just in case we don't initialize the thread pool with the defaults.
    if (executorService == null) {
      initialize(new Configuration());
    }
    return executorService.submit(c);
  }

  public static ListenableFuture<?> submitToWriteOpsThreadPool(Callable<?> c) {
    return submit(fsmWriteOpsExecutorService, c);
  }

  public static ListenableFuture<?>  submitToReadOpsThreadPool(Callable<?> c) {
    return submit(fsmReadOpsExecutorService, c);
  }
}
