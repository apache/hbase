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

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Executor service which allows creation of arbitrary number of serial streams, all to be executed
 * on a single thread pool. All commands scheduled on a single serial stream are executed serially
 * (not necessarily on the same thread).
 */
public interface SerialExecutorService {
  /** Creates new serial stream */
  SerialExecutionStream createStream();

  /**
   * Executor, that makes sure commands are executed serially, in
   * order they are scheduled. No two commands will be running at the same time.
   */
  public interface SerialExecutionStream {
    /**
     * Executes the given command on this stream, returning future.
     * If future command return is not null, next command in the stream
     * will not be executed before future completes.
     *
     * I.e. async commands should return future when it completes,
     * and non-async commands should return null.
     *
     * @return Returns future which is going to be completed when command and
     *   future it returns both finish.
     */
    ListenableFuture<Void> execute(Callable<ListenableFuture<Void>> command);
  }
}
