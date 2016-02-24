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
package org.apache.hadoop.hbase.util;

import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;

public final class FanOutOneBlockAsyncDFSOutputFlushHandler
    implements CompletionHandler<Long, Void> {

  private long size;

  private Throwable error;

  private boolean finished;

  @Override
  public synchronized void completed(Long result, Void attachment) {
    size = result.longValue();
    finished = true;
    notifyAll();
  }

  @Override
  public synchronized void failed(Throwable exc, Void attachment) {
    error = exc;
    finished = true;
    notifyAll();
  }

  public synchronized long get() throws InterruptedException, ExecutionException {
    while (!finished) {
      wait();
    }
    if (error != null) {
      throw new ExecutionException(error);
    }
    return size;
  }

  public void reset() {
    size = 0L;
    error = null;
    finished = false;
  }
}