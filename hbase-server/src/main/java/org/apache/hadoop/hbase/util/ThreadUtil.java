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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class ThreadUtil {

  /**
   * Waits if necessary for the computation to complete, and then retrieves the results of all
   * future objects.
   * @param list of future objects for which the results will be retrieved
   * @return list of computed results
   * @throws InterruptedException if the current thread was interrupted while waiting
   */
  public static <T> List<T> getAllResults(List<Future<T>> futures) throws IOException {
    List<T> results = new ArrayList<T>();
    for (Future<T> future : futures) {
      try {
        T t = future.get();
        if (t != null) {
          results.add(t);
        }
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
    return results;
  }

  /**
   * Blocks until all tasks have completed execution after a shutdown request, or the timeout
   * occurs, or the current thread is interrupted, whichever happens first.
   * @param timeoutInMillis the maximum time to wait
   * @return {@code true} if this executor terminated and {@code false} if the timeout elapsed
   *         before termination
   * @throws InterruptedException if interrupted while waiting
   */
  public static void waitOnShutdown(ExecutorService threadPool, long timeoutInMillis,
    String errMessage) throws IOException {
    try {
      boolean stillRunning = !threadPool.awaitTermination(timeoutInMillis, TimeUnit.MILLISECONDS);
      if (stillRunning) {
        threadPool.shutdownNow();
        // wait for the thread to shutdown completely.
        while (!threadPool.isTerminated()) {
          Thread.sleep(50);
        }
        throw new IOException(errMessage);
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
  }

}
