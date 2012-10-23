/**
 * Copyright The Apache Software Foundation
 *
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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Utilities for multi-threaded unit tests.
 */
public class MultiThreadedTestUtils {

  /**
   * Verify that no assertions have failed inside a future.
   * Used for unit tests that spawn threads. E.g.,
   * <p>
   * <code>
   *   List<Future<Void>> results = Lists.newArrayList();
   *   Future<Void> f = executor.submit(new Callable<Void> {
   *     public Void call() {
   *       assertTrue(someMethod());
   *     }
   *   });
   *   results.add(f);
   *   assertOnFutures(results);
   * </code>
   * @param threadResults A list of futures
   * @param <T>
   * @throws InterruptedException If interrupted when waiting for a result
   *                              from one of the futures
   * @throws ExecutionException If an exception other than AssertionError
   *                            occurs inside any of the futures
   */
  public static <T> void assertOnFutures(List<Future<T>> threadResults)
  throws InterruptedException, ExecutionException {
    for (Future<T> threadResult : threadResults) {
      try {
        threadResult.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof AssertionError) {
          throw (AssertionError) e.getCause();
        }
        throw e;
      }
    }
  }
}
