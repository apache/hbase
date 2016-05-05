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

package org.apache.hadoop.hbase.zookeeper;

import java.util.concurrent.CountDownLatch;

/**
 * Placeholder of an instance which will be accessed by other threads
 * but is not yet created. Thread safe.
 */
class InstancePending<T> {
  // Based on a subtle part of the Java Language Specification,
  // in order to avoid a slight overhead of synchronization for each access.

  private final CountDownLatch pendingLatch = new CountDownLatch(1);

  /** Piggybacking on {@code pendingLatch}. */
  private InstanceHolder<T> instanceHolder;

  private static class InstanceHolder<T> {
    // The JLS ensures the visibility of a final field and its contents
    // unless they are exposed to another thread while the construction.
    final T instance;

    InstanceHolder(T instance) {
      this.instance = instance;
    }
  }

  /**
   * Returns the instance given by the method {@link #prepare}.
   * This is an interruptible blocking method
   * and the interruption flag will be set just before returning if any.
   */
  T get() {
    InstanceHolder<T> instanceHolder;
    boolean interrupted = false;

    while ((instanceHolder = this.instanceHolder) == null) {
      try {
        pendingLatch.await();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return instanceHolder.instance;
  }

  /**
   * Associates the given instance for the method {@link #get}.
   * This method should be called once, and {@code instance} should be non-null.
   * This method is expected to call as soon as possible
   * because the method {@code get} is uninterruptibly blocked until this method is called.
   */
  void prepare(T instance) {
    assert instance != null;
    instanceHolder = new InstanceHolder<T>(instance);
    pendingLatch.countDown();
  }
}
