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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * An interface for an application-specific lock.
 */
@InterfaceAudience.Private
public interface InterProcessLock {

  /**
   * Acquire the lock, waiting indefinitely until the lock is released or
   * the thread is interrupted.
   * @throws IOException If there is an unrecoverable error releasing the lock
   * @throws InterruptedException If current thread is interrupted while
   *                              waiting for the lock
   */
  void acquire() throws IOException, InterruptedException;

  /**
   * Acquire the lock within a wait time.
   * @param timeoutMs The maximum time (in milliseconds) to wait for the lock,
   *                  -1 to wait indefinitely
   * @return True if the lock was acquired, false if waiting time elapsed
   *         before the lock was acquired
   * @throws IOException If there is an unrecoverable error talking talking
   *                     (e.g., when talking to a lock service) when acquiring
   *                     the lock
   * @throws InterruptedException If the thread is interrupted while waiting to
   *                              acquire the lock
   */
  boolean tryAcquire(long timeoutMs)
  throws IOException, InterruptedException;

  /**
   * Release the lock.
   * @throws IOException If there is an unrecoverable error releasing the lock
   * @throws InterruptedException If the thread is interrupted while releasing
   *                              the lock
   */
  void release() throws IOException, InterruptedException;

  /**
   * If supported, attempts to reap all the locks of this type by forcefully
   * deleting the locks (both held and attempted) that have expired according
   * to the given timeout. Lock reaping is different than coordinated lock revocation
   * in that, there is no coordination, and the behavior is undefined if the
   * lock holder is still alive.
   * @throws IOException If there is an unrecoverable error reaping the locks
   */
  void reapExpiredLocks(long expireTimeoutMs) throws IOException;

  /**
   * If supported, attempts to reap all the locks of this type by forcefully
   * deleting the locks (both held and attempted). Lock reaping is different
   * than coordinated lock revocation in that, there is no coordination, and
   * the behavior is undefined if the lock holder is still alive.
   * Calling this should have the same affect as calling {@link #reapExpiredLocks(long)}
   * with timeout=0.
   * @throws IOException If there is an unrecoverable error reaping the locks
   */
  void reapAllLocks() throws IOException;

  /**
   * An interface for objects that process lock metadata.
   */
  interface MetadataHandler {

    /**
     * Called after lock metadata is successfully read from a distributed
     * lock service. This method may contain any procedures for, e.g.,
     * printing the metadata in a humanly-readable format.
     * @param metadata The metadata
     */
    void handleMetadata(byte[] metadata);
  }

  /**
   * Visits the locks (both held and attempted) of this type with the given
   * {@link MetadataHandler}.
   * @throws InterruptedException If there is an unrecoverable error
   */
  void visitLocks(MetadataHandler handler) throws IOException;
}
