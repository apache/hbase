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
package org.apache.hadoop.hbase.zookeeper.lock;

/**
 * Represents information about an acquired lock. Used by BaseDistributedLock.
 */
public class AcquiredLock {

  private final String path;
  private final int version;

  /**
   * Store information about a lock.
   * @param path The path to a lock's ZNode
   * @param version The current version of the lock's ZNode
   */
  public AcquiredLock(String path, int version) {
    this.path = path;
    this.version = version;
  }

  public String getPath() {
    return path;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "AcquiredLockInfo{" +
        "path='" + path + '\'' +
        ", version=" + version +
        '}';
  }
}
