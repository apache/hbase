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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.InterProcessLock.MetadataHandler;
import org.apache.hadoop.hbase.InterProcessReadWriteLock;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * ZooKeeper based implementation of {@link InterProcessReadWriteLock}. This lock is fair,
 * not reentrant, and not revocable.
 */
@InterfaceAudience.Private
public class ZKInterProcessReadWriteLock implements InterProcessReadWriteLock {

  private final ZooKeeperWatcher zkWatcher;
  private final String znode;
  private final MetadataHandler handler;

  /**
   * Creates a DistributedReadWriteLock instance.
   * @param zkWatcher
   * @param znode ZNode path for the lock
   * @param handler An object that will handle de-serializing and processing
   *                the metadata associated with reader or writer locks
   *                created by this object or null if none desired.
   */
  public ZKInterProcessReadWriteLock(ZooKeeperWatcher zkWatcher, String znode,
      MetadataHandler handler) {
    this.zkWatcher = zkWatcher;
    this.znode = znode;
    this.handler = handler;
  }

  /**
   * {@inheritDoc}
   */
  public ZKInterProcessReadLock readLock(byte[] metadata) {
    return new ZKInterProcessReadLock(zkWatcher, znode, metadata, handler);
  }

  /**
   * {@inheritDoc}
   */
  public ZKInterProcessWriteLock writeLock(byte[] metadata) {
    return new ZKInterProcessWriteLock(zkWatcher, znode, metadata, handler);
  }
}
