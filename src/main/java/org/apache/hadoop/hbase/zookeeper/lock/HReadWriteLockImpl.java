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

import org.apache.hadoop.hbase.HLock.MetadataHandler;
import org.apache.hadoop.hbase.HReadWriteLock;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

/**
 * ZooKeeper based implementation of HReadWriteLock.
 */
public class HReadWriteLockImpl implements HReadWriteLock {

  private final ZooKeeperWrapper zkWrapper;
  private final String name;
  private final MetadataHandler handler;

  /**
   * Creates a DistributedReadWriteLock instance.
   * @param zkWrapper
   * @param name (Non-fully qualified) ZNode path for the lock
   * @param handler An object that will handle de-serializing and processing
   *                the metadata associated with reader or writer locks
   *                created by this object or null if none desired.
   */
  public HReadWriteLockImpl(ZooKeeperWrapper zkWrapper, String name,
      MetadataHandler handler) {
    this.zkWrapper = zkWrapper;
    this.name = name;
    this.handler = handler;
  }

  /**
   * {@inheritDoc}
   */
  public HReadLockImpl readLock(byte[] metadata) {
    return new HReadLockImpl(zkWrapper, name, metadata, handler);
  }

  /**
   * {@inheritDoc}
   */
  public HWriteLockImpl writeLock(byte[] metadata) {
    return new HWriteLockImpl(zkWrapper, name, metadata, handler);
  }
}
