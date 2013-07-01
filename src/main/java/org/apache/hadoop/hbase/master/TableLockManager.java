/*
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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HLock;
import org.apache.hadoop.hbase.HLock.MetadataHandler;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.TableLockTimeoutException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.hbase.zookeeper.lock.HReadWriteLockImpl;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A manager for distributed table level locks.
 */
public class TableLockManager {

  private static final Log LOG = LogFactory.getLog(TableLockManager.class);

  private static final MetadataHandler METADATA_HANDLER =
    new MetadataHandler() {
      @Override
      public void handleMetadata(byte[] ownerMetadata) {
        LOG.info("Table is locked: " + Bytes.toString(ownerMetadata));
      }
    };

  /**
   * Tables that are currently locked by this instance. Allows locks to
   * be released by table name.
   */
  private final ConcurrentMap<String, HLock> acquiredTableLocks;

  private final HServerAddress serverAddress;

  private final ZooKeeperWrapper zkWrapper;

  private final int lockTimeoutMs;

  /**
   * Initialize a new manager for table-level locks.
   * @param zkWrapper
   * @param serverAddress Address of the server responsible for acquiring and
   *                      releasing the table-level locks
   * @param lockTimeoutMs Timeout (in milliseconds) for acquiring a lock for a
   *                      given table, or -1 for no timeout
   */
  public TableLockManager(ZooKeeperWrapper zkWrapper,
    HServerAddress serverAddress, int lockTimeoutMs) {
    this.zkWrapper = zkWrapper;
    this.serverAddress = serverAddress;
    this.lockTimeoutMs = lockTimeoutMs;
    this.acquiredTableLocks =
        new ConcurrentHashMap<String, HLock>();
  }

  /**
   * Try to lock a table, given a purpose.
   * @param tableName Table to lock
   * @param purpose Human readable reason for locking the table
   * @return True if the table was locked before the timeout false otherwise
   * @throws IOException If unrecoverable ZooKeeper error occurs
   */
  public boolean tryLockTable(byte[] tableName, String purpose, long timeout)
      throws IOException {
    boolean result;
    String tableNameStr = Bytes.toString(tableName);
    HLock lock = createTableLock(tableNameStr, purpose);
    try {
      result = lock.tryAcquire(timeout);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted acquiring a lock for " + tableNameStr, e);
      throw new InterruptedIOException("Interrupted acquiring a lock");
    }

    if (result &&
        acquiredTableLocks.putIfAbsent(tableNameStr, lock) != null) {
      // This should never execute if DistributedLock is implemented
      // correctly.
      LOG.error("Lock for " + tableNameStr + " acquired by multiple owners!");
      LOG.error("Currently held locks: " + acquiredTableLocks);
      throw new IllegalStateException("Lock for " + tableNameStr +
          " was acquired by multiple owners!");
    }

    return result;
  }

  /**
   * Lock a table, given a purpose.
   * @param tableName Table to lock
   * @param purpose Human readable reason for locking the table
   * @throws IOException If unrecoverable ZooKeeper error occurs
   */
  public void lockTable(byte[] tableName, String purpose)
  throws IOException {
    long waitIndefinitely = -1;
    tryLockTable(tableName, purpose, waitIndefinitely);
  }

  private HLock createTableLock(String tableName, String purpose) {
    String tableLockZNode = zkWrapper.getZNode(zkWrapper.tableLockZNode,
      tableName);
    byte[] lockMetadata = Bytes.toBytes("[Table = " + tableName +
      "\nOwner server address = " + serverAddress +
      "\nOwner thread id = " + Thread.currentThread().getId() +
      "\nPurpose = " + purpose + "]");
    return new HReadWriteLockImpl(zkWrapper, tableLockZNode,
        METADATA_HANDLER).writeLock(lockMetadata);
  }

  public void unlockTable(byte[] tableName)
  throws IOException {
    String tableNameStr = Bytes.toString(tableName);
    HLock lock = acquiredTableLocks.get(tableNameStr);
    if (lock == null) {
      throw new IllegalStateException("Table " + tableNameStr +
        " is not locked!");
    }

    try {
      acquiredTableLocks.remove(tableNameStr);
      lock.release();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while releasing a lock for " + tableNameStr);
      Thread.currentThread().interrupt();
      throw new InterruptedIOException();
    }
  }

  /**
   * Used to determine if a table is currently locked from this table lock manager.
   *
   * @param tableName
   * @return True if the table was locked by this table manager false otherwise.
   */
  public boolean isTableLocked(byte[] tableName) {
    return acquiredTableLocks.containsKey(Bytes.toString(tableName));
  }
}
