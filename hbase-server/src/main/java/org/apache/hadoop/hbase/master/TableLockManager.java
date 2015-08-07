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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.InterProcessLock;
import org.apache.hadoop.hbase.InterProcessLock.MetadataHandler;
import org.apache.hadoop.hbase.InterProcessReadWriteLock;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.LockTimeoutException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.zookeeper.lock.ZKInterProcessReadWriteLock;
import org.apache.zookeeper.KeeperException;

/**
 * A manager for distributed table level locks.
 */
@InterfaceAudience.Private
public abstract class TableLockManager {

  private static final Log LOG = LogFactory.getLog(TableLockManager.class);

  /** Configuration key for enabling table-level locks for schema changes */
  public static final String TABLE_LOCK_ENABLE =
    "hbase.table.lock.enable";

  /** by default we should enable table-level locks for schema changes */
  private static final boolean DEFAULT_TABLE_LOCK_ENABLE = true;

  /** Configuration key for time out for trying to acquire table locks */
  protected static final String TABLE_WRITE_LOCK_TIMEOUT_MS =
    "hbase.table.write.lock.timeout.ms";

  /** Configuration key for time out for trying to acquire table locks */
  protected static final String TABLE_READ_LOCK_TIMEOUT_MS =
    "hbase.table.read.lock.timeout.ms";

  protected static final long DEFAULT_TABLE_WRITE_LOCK_TIMEOUT_MS =
    600 * 1000; //10 min default

  protected static final long DEFAULT_TABLE_READ_LOCK_TIMEOUT_MS =
    600 * 1000; //10 min default

  public static final String TABLE_LOCK_EXPIRE_TIMEOUT = "hbase.table.lock.expire.ms";

  public static final long DEFAULT_TABLE_LOCK_EXPIRE_TIMEOUT_MS =
      600 * 1000; //10 min default

  /**
   * A distributed lock for a table.
   */
  @InterfaceAudience.Private
  public interface TableLock {
    /**
     * Acquire the lock, with the configured lock timeout.
     * @throws LockTimeoutException If unable to acquire a lock within a specified
     * time period (if any)
     * @throws IOException If unrecoverable error occurs
     */
    void acquire() throws IOException;

    /**
     * Release the lock already held.
     * @throws IOException If there is an unrecoverable error releasing the lock
     */
    void release() throws IOException;
  }

  /**
   * Returns a TableLock for locking the table for exclusive access
   * @param tableName Table to lock
   * @param purpose Human readable reason for locking the table
   * @return A new TableLock object for acquiring a write lock
   */
  public abstract TableLock writeLock(TableName tableName, String purpose);

  /**
   * Returns a TableLock for locking the table for shared access among read-lock holders
   * @param tableName Table to lock
   * @param purpose Human readable reason for locking the table
   * @return A new TableLock object for acquiring a read lock
   */
  public abstract TableLock readLock(TableName tableName, String purpose);

  /**
   * Visits all table locks(read and write), and lock attempts with the given callback
   * MetadataHandler.
   * @param handler the metadata handler to call
   * @throws IOException If there is an unrecoverable error
   */
  public abstract void visitAllLocks(MetadataHandler handler) throws IOException;

  /**
   * Force releases all table locks(read and write) that have been held longer than
   * "hbase.table.lock.expire.ms". Assumption is that the clock skew between zookeeper
   * and this servers is negligible.
   * The behavior of the lock holders still thinking that they have the lock is undefined.
   * @throws IOException If there is an unrecoverable error
   */
  public abstract void reapAllExpiredLocks() throws IOException;

  /**
   * Force releases table write locks and lock attempts even if this thread does
   * not own the lock. The behavior of the lock holders still thinking that they
   * have the lock is undefined. This should be used carefully and only when
   * we can ensure that all write-lock holders have died. For example if only
   * the master can hold write locks, then we can reap it's locks when the backup
   * master starts.
   * @throws IOException If there is an unrecoverable error
   */
  public abstract void reapWriteLocks() throws IOException;

  /**
   * Called after a table has been deleted, and after the table lock is  released.
   * TableLockManager should do cleanup for the table state.
   * @param tableName name of the table
   * @throws IOException If there is an unrecoverable error releasing the lock
   */
  public abstract void tableDeleted(TableName tableName)
      throws IOException;

  /**
   * Creates and returns a TableLockManager according to the configuration
   */
  public static TableLockManager createTableLockManager(Configuration conf,
      ZooKeeperWatcher zkWatcher, ServerName serverName) {
    // Initialize table level lock manager for schema changes, if enabled.
    if (conf.getBoolean(TABLE_LOCK_ENABLE,
        DEFAULT_TABLE_LOCK_ENABLE)) {
      long writeLockTimeoutMs = conf.getLong(TABLE_WRITE_LOCK_TIMEOUT_MS,
          DEFAULT_TABLE_WRITE_LOCK_TIMEOUT_MS);
      long readLockTimeoutMs = conf.getLong(TABLE_READ_LOCK_TIMEOUT_MS,
          DEFAULT_TABLE_READ_LOCK_TIMEOUT_MS);
      long lockExpireTimeoutMs = conf.getLong(TABLE_LOCK_EXPIRE_TIMEOUT,
          DEFAULT_TABLE_LOCK_EXPIRE_TIMEOUT_MS);

      return new ZKTableLockManager(zkWatcher, serverName, writeLockTimeoutMs, readLockTimeoutMs, lockExpireTimeoutMs);
    }

    return new NullTableLockManager();
  }

  /**
   * A null implementation
   */
  @InterfaceAudience.Private
  public static class NullTableLockManager extends TableLockManager {
    static class NullTableLock implements TableLock {
      @Override
      public void acquire() throws IOException {
      }
      @Override
      public void release() throws IOException {
      }
    }
    @Override
    public TableLock writeLock(TableName tableName, String purpose) {
      return new NullTableLock();
    }
    @Override
    public TableLock readLock(TableName tableName, String purpose) {
      return new NullTableLock();
    }
    @Override
    public void reapAllExpiredLocks() throws IOException {
    }
    @Override
    public void reapWriteLocks() throws IOException {
    }
    @Override
    public void tableDeleted(TableName tableName) throws IOException {
    }
    @Override
    public void visitAllLocks(MetadataHandler handler) throws IOException {
    }
  }

  /** Public for hbck */
  public static ZooKeeperProtos.TableLock fromBytes(byte[] bytes) {
    int pblen = ProtobufUtil.lengthOfPBMagic();
    if (bytes == null || bytes.length < pblen) {
      return null;
    }
    try {
      ZooKeeperProtos.TableLock.Builder builder = ZooKeeperProtos.TableLock.newBuilder();
      ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
      return builder.build();
    } catch (IOException ex) {
      LOG.warn("Exception in deserialization", ex);
    }
    return null;
  }

  /**
   * ZooKeeper based TableLockManager
   */
  @InterfaceAudience.Private
  private static class ZKTableLockManager extends TableLockManager {

    private static final MetadataHandler METADATA_HANDLER = new MetadataHandler() {
      @Override
      public void handleMetadata(byte[] ownerMetadata) {
        if (!LOG.isDebugEnabled()) {
          return;
        }
        ZooKeeperProtos.TableLock data = fromBytes(ownerMetadata);
        if (data == null) {
          return;
        }
        LOG.debug("Table is locked by " +
            String.format("[tableName=%s:%s, lockOwner=%s, threadId=%s, " +
                "purpose=%s, isShared=%s, createTime=%s]",
                data.getTableName().getNamespace().toStringUtf8(),
                data.getTableName().getQualifier().toStringUtf8(),
                ProtobufUtil.toServerName(data.getLockOwner()), data.getThreadId(),
                data.getPurpose(), data.getIsShared(), data.getCreateTime()));
      }
    };

    private static class TableLockImpl implements TableLock {
      long lockTimeoutMs;
      TableName tableName;
      InterProcessLock lock;
      boolean isShared;
      ZooKeeperWatcher zkWatcher;
      ServerName serverName;
      String purpose;

      public TableLockImpl(TableName tableName, ZooKeeperWatcher zkWatcher,
          ServerName serverName, long lockTimeoutMs, boolean isShared, String purpose) {
        this.tableName = tableName;
        this.zkWatcher = zkWatcher;
        this.serverName = serverName;
        this.lockTimeoutMs = lockTimeoutMs;
        this.isShared = isShared;
        this.purpose = purpose;
      }

      @Override
      public void acquire() throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Attempt to acquire table " + (isShared ? "read" : "write") +
            " lock on: " + tableName + " for:" + purpose);
        }

        lock = createTableLock();
        try {
          if (lockTimeoutMs == -1) {
            // Wait indefinitely
            lock.acquire();
          } else {
            if (!lock.tryAcquire(lockTimeoutMs)) {
              throw new LockTimeoutException("Timed out acquiring " +
                (isShared ? "read" : "write") + "lock for table:" + tableName +
                "for:" + purpose + " after " + lockTimeoutMs + " ms.");
            }
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted acquiring a lock for " + tableName, e);
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Interrupted acquiring a lock");
        }
        if (LOG.isTraceEnabled()) LOG.trace("Acquired table " + (isShared ? "read" : "write")
            + " lock on " + tableName + " for " + purpose);
      }

      @Override
      public void release() throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Attempt to release table " + (isShared ? "read" : "write")
              + " lock on " + tableName);
        }
        if (lock == null) {
          throw new IllegalStateException("Table " + tableName +
            " is not locked!");
        }

        try {
          lock.release();
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while releasing a lock for " + tableName);
          throw new InterruptedIOException();
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Released table lock on " + tableName);
        }
      }

      private InterProcessLock createTableLock() {
        String tableLockZNode = ZKUtil.joinZNode(zkWatcher.tableLockZNode,
            tableName.getNameAsString());

        ZooKeeperProtos.TableLock data = ZooKeeperProtos.TableLock.newBuilder()
          .setTableName(ProtobufUtil.toProtoTableName(tableName))
          .setLockOwner(ProtobufUtil.toServerName(serverName))
          .setThreadId(Thread.currentThread().getId())
          .setPurpose(purpose)
          .setIsShared(isShared)
          .setCreateTime(EnvironmentEdgeManager.currentTime()).build();
        byte[] lockMetadata = toBytes(data);

        InterProcessReadWriteLock lock = new ZKInterProcessReadWriteLock(zkWatcher, tableLockZNode,
          METADATA_HANDLER);
        return isShared ? lock.readLock(lockMetadata) : lock.writeLock(lockMetadata);
      }
    }

    private static byte[] toBytes(ZooKeeperProtos.TableLock data) {
      return ProtobufUtil.prependPBMagic(data.toByteArray());
    }

    private final ServerName serverName;
    private final ZooKeeperWatcher zkWatcher;
    private final long writeLockTimeoutMs;
    private final long readLockTimeoutMs;
    private final long lockExpireTimeoutMs;

    /**
     * Initialize a new manager for table-level locks.
     * @param zkWatcher
     * @param serverName Address of the server responsible for acquiring and
     * releasing the table-level locks
     * @param writeLockTimeoutMs Timeout (in milliseconds) for acquiring a write lock for a
     * given table, or -1 for no timeout
     * @param readLockTimeoutMs Timeout (in milliseconds) for acquiring a read lock for a
     * given table, or -1 for no timeout
     */
    public ZKTableLockManager(ZooKeeperWatcher zkWatcher,
      ServerName serverName, long writeLockTimeoutMs, long readLockTimeoutMs, long lockExpireTimeoutMs) {
      this.zkWatcher = zkWatcher;
      this.serverName = serverName;
      this.writeLockTimeoutMs = writeLockTimeoutMs;
      this.readLockTimeoutMs = readLockTimeoutMs;
      this.lockExpireTimeoutMs = lockExpireTimeoutMs;
    }

    @Override
    public TableLock writeLock(TableName tableName, String purpose) {
      return new TableLockImpl(tableName, zkWatcher,
          serverName, writeLockTimeoutMs, false, purpose);
    }

    public TableLock readLock(TableName tableName, String purpose) {
      return new TableLockImpl(tableName, zkWatcher,
          serverName, readLockTimeoutMs, true, purpose);
    }

    public void visitAllLocks(MetadataHandler handler) throws IOException {
      for (String tableName : getTableNames()) {
        String tableLockZNode = ZKUtil.joinZNode(zkWatcher.tableLockZNode, tableName);
        ZKInterProcessReadWriteLock lock = new ZKInterProcessReadWriteLock(
            zkWatcher, tableLockZNode, null);
        lock.readLock(null).visitLocks(handler);
        lock.writeLock(null).visitLocks(handler);
      }
    }

    private List<String> getTableNames() throws IOException {

      List<String> tableNames;
      try {
        tableNames = ZKUtil.listChildrenNoWatch(zkWatcher, zkWatcher.tableLockZNode);
      } catch (KeeperException e) {
        LOG.error("Unexpected ZooKeeper error when listing children", e);
        throw new IOException("Unexpected ZooKeeper exception", e);
      }
      return tableNames;
    }

    @Override
    public void reapWriteLocks() throws IOException {
      //get the table names
      try {
        for (String tableName : getTableNames()) {
          String tableLockZNode = ZKUtil.joinZNode(zkWatcher.tableLockZNode, tableName);
          ZKInterProcessReadWriteLock lock = new ZKInterProcessReadWriteLock(
              zkWatcher, tableLockZNode, null);
          lock.writeLock(null).reapAllLocks();
        }
      } catch (IOException ex) {
        throw ex;
      } catch (Exception ex) {
        LOG.warn("Caught exception while reaping table write locks", ex);
      }
    }

    @Override
    public void reapAllExpiredLocks() throws IOException {
      //get the table names
      try {
        for (String tableName : getTableNames()) {
          String tableLockZNode = ZKUtil.joinZNode(zkWatcher.tableLockZNode, tableName);
          ZKInterProcessReadWriteLock lock = new ZKInterProcessReadWriteLock(
              zkWatcher, tableLockZNode, null);
          lock.readLock(null).reapExpiredLocks(lockExpireTimeoutMs);
          lock.writeLock(null).reapExpiredLocks(lockExpireTimeoutMs);
        }
      } catch (IOException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new IOException(ex);
      }
    }

    @Override
    public void tableDeleted(TableName tableName) throws IOException {
      //table write lock from DeleteHandler is already released, just delete the parent znode
      String tableNameStr = tableName.getNameAsString();
      String tableLockZNode = ZKUtil.joinZNode(zkWatcher.tableLockZNode, tableNameStr);
      try {
        ZKUtil.deleteNode(zkWatcher, tableLockZNode);
      } catch (KeeperException ex) {
        if (ex.code() == KeeperException.Code.NOTEMPTY) {
          //we might get this in rare occasions where a CREATE table or some other table operation
          //is waiting to acquire the lock. In this case, parent znode won't be deleted.
          LOG.warn("Could not delete the znode for table locks because NOTEMPTY: "
              + tableLockZNode);
          return;
        }
        throw new IOException(ex);
      }
    }
  }
}
