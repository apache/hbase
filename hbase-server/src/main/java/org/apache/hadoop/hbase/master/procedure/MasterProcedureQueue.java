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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureFairRunQueues;
import org.apache.hadoop.hbase.procedure2.ProcedureRunnableSet;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface.TableOperationType;

/**
 * ProcedureRunnableSet for the Master Procedures.
 * This RunnableSet tries to provide to the ProcedureExecutor procedures
 * that can be executed without having to wait on a lock.
 * Most of the master operations can be executed concurrently, if they
 * are operating on different tables (e.g. two create table can be performed
 * at the same, time assuming table A and table B) or against two different servers; say
 * two servers that crashed at about the same time.
 *
 * <p>Each procedure should implement an interface providing information for this queue.
 * for example table related procedures should implement TableProcedureInterface.
 * each procedure will be pushed in its own queue, and based on the operation type
 * we may take smarter decision. e.g. we can abort all the operations preceding
 * a delete table, or similar.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterProcedureQueue implements ProcedureRunnableSet {
  private static final Log LOG = LogFactory.getLog(MasterProcedureQueue.class);

  // Two queues to ensure that server procedures run ahead of table precedures always.
  private final ProcedureFairRunQueues<TableName, RunQueue> tableFairQ;
  /**
   * Rely on basic fair q. ServerCrashProcedure will yield if meta is not assigned. This way, the
   * server that was carrying meta should rise to the top of the queue (this is how it used to
   * work when we had handlers and ServerShutdownHandler ran). TODO: special handling of servers
   * that were carrying system tables on crash; do I need to have these servers have priority?
   *
   * <p>Apart from the special-casing of meta and system tables, fairq is what we want
   */
  private final ProcedureFairRunQueues<ServerName, RunQueue> serverFairQ;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();
  private final TableLockManager lockManager;

  private final int metaTablePriority;
  private final int userTablePriority;
  private final int sysTablePriority;
  private static final int DEFAULT_SERVER_PRIORITY = 1;

  /**
   * Keeps count across server and table queues.
   */
  private int queueSize;

  public MasterProcedureQueue(final Configuration conf, final TableLockManager lockManager) {
    this.tableFairQ = new ProcedureFairRunQueues<TableName, RunQueue>(1);
    this.serverFairQ = new ProcedureFairRunQueues<ServerName, RunQueue>(1);
    this.lockManager = lockManager;

    // TODO: should this be part of the HTD?
    metaTablePriority = conf.getInt("hbase.master.procedure.queue.meta.table.priority", 3);
    sysTablePriority = conf.getInt("hbase.master.procedure.queue.system.table.priority", 2);
    userTablePriority = conf.getInt("hbase.master.procedure.queue.user.table.priority", 1);
  }

  @Override
  public void addFront(final Procedure proc) {
    lock.lock();
    try {
      getRunQueueOrCreate(proc).addFront(proc);
      queueSize++;
      waitCond.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addBack(final Procedure proc) {
    lock.lock();
    try {
      getRunQueueOrCreate(proc).addBack(proc);
      queueSize++;
      waitCond.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void yield(final Procedure proc) {
    addBack(proc);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("WA_AWAIT_NOT_IN_LOOP")
  public Long poll() {
    Long pollResult = null;
    lock.lock();
    try {
      if (queueSize == 0) {
        waitCond.await();
        if (queueSize == 0) {
          return null;
        }
      }
      // For now, let server handling have precedence over table handling; presumption is that it
      // is more important handling crashed servers than it is running the
      // enabling/disabling tables, etc.
      pollResult = doPoll(serverFairQ.poll());
      if (pollResult == null) {
        pollResult = doPoll(tableFairQ.poll());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      lock.unlock();
    }
    return pollResult;
  }

  private Long doPoll(final RunQueue rq) {
    if (rq == null || !rq.isAvailable()) return null;
    this.queueSize--;
    return rq.poll();
  }

  @Override
  public void signalAll() {
    lock.lock();
    try {
      waitCond.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() {
    lock.lock();
    try {
      serverFairQ.clear();
      tableFairQ.clear();
      queueSize = 0;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return queueSize;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    lock.lock();
    try {
      return "MasterProcedureQueue size=" + queueSize + ": tableFairQ: " + tableFairQ +
        ", serverFairQ: " + serverFairQ;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void completionCleanup(Procedure proc) {
    if (proc instanceof TableProcedureInterface) {
      TableProcedureInterface iProcTable = (TableProcedureInterface)proc;
      boolean tableDeleted;
      if (proc.hasException()) {
        IOException procEx =  proc.getException().unwrapRemoteException();
        if (iProcTable.getTableOperationType() == TableOperationType.CREATE) {
          // create failed because the table already exist
          tableDeleted = !(procEx instanceof TableExistsException);
        } else {
          // the operation failed because the table does not exist
          tableDeleted = (procEx instanceof TableNotFoundException);
        }
      } else {
        // the table was deleted
        tableDeleted = (iProcTable.getTableOperationType() == TableOperationType.DELETE);
      }
      if (tableDeleted) {
        markTableAsDeleted(iProcTable.getTableName());
      }
    }
    // No cleanup for ServerProcedureInterface types, yet.
  }

  private RunQueue getRunQueueOrCreate(final Procedure proc) {
    if (proc instanceof TableProcedureInterface) {
      final TableName table = ((TableProcedureInterface)proc).getTableName();
      return getRunQueueOrCreate(table);
    }
    if (proc instanceof ServerProcedureInterface) {
      return getRunQueueOrCreate((ServerProcedureInterface)proc);
    }
    // TODO: at the moment we only have Table and Server procedures
    // if you are implementing a non-table/non-server procedure, you have two options: create
    // a group for all the non-table/non-server procedures or try to find a key for your
    // non-table/non-server procedures and implement something similar to the TableRunQueue.
    throw new UnsupportedOperationException("RQs for non-table procedures are not implemented yet");
  }

  private TableRunQueue getRunQueueOrCreate(final TableName table) {
    final TableRunQueue queue = getRunQueue(table);
    if (queue != null) return queue;
    return (TableRunQueue)tableFairQ.add(table, createTableRunQueue(table));
  }

  private ServerRunQueue getRunQueueOrCreate(final ServerProcedureInterface spi) {
    final ServerRunQueue queue = getRunQueue(spi.getServerName());
    if (queue != null) return queue;
    return (ServerRunQueue)serverFairQ.add(spi.getServerName(), createServerRunQueue(spi));
  }

  private TableRunQueue createTableRunQueue(final TableName table) {
    int priority = userTablePriority;
    if (table.equals(TableName.META_TABLE_NAME)) {
      priority = metaTablePriority;
    } else if (table.isSystemTable()) {
      priority = sysTablePriority;
    }
    return new TableRunQueue(priority);
  }

  private ServerRunQueue createServerRunQueue(final ServerProcedureInterface spi) {
    return new ServerRunQueue(DEFAULT_SERVER_PRIORITY);
  }

  private TableRunQueue getRunQueue(final TableName table) {
    return (TableRunQueue)tableFairQ.get(table);
  }

  private ServerRunQueue getRunQueue(final ServerName sn) {
    return (ServerRunQueue)serverFairQ.get(sn);
  }

  /**
   * Try to acquire the write lock on the specified table.
   * other operations in the table-queue will be executed after the lock is released.
   * @param table Table to lock
   * @param purpose Human readable reason for locking the table
   * @return true if we were able to acquire the lock on the table, otherwise false.
   */
  public boolean tryAcquireTableExclusiveLock(final TableName table, final String purpose) {
    return getRunQueueOrCreate(table).tryExclusiveLock(lockManager, table, purpose);
  }

  /**
   * Release the write lock taken with tryAcquireTableWrite()
   * @param table the name of the table that has the write lock
   */
  public void releaseTableExclusiveLock(final TableName table) {
    getRunQueue(table).releaseExclusiveLock(lockManager, table);
  }

  /**
   * Try to acquire the read lock on the specified table.
   * other read operations in the table-queue may be executed concurrently,
   * otherwise they have to wait until all the read-locks are released.
   * @param table Table to lock
   * @param purpose Human readable reason for locking the table
   * @return true if we were able to acquire the lock on the table, otherwise false.
   */
  public boolean tryAcquireTableSharedLock(final TableName table, final String purpose) {
    return getRunQueueOrCreate(table).trySharedLock(lockManager, table, purpose);
  }

  /**
   * Release the read lock taken with tryAcquireTableRead()
   * @param table the name of the table that has the read lock
   */
  public void releaseTableSharedLock(final TableName table) {
    getRunQueue(table).releaseSharedLock(lockManager, table);
  }

  /**
   * Try to acquire the write lock on the specified server.
   * @see #releaseServerExclusiveLock(ServerProcedureInterface)
   * @param spi Server to lock
   * @return true if we were able to acquire the lock on the server, otherwise false.
   */
  public boolean tryAcquireServerExclusiveLock(final ServerProcedureInterface spi) {
    return getRunQueueOrCreate(spi).tryExclusiveLock();
  }

  /**
   * Release the write lock
   * @see #tryAcquireServerExclusiveLock(ServerProcedureInterface)
   * @param spi the server that has the write lock
   */
  public void releaseServerExclusiveLock(final ServerProcedureInterface spi) {
    getRunQueue(spi.getServerName()).releaseExclusiveLock();
  }

  /**
   * Try to acquire the read lock on the specified server.
   * @see #releaseServerSharedLock(ServerProcedureInterface)
   * @param spi Server to lock
   * @return true if we were able to acquire the lock on the server, otherwise false.
   */
  public boolean tryAcquireServerSharedLock(final ServerProcedureInterface spi) {
    return getRunQueueOrCreate(spi).trySharedLock();
  }

  /**
   * Release the read lock taken
   * @see #tryAcquireServerSharedLock(ServerProcedureInterface)
   * @param spi the server that has the read lock
   */
  public void releaseServerSharedLock(final ServerProcedureInterface spi) {
    getRunQueue(spi.getServerName()).releaseSharedLock();
  }

  /**
   * Tries to remove the queue and the table-lock of the specified table.
   * If there are new operations pending (e.g. a new create),
   * the remove will not be performed.
   * @param table the name of the table that should be marked as deleted
   * @return true if deletion succeeded, false otherwise meaning that there are
   *    other new operations pending for that table (e.g. a new create).
   */
  protected boolean markTableAsDeleted(final TableName table) {
    TableRunQueue queue = getRunQueue(table);
    if (queue != null) {
      lock.lock();
      try {
        if (queue.isEmpty() && queue.acquireDeleteLock()) {
          tableFairQ.remove(table);

          // Remove the table lock
          try {
            lockManager.tableDeleted(table);
          } catch (IOException e) {
            LOG.warn("Received exception from TableLockManager.tableDeleted:", e); //not critical
          }
        } else {
          // TODO: If there are no create, we can drop all the other ops
          return false;
        }
      } finally {
        lock.unlock();
      }
    }
    return true;
  }

  private interface RunQueue extends ProcedureFairRunQueues.FairObject {
    void addFront(Procedure proc);
    void addBack(Procedure proc);
    Long poll();
    boolean acquireDeleteLock();
  }

  /**
   * Base abstract class for RunQueue implementations.
   * Be careful honoring synchronizations in subclasses. In here we protect access but if you are
   * acting on a state found in here, be sure dependent code keeps synchronization.
   * Implements basic in-memory read/write locking mechanism to prevent procedure steps being run
   * in parallel.
   */
  private static abstract class AbstractRunQueue implements RunQueue {
    // All modification of runnables happens with #lock held.
    private final Deque<Long> runnables = new ArrayDeque<Long>();
    private final int priority;
    private boolean exclusiveLock = false;
    private int sharedLock = 0;

    public AbstractRunQueue(int priority) {
      this.priority = priority;
    }

    boolean isEmpty() {
      return this.runnables.isEmpty();
    }

    @Override
    public boolean isAvailable() {
      synchronized (this) {
        return !exclusiveLock && !runnables.isEmpty();
      }
    }

    @Override
    public int getPriority() {
      return this.priority;
    }

    @Override
    public void addFront(Procedure proc) {
      this.runnables.addFirst(proc.getProcId());
    }

    @Override
    public void addBack(Procedure proc) {
      this.runnables.addLast(proc.getProcId());
    }

    @Override
    public Long poll() {
      return this.runnables.poll();
    }

    @Override
    public synchronized boolean acquireDeleteLock() {
      return tryExclusiveLock();
    }

    public synchronized boolean isLocked() {
      return isExclusiveLock() || sharedLock > 0;
    }

    public synchronized boolean isExclusiveLock() {
      return this.exclusiveLock;
    }

    public synchronized boolean trySharedLock() {
      if (isExclusiveLock()) return false;
      sharedLock++;
      return true;
    }

    public synchronized void releaseSharedLock() {
      sharedLock--;
    }

    /**
     * @return True if only one instance of a shared lock outstanding.
     */
    synchronized boolean isSingleSharedLock() {
      return sharedLock == 1;
    }

    public synchronized boolean tryExclusiveLock() {
      if (isLocked()) return false;
      exclusiveLock = true;
      return true;
    }

    public synchronized void releaseExclusiveLock() {
      exclusiveLock = false;
    }

    @Override
    public String toString() {
      return this.runnables.toString();
    }
  }

  /**
   * Run Queue for Server procedures.
   */
  private static class ServerRunQueue extends AbstractRunQueue {
    public ServerRunQueue(int priority) {
      super(priority);
    }
  }

  /**
   * Run Queue for a Table. It contains a read-write lock that is used by the
   * MasterProcedureQueue to decide if we should fetch an item from this queue
   * or skip to another one which will be able to run without waiting for locks.
   */
  private static class TableRunQueue extends AbstractRunQueue {
    private TableLock tableLock = null;

    public TableRunQueue(int priority) {
      super(priority);
    }

    // TODO: Improve run-queue push with TableProcedureInterface.getType()
    //       we can take smart decisions based on the type of the operation (e.g. create/delete)
    @Override
    public void addBack(final Procedure proc) {
      super.addBack(proc);
    }

    public synchronized boolean trySharedLock(final TableLockManager lockManager,
        final TableName tableName, final String purpose) {
      if (isExclusiveLock()) return false;

      // Take zk-read-lock
      tableLock = lockManager.readLock(tableName, purpose);
      try {
        tableLock.acquire();
      } catch (IOException e) {
        LOG.error("failed acquire read lock on " + tableName, e);
        tableLock = null;
        return false;
      }
      trySharedLock();
      return true;
    }

    public synchronized void releaseSharedLock(final TableLockManager lockManager,
        final TableName tableName) {
      releaseTableLock(lockManager, isSingleSharedLock());
      releaseSharedLock();
    }

    public synchronized boolean tryExclusiveLock(final TableLockManager lockManager,
        final TableName tableName, final String purpose) {
      if (isLocked()) return false;
      // Take zk-write-lock
      tableLock = lockManager.writeLock(tableName, purpose);
      try {
        tableLock.acquire();
      } catch (IOException e) {
        LOG.error("failed acquire write lock on " + tableName, e);
        tableLock = null;
        return false;
      }
      tryExclusiveLock();
      return true;
    }

    public synchronized void releaseExclusiveLock(final TableLockManager lockManager,
        final TableName tableName) {
      releaseTableLock(lockManager, true);
      releaseExclusiveLock();
    }

    private void releaseTableLock(final TableLockManager lockManager, boolean reset) {
      for (int i = 0; i < 3; ++i) {
        try {
          tableLock.release();
          if (reset) {
            tableLock = null;
          }
          break;
        } catch (IOException e) {
          LOG.warn("Could not release the table write-lock", e);
        }
      }
    }
  }
}
