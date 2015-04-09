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
 * Most of the master operations can be executed concurrently, if the they
 * are operating on different tables (e.g. two create table can be performed
 * at the same, time assuming table A and table B).
 *
 * Each procedure should implement an interface providing information for this queue.
 * for example table related procedures should implement TableProcedureInterface.
 * each procedure will be pushed in its own queue, and based on the operation type
 * we may take smarter decision. e.g. we can abort all the operations preceding
 * a delete table, or similar.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterProcedureQueue implements ProcedureRunnableSet {
  private static final Log LOG = LogFactory.getLog(MasterProcedureQueue.class);

  private final ProcedureFairRunQueues<TableName, RunQueue> fairq;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();
  private final TableLockManager lockManager;

  private final int metaTablePriority;
  private final int userTablePriority;
  private final int sysTablePriority;

  private int queueSize;

  public MasterProcedureQueue(final Configuration conf, final TableLockManager lockManager) {
    this.fairq = new ProcedureFairRunQueues<TableName, RunQueue>(1);
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
    addFront(proc);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("WA_AWAIT_NOT_IN_LOOP")
  public Long poll() {
    lock.lock();
    try {
      if (queueSize == 0) {
        waitCond.await();
        if (queueSize == 0) {
          return null;
        }
      }

      RunQueue queue = fairq.poll();
      if (queue != null && queue.isAvailable()) {
        queueSize--;
        return queue.poll();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } finally {
      lock.unlock();
    }
    return null;
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
      fairq.clear();
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
      return "MasterProcedureQueue size=" + queueSize + ": " + fairq;
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
  }

  private RunQueue getRunQueueOrCreate(final Procedure proc) {
    if (proc instanceof TableProcedureInterface) {
      final TableName table = ((TableProcedureInterface)proc).getTableName();
      return getRunQueueOrCreate(table);
    }
    // TODO: at the moment we only have Table procedures
    // if you are implementing a non-table procedure, you have two option create
    // a group for all the non-table procedures or try to find a key for your
    // non-table procedure and implement something similar to the TableRunQueue.
    throw new UnsupportedOperationException("RQs for non-table procedures are not implemented yet");
  }

  private TableRunQueue getRunQueueOrCreate(final TableName table) {
    final TableRunQueue queue = getRunQueue(table);
    if (queue != null) return queue;
    return (TableRunQueue)fairq.add(table, createTableRunQueue(table));
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

  private TableRunQueue getRunQueue(final TableName table) {
    return (TableRunQueue)fairq.get(table);
  }

  /**
   * Try to acquire the read lock on the specified table.
   * other read operations in the table-queue may be executed concurrently,
   * otherwise they have to wait until all the read-locks are released.
   * @param table Table to lock
   * @param purpose Human readable reason for locking the table
   * @return true if we were able to acquire the lock on the table, otherwise false.
   */
  public boolean tryAcquireTableRead(final TableName table, final String purpose) {
    return getRunQueueOrCreate(table).tryRead(lockManager, table, purpose);
  }

  /**
   * Release the read lock taken with tryAcquireTableRead()
   * @param table the name of the table that has the read lock
   */
  public void releaseTableRead(final TableName table) {
    getRunQueue(table).releaseRead(lockManager, table);
  }

  /**
   * Try to acquire the write lock on the specified table.
   * other operations in the table-queue will be executed after the lock is released.
   * @param table Table to lock
   * @param purpose Human readable reason for locking the table
   * @return true if we were able to acquire the lock on the table, otherwise false.
   */
  public boolean tryAcquireTableWrite(final TableName table, final String purpose) {
    return getRunQueueOrCreate(table).tryWrite(lockManager, table, purpose);
  }

  /**
   * Release the write lock taken with tryAcquireTableWrite()
   * @param table the name of the table that has the write lock
   */
  public void releaseTableWrite(final TableName table) {
    getRunQueue(table).releaseWrite(lockManager, table);
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
        if (queue.isEmpty() && !queue.isLocked()) {
          fairq.remove(table);

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
    boolean isLocked();
  }

  /**
   * Run Queue for a Table. It contains a read-write lock that is used by the
   * MasterProcedureQueue to decide if we should fetch an item from this queue
   * or skip to another one which will be able to run without waiting for locks.
   */
  private static class TableRunQueue implements RunQueue {
    private final Deque<Long> runnables = new ArrayDeque<Long>();
    private final int priority;

    private TableLock tableLock = null;
    private boolean wlock = false;
    private int rlock = 0;

    public TableRunQueue(int priority) {
      this.priority = priority;
    }

    @Override
    public void addFront(final Procedure proc) {
      runnables.addFirst(proc.getProcId());
    }

    // TODO: Improve run-queue push with TableProcedureInterface.getType()
    //       we can take smart decisions based on the type of the operation (e.g. create/delete)
    @Override
    public void addBack(final Procedure proc) {
      runnables.addLast(proc.getProcId());
    }

    @Override
    public Long poll() {
      return runnables.poll();
    }

    @Override
    public boolean isAvailable() {
      synchronized (this) {
        return !wlock && !runnables.isEmpty();
      }
    }

    public boolean isEmpty() {
      return runnables.isEmpty();
    }

    @Override
    public boolean isLocked() {
      synchronized (this) {
        return wlock || rlock > 0;
      }
    }

    public boolean tryRead(final TableLockManager lockManager,
        final TableName tableName, final String purpose) {
      synchronized (this) {
        if (wlock) {
          return false;
        }

        // Take zk-read-lock
        tableLock = lockManager.readLock(tableName, purpose);
        try {
          tableLock.acquire();
        } catch (IOException e) {
          LOG.error("failed acquire read lock on " + tableName, e);
          tableLock = null;
          return false;
        }

        rlock++;
      }
      return true;
    }

    public void releaseRead(final TableLockManager lockManager,
        final TableName tableName) {
      synchronized (this) {
        releaseTableLock(lockManager, rlock == 1);
        rlock--;
      }
    }

    public boolean tryWrite(final TableLockManager lockManager,
        final TableName tableName, final String purpose) {
      synchronized (this) {
        if (wlock || rlock > 0) {
          return false;
        }

        // Take zk-write-lock
        tableLock = lockManager.writeLock(tableName, purpose);
        try {
          tableLock.acquire();
        } catch (IOException e) {
          LOG.error("failed acquire write lock on " + tableName, e);
          tableLock = null;
          return false;
        }
        wlock = true;
      }
      return true;
    }

    public void releaseWrite(final TableLockManager lockManager,
        final TableName tableName) {
      synchronized (this) {
        releaseTableLock(lockManager, true);
        wlock = false;
      }
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

    @Override
    public int getPriority() {
      return priority;
    }

    @Override
    public String toString() {
      return runnables.toString();
    }
  }
}
