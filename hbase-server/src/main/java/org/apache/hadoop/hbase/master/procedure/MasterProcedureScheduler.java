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
 * WITHOUTKey WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface.TableOperationType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureRunnableSet;

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
public class MasterProcedureScheduler implements ProcedureRunnableSet {
  private static final Log LOG = LogFactory.getLog(MasterProcedureScheduler.class);

  private final TableLockManager lockManager;
  private final ReentrantLock schedLock = new ReentrantLock();
  private final Condition schedWaitCond = schedLock.newCondition();

  private final FairQueue<ServerName> serverRunQueue = new FairQueue<ServerName>();
  private final FairQueue<TableName> tableRunQueue = new FairQueue<TableName>();
  private int queueSize = 0;

  private final Object[] serverBuckets = new Object[128];
  private Queue<String> namespaceMap = null;
  private Queue<TableName> tableMap = null;

  private final int metaTablePriority;
  private final int userTablePriority;
  private final int sysTablePriority;

  // TODO: metrics
  private long pollCalls = 0;
  private long nullPollCalls = 0;

  public MasterProcedureScheduler(final Configuration conf, final TableLockManager lockManager) {
    this.lockManager = lockManager;

    // TODO: should this be part of the HTD?
    metaTablePriority = conf.getInt("hbase.master.procedure.queue.meta.table.priority", 3);
    sysTablePriority = conf.getInt("hbase.master.procedure.queue.system.table.priority", 2);
    userTablePriority = conf.getInt("hbase.master.procedure.queue.user.table.priority", 1);
  }

  @Override
  public void addFront(Procedure proc) {
    doAdd(proc, true);
  }

  @Override
  public void addBack(Procedure proc) {
    doAdd(proc, false);
  }

  @Override
  public void yield(final Procedure proc) {
    doAdd(proc, isTableProcedure(proc));
  }

  private void doAdd(final Procedure proc, final boolean addFront) {
    schedLock.lock();
    try {
      if (isTableProcedure(proc)) {
        doAdd(tableRunQueue, getTableQueue(getTableName(proc)), proc, addFront);
      } else if (isServerProcedure(proc)) {
        doAdd(serverRunQueue, getServerQueue(getServerName(proc)), proc, addFront);
      } else {
        // TODO: at the moment we only have Table and Server procedures
        // if you are implementing a non-table/non-server procedure, you have two options: create
        // a group for all the non-table/non-server procedures or try to find a key for your
        // non-table/non-server procedures and implement something similar to the TableRunQueue.
        throw new UnsupportedOperationException(
          "RQs for non-table/non-server procedures are not implemented yet");
      }
      schedWaitCond.signal();
    } finally {
      schedLock.unlock();
    }
  }

  private <T extends Comparable<T>> void doAdd(final FairQueue<T> fairq,
      final Queue<T> queue, final Procedure proc, final boolean addFront) {
    queue.add(proc, addFront);
    if (!(queue.isSuspended() || queue.hasExclusiveLock())) {
      if (queue.size() == 1 && !IterableList.isLinked(queue)) {
        fairq.add(queue);
      }
      queueSize++;
    }
  }

  @Override
  public Procedure poll() {
    return poll(-1);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("WA_AWAIT_NOT_IN_LOOP")
  Procedure poll(long waitNsec) {
    Procedure pollResult = null;
    schedLock.lock();
    try {
      if (queueSize == 0) {
        if (waitNsec < 0) {
          schedWaitCond.await();
        } else {
          schedWaitCond.awaitNanos(waitNsec);
        }
        if (queueSize == 0) {
          return null;
        }
      }

      // For now, let server handling have precedence over table handling; presumption is that it
      // is more important handling crashed servers than it is running the
      // enabling/disabling tables, etc.
      pollResult = doPoll(serverRunQueue);
      if (pollResult == null) {
        pollResult = doPoll(tableRunQueue);
      }

      // update metrics
      pollCalls++;
      nullPollCalls += (pollResult == null) ? 1 : 0;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      schedLock.unlock();
    }
    return pollResult;
  }

  private <T extends Comparable<T>> Procedure doPoll(final FairQueue<T> fairq) {
    Queue<T> rq = fairq.poll();
    if (rq == null || !rq.isAvailable()) {
      return null;
    }

    assert !rq.isSuspended() : "rq=" + rq + " is suspended";
    Procedure pollResult = rq.poll();
    this.queueSize--;
    if (rq.isEmpty() || rq.requireExclusiveLock(pollResult)) {
      removeFromRunQueue(fairq, rq);
    }
    return pollResult;
  }

  @Override
  public void clear() {
    // NOTE: USED ONLY FOR TESTING
    schedLock.lock();
    try {
      // Remove Servers
      for (int i = 0; i < serverBuckets.length; ++i) {
        clear((ServerQueue)serverBuckets[i], serverRunQueue);
        serverBuckets[i] = null;
      }

      // Remove Tables
      clear(tableMap, tableRunQueue);
      tableMap = null;

      assert queueSize == 0 : "expected queue size to be 0, got " + queueSize;
    } finally {
      schedLock.unlock();
    }
  }

  private <T extends Comparable<T>> void clear(Queue<T> treeMap, FairQueue<T> fairq) {
    while (treeMap != null) {
      Queue<T> node = AvlTree.getFirst(treeMap);
      assert !node.isSuspended() : "can't clear suspended " + node.getKey();
      treeMap = AvlTree.remove(treeMap, node.getKey());
      removeFromRunQueue(fairq, node);
    }
  }

  @Override
  public void signalAll() {
    schedLock.lock();
    try {
      schedWaitCond.signalAll();
    } finally {
      schedLock.unlock();
    }
  }

  @Override
  public int size() {
    schedLock.lock();
    try {
      return queueSize;
    } finally {
      schedLock.unlock();
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
        return;
      }
    } else {
      // No cleanup for ServerProcedureInterface types, yet.
      return;
    }
  }

  private <T extends Comparable<T>> void addToRunQueue(FairQueue<T> fairq, Queue<T> queue) {
    if (IterableList.isLinked(queue)) return;
    if (!queue.isEmpty())  {
      fairq.add(queue);
      queueSize += queue.size();
    }
  }

  private <T extends Comparable<T>> void removeFromRunQueue(FairQueue<T> fairq, Queue<T> queue) {
    if (!IterableList.isLinked(queue)) return;
    fairq.remove(queue);
    queueSize -= queue.size();
  }

  // ============================================================================
  //  TODO: Metrics
  // ============================================================================
  public long getPollCalls() {
    return pollCalls;
  }

  public long getNullPollCalls() {
    return nullPollCalls;
  }

  // ============================================================================
  //  Event Helpers
  // ============================================================================
  public boolean waitEvent(ProcedureEvent event, Procedure procedure) {
    return waitEvent(event, procedure, false);
  }

  public boolean waitEvent(ProcedureEvent event, Procedure procedure, boolean suspendQueue) {
    synchronized (event) {
      if (event.isReady()) {
        return false;
      }

      // TODO: Suspend single procedure not implemented yet, fallback to suspending the queue
      if (!suspendQueue) suspendQueue = true;

      if (isTableProcedure(procedure)) {
        waitTableEvent(event, procedure, suspendQueue);
      } else if (isServerProcedure(procedure)) {
        waitServerEvent(event, procedure, suspendQueue);
      } else {
        // TODO: at the moment we only have Table and Server procedures
        // if you are implementing a non-table/non-server procedure, you have two options: create
        // a group for all the non-table/non-server procedures or try to find a key for your
        // non-table/non-server procedures and implement something similar to the TableRunQueue.
        throw new UnsupportedOperationException(
          "RQs for non-table/non-server procedures are not implemented yet");
      }
    }
    return true;
  }

  private void waitTableEvent(ProcedureEvent event, Procedure procedure, boolean suspendQueue) {
    final TableName tableName = getTableName(procedure);
    final boolean isDebugEnabled = LOG.isDebugEnabled();

    schedLock.lock();
    try {
      TableQueue queue = getTableQueue(tableName);
      if (queue.isSuspended()) return;

      // TODO: if !suspendQueue

      if (isDebugEnabled) {
        LOG.debug("Suspend table queue " + tableName);
      }
      queue.setSuspended(true);
      removeFromRunQueue(tableRunQueue, queue);
      event.suspendTableQueue(queue);
    } finally {
      schedLock.unlock();
    }
  }

  private void waitServerEvent(ProcedureEvent event, Procedure procedure, boolean suspendQueue) {
    final ServerName serverName = getServerName(procedure);
    final boolean isDebugEnabled = LOG.isDebugEnabled();

    schedLock.lock();
    try {
      // TODO: This will change once we have the new AM
      ServerQueue queue = getServerQueue(serverName);
      if (queue.isSuspended()) return;

      // TODO: if !suspendQueue

      if (isDebugEnabled) {
        LOG.debug("Suspend server queue " + serverName);
      }
      queue.setSuspended(true);
      removeFromRunQueue(serverRunQueue, queue);
      event.suspendServerQueue(queue);
    } finally {
      schedLock.unlock();
    }
  }

  public void suspend(ProcedureEvent event) {
    final boolean isDebugEnabled = LOG.isDebugEnabled();
    synchronized (event) {
      event.setReady(false);
      if (isDebugEnabled) {
        LOG.debug("Suspend event " + event);
      }
    }
  }

  public void wake(ProcedureEvent event) {
    final boolean isDebugEnabled = LOG.isDebugEnabled();
    synchronized (event) {
      event.setReady(true);
      if (isDebugEnabled) {
        LOG.debug("Wake event " + event);
      }

      schedLock.lock();
      try {
        while (event.hasWaitingTables()) {
          Queue<TableName> queue = event.popWaitingTable();
          addToRunQueue(tableRunQueue, queue);
        }
        // TODO: This will change once we have the new AM
        while (event.hasWaitingServers()) {
          Queue<ServerName> queue = event.popWaitingServer();
          addToRunQueue(serverRunQueue, queue);
        }

        if (queueSize > 1) {
          schedWaitCond.signalAll();
        } else if (queueSize > 0) {
          schedWaitCond.signal();
        }
      } finally {
        schedLock.unlock();
      }
    }
  }

  public static class ProcedureEvent {
    private final String description;

    private Queue<ServerName> waitingServers = null;
    private Queue<TableName> waitingTables = null;
    private boolean ready = false;

    public ProcedureEvent(String description) {
      this.description = description;
    }

    public synchronized boolean isReady() {
      return ready;
    }

    private synchronized void setReady(boolean isReady) {
      this.ready = isReady;
    }

    private void suspendTableQueue(Queue<TableName> queue) {
      waitingTables = IterableList.append(waitingTables, queue);
    }

    private void suspendServerQueue(Queue<ServerName> queue) {
      waitingServers = IterableList.append(waitingServers, queue);
    }

    private boolean hasWaitingTables() {
      return waitingTables != null;
    }

    private Queue<TableName> popWaitingTable() {
      Queue<TableName> node = waitingTables;
      waitingTables = IterableList.remove(waitingTables, node);
      node.setSuspended(false);
      return node;
    }

    private boolean hasWaitingServers() {
      return waitingServers != null;
    }

    private Queue<ServerName> popWaitingServer() {
      Queue<ServerName> node = waitingServers;
      waitingServers = IterableList.remove(waitingServers, node);
      node.setSuspended(false);
      return node;
    }

    @Override
    public String toString() {
      return String.format("ProcedureEvent(%s)", description);
    }
  }

  // ============================================================================
  //  Table Queue Lookup Helpers
  // ============================================================================
  private TableQueue getTableQueueWithLock(TableName tableName) {
    schedLock.lock();
    try {
      return getTableQueue(tableName);
    } finally {
      schedLock.unlock();
    }
  }

  private TableQueue getTableQueue(TableName tableName) {
    Queue<TableName> node = AvlTree.get(tableMap, tableName);
    if (node != null) return (TableQueue)node;

    NamespaceQueue nsQueue = getNamespaceQueue(tableName.getNamespaceAsString());
    node = new TableQueue(tableName, nsQueue, getTablePriority(tableName));
    tableMap = AvlTree.insert(tableMap, node);
    return (TableQueue)node;
  }

  private void removeTableQueue(TableName tableName) {
    tableMap = AvlTree.remove(tableMap, tableName);
  }

  private int getTablePriority(TableName tableName) {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      return metaTablePriority;
    } else if (tableName.isSystemTable()) {
      return sysTablePriority;
    }
    return userTablePriority;
  }

  private static boolean isTableProcedure(Procedure proc) {
    return proc instanceof TableProcedureInterface;
  }

  private static TableName getTableName(Procedure proc) {
    return ((TableProcedureInterface)proc).getTableName();
  }

  // ============================================================================
  //  Namespace Queue Lookup Helpers
  // ============================================================================
  private NamespaceQueue getNamespaceQueue(String namespace) {
    Queue<String> node = AvlTree.get(namespaceMap, namespace);
    if (node != null) return (NamespaceQueue)node;

    node = new NamespaceQueue(namespace);
    namespaceMap = AvlTree.insert(namespaceMap, node);
    return (NamespaceQueue)node;
  }

  // ============================================================================
  //  Server Queue Lookup Helpers
  // ============================================================================
  private ServerQueue getServerQueueWithLock(ServerName serverName) {
    schedLock.lock();
    try {
      return getServerQueue(serverName);
    } finally {
      schedLock.unlock();
    }
  }

  private ServerQueue getServerQueue(ServerName serverName) {
    int index = getBucketIndex(serverBuckets, serverName.hashCode());
    Queue<ServerName> root = getTreeRoot(serverBuckets, index);
    Queue<ServerName> node = AvlTree.get(root, serverName);
    if (node != null) return (ServerQueue)node;

    node = new ServerQueue(serverName);
    serverBuckets[index] = AvlTree.insert(root, node);
    return (ServerQueue)node;
  }

  private void removeServerQueue(ServerName serverName) {
    int index = getBucketIndex(serverBuckets, serverName.hashCode());
    serverBuckets[index] = AvlTree.remove((ServerQueue)serverBuckets[index], serverName);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Comparable<T>> Queue<T> getTreeRoot(Object[] buckets, int index) {
    return (Queue<T>) buckets[index];
  }

  private static int getBucketIndex(Object[] buckets, int hashCode) {
    return Math.abs(hashCode) % buckets.length;
  }

  private static boolean isServerProcedure(Procedure proc) {
    return proc instanceof ServerProcedureInterface;
  }

  private static ServerName getServerName(Procedure proc) {
    return ((ServerProcedureInterface)proc).getServerName();
  }

  // ============================================================================
  //  Table and Server Queue Implementation
  // ============================================================================
  public static class ServerQueue extends QueueImpl<ServerName> {
    public ServerQueue(ServerName serverName) {
      super(serverName);
    }

    public boolean requireExclusiveLock(Procedure proc) {
      ServerProcedureInterface spi = (ServerProcedureInterface)proc;
      switch (spi.getServerOperationType()) {
        case CRASH_HANDLER:
          return true;
        default:
          break;
      }
      throw new UnsupportedOperationException("unexpected type " + spi.getServerOperationType());
    }
  }

  public static class TableQueue extends QueueImpl<TableName> {
    private final NamespaceQueue namespaceQueue;

    private TableLock tableLock = null;

    public TableQueue(TableName tableName, NamespaceQueue namespaceQueue, int priority) {
      super(tableName, priority);
      this.namespaceQueue = namespaceQueue;
    }

    public NamespaceQueue getNamespaceQueue() {
      return namespaceQueue;
    }

    @Override
    public synchronized boolean isAvailable() {
      return super.isAvailable() && !namespaceQueue.hasExclusiveLock();
    }

    // TODO: We can abort pending/in-progress operation if the new call is
    //       something like drop table. We can Override addBack(),
    //       check the type and abort all the in-flight procedurs.
    private boolean canAbortPendingOperations(Procedure proc) {
      TableProcedureInterface tpi = (TableProcedureInterface)proc;
      switch (tpi.getTableOperationType()) {
        case DELETE:
          return true;
        default:
          return false;
      }
    }

    public boolean requireExclusiveLock(Procedure proc) {
      TableProcedureInterface tpi = (TableProcedureInterface)proc;
      switch (tpi.getTableOperationType()) {
        case CREATE:
        case DELETE:
        case DISABLE:
        case ENABLE:
          return true;
        case EDIT:
          // we allow concurrent edit on the NS table
          return !tpi.getTableName().equals(TableName.NAMESPACE_TABLE_NAME);
        case READ:
          return false;
        default:
          break;
      }
      throw new UnsupportedOperationException("unexpected type " + tpi.getTableOperationType());
    }

    private synchronized boolean tryZkSharedLock(final TableLockManager lockManager,
        final String purpose) {
      // Take zk-read-lock
      TableName tableName = getKey();
      tableLock = lockManager.readLock(tableName, purpose);
      try {
        tableLock.acquire();
      } catch (IOException e) {
        LOG.error("failed acquire read lock on " + tableName, e);
        tableLock = null;
        return false;
      }
      return true;
    }

    private synchronized void releaseZkSharedLock(final TableLockManager lockManager) {
      releaseTableLock(lockManager, isSingleSharedLock());
    }

    private synchronized boolean tryZkExclusiveLock(final TableLockManager lockManager,
        final String purpose) {
      // Take zk-write-lock
      TableName tableName = getKey();
      tableLock = lockManager.writeLock(tableName, purpose);
      try {
        tableLock.acquire();
      } catch (IOException e) {
        LOG.error("failed acquire write lock on " + tableName, e);
        tableLock = null;
        return false;
      }
      return true;
    }

    private synchronized void releaseZkExclusiveLock(final TableLockManager lockManager) {
      releaseTableLock(lockManager, true);
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

  /**
   * the namespace is currently used just as a rwlock, not as a queue.
   * because ns operation are not frequent enough. so we want to avoid
   * having to move table queues around for suspend/resume.
   */
  private static class NamespaceQueue extends Queue<String> {
    public NamespaceQueue(String namespace) {
      super(namespace);
    }

    @Override
    public boolean requireExclusiveLock(Procedure proc) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void add(final Procedure proc, final boolean addToFront) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Procedure peek() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Procedure poll() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      throw new UnsupportedOperationException();
    }
  }

  // ============================================================================
  //  Table Locking Helpers
  // ============================================================================
  /**
   * Try to acquire the exclusive lock on the specified table.
   * other operations in the table-queue will be executed after the lock is released.
   * @param procedure the procedure trying to acquire the lock
   * @param table Table to lock
   * @return true if we were able to acquire the lock on the table, otherwise false.
   */
  public boolean tryAcquireTableExclusiveLock(final Procedure procedure, final TableName table) {
    schedLock.lock();
    TableQueue queue = getTableQueue(table);
    if (!queue.getNamespaceQueue().trySharedLock()) {
      return false;
    }

    if (!queue.tryExclusiveLock(procedure.getProcId())) {
      queue.getNamespaceQueue().releaseSharedLock();
      schedLock.unlock();
      return false;
    }

    removeFromRunQueue(tableRunQueue, queue);
    schedLock.unlock();

    // Zk lock is expensive...
    boolean hasXLock = queue.tryZkExclusiveLock(lockManager, procedure.toString());
    if (!hasXLock) {
      schedLock.lock();
      queue.releaseExclusiveLock();
      queue.getNamespaceQueue().releaseSharedLock();
      addToRunQueue(tableRunQueue, queue);
      schedLock.unlock();
    }
    return hasXLock;
  }

  /**
   * Release the exclusive lock taken with tryAcquireTableWrite()
   * @param procedure the procedure releasing the lock
   * @param table the name of the table that has the exclusive lock
   */
  public void releaseTableExclusiveLock(final Procedure procedure, final TableName table) {
    schedLock.lock();
    TableQueue queue = getTableQueue(table);
    schedLock.unlock();

    // Zk lock is expensive...
    queue.releaseZkExclusiveLock(lockManager);

    schedLock.lock();
    queue.releaseExclusiveLock();
    queue.getNamespaceQueue().releaseSharedLock();
    addToRunQueue(tableRunQueue, queue);
    schedLock.unlock();
  }

  /**
   * Try to acquire the shared lock on the specified table.
   * other "read" operations in the table-queue may be executed concurrently,
   * @param procedure the procedure trying to acquire the lock
   * @param table Table to lock
   * @return true if we were able to acquire the lock on the table, otherwise false.
   */
  public boolean tryAcquireTableSharedLock(final Procedure procedure, final TableName table) {
    return tryAcquireTableQueueSharedLock(procedure, table) != null;
  }

  private TableQueue tryAcquireTableQueueSharedLock(final Procedure procedure,
      final TableName table) {
    schedLock.lock();
    TableQueue queue = getTableQueue(table);
    if (!queue.getNamespaceQueue().trySharedLock()) {
      return null;
    }

    if (!queue.trySharedLock()) {
      queue.getNamespaceQueue().releaseSharedLock();
      schedLock.unlock();
      return null;
    }

    schedLock.unlock();

    // Zk lock is expensive...
    if (!queue.tryZkSharedLock(lockManager, procedure.toString())) {
      schedLock.lock();
      queue.releaseSharedLock();
      queue.getNamespaceQueue().releaseSharedLock();
      schedLock.unlock();
      return null;
    }
    return queue;
  }

  /**
   * Release the shared lock taken with tryAcquireTableRead()
   * @param procedure the procedure releasing the lock
   * @param table the name of the table that has the shared lock
   */
  public void releaseTableSharedLock(final Procedure procedure, final TableName table) {
    final TableQueue queue = getTableQueueWithLock(table);

    // Zk lock is expensive...
    queue.releaseZkSharedLock(lockManager);

    schedLock.lock();
    queue.releaseSharedLock();
    queue.getNamespaceQueue().releaseSharedLock();
    schedLock.unlock();
  }

  /**
   * Tries to remove the queue and the table-lock of the specified table.
   * If there are new operations pending (e.g. a new create),
   * the remove will not be performed.
   * @param table the name of the table that should be marked as deleted
   * @return true if deletion succeeded, false otherwise meaning that there are
   *     other new operations pending for that table (e.g. a new create).
   */
  protected boolean markTableAsDeleted(final TableName table) {
    final ReentrantLock l = schedLock;
    l.lock();
    try {
      TableQueue queue = getTableQueue(table);
      if (queue == null) return true;

      if (queue.isEmpty() && queue.tryExclusiveLock(0)) {
        // remove the table from the run-queue and the map
        if (IterableList.isLinked(queue)) {
          tableRunQueue.remove(queue);
        }

        // Remove the table lock
        try {
          lockManager.tableDeleted(table);
        } catch (IOException e) {
          LOG.warn("Received exception from TableLockManager.tableDeleted:", e); //not critical
        }

        removeTableQueue(table);
      } else {
        // TODO: If there are no create, we can drop all the other ops
        return false;
      }
    } finally {
      l.unlock();
    }
    return true;
  }

  // ============================================================================
  //  Namespace Locking Helpers
  // ============================================================================
  /**
   * Try to acquire the exclusive lock on the specified namespace.
   * @see #releaseNamespaceExclusiveLock(Procedure,String)
   * @param procedure the procedure trying to acquire the lock
   * @param nsName Namespace to lock
   * @return true if we were able to acquire the lock on the namespace, otherwise false.
   */
  public boolean tryAcquireNamespaceExclusiveLock(final Procedure procedure, final String nsName) {
    schedLock.lock();
    try {
      TableQueue tableQueue = getTableQueue(TableName.NAMESPACE_TABLE_NAME);
      if (!tableQueue.trySharedLock()) return false;

      NamespaceQueue nsQueue = getNamespaceQueue(nsName);
      boolean hasLock = nsQueue.tryExclusiveLock(procedure.getProcId());
      if (!hasLock) {
        tableQueue.releaseSharedLock();
      }
      return hasLock;
    } finally {
      schedLock.unlock();
    }
  }

  /**
   * Release the exclusive lock
   * @see #tryAcquireNamespaceExclusiveLock(Procedure,String)
   * @param procedure the procedure releasing the lock
   * @param nsName the namespace that has the exclusive lock
   */
  public void releaseNamespaceExclusiveLock(final Procedure procedure, final String nsName) {
    schedLock.lock();
    try {
      TableQueue tableQueue = getTableQueue(TableName.NAMESPACE_TABLE_NAME);
      tableQueue.releaseSharedLock();

      NamespaceQueue queue = getNamespaceQueue(nsName);
      queue.releaseExclusiveLock();
    } finally {
      schedLock.unlock();
    }
  }

  // ============================================================================
  //  Server Locking Helpers
  // ============================================================================
  /**
   * Try to acquire the exclusive lock on the specified server.
   * @see #releaseServerExclusiveLock(Procedure,ServerName)
   * @param procedure the procedure trying to acquire the lock
   * @param serverName Server to lock
   * @return true if we were able to acquire the lock on the server, otherwise false.
   */
  public boolean tryAcquireServerExclusiveLock(final Procedure procedure,
      final ServerName serverName) {
    schedLock.lock();
    try {
      ServerQueue queue = getServerQueue(serverName);
      if (queue.tryExclusiveLock(procedure.getProcId())) {
        removeFromRunQueue(serverRunQueue, queue);
        return true;
      }
    } finally {
      schedLock.unlock();
    }
    return false;
  }

  /**
   * Release the exclusive lock
   * @see #tryAcquireServerExclusiveLock(Procedure,ServerName)
   * @param procedure the procedure releasing the lock
   * @param serverName the server that has the exclusive lock
   */
  public void releaseServerExclusiveLock(final Procedure procedure,
      final ServerName serverName) {
    schedLock.lock();
    try {
      ServerQueue queue = getServerQueue(serverName);
      queue.releaseExclusiveLock();
      addToRunQueue(serverRunQueue, queue);
    } finally {
      schedLock.unlock();
    }
  }

  /**
   * Try to acquire the shared lock on the specified server.
   * @see #releaseServerSharedLock(Procedure,ServerName)
   * @param procedure the procedure releasing the lock
   * @param serverName Server to lock
   * @return true if we were able to acquire the lock on the server, otherwise false.
   */
  public boolean tryAcquireServerSharedLock(final Procedure procedure,
      final ServerName serverName) {
    return getServerQueueWithLock(serverName).trySharedLock();
  }

  /**
   * Release the shared lock taken
   * @see #tryAcquireServerSharedLock(Procedure,ServerName)
   * @param procedure the procedure releasing the lock
   * @param serverName the server that has the shared lock
   */
  public void releaseServerSharedLock(final Procedure procedure,
      final ServerName serverName) {
    getServerQueueWithLock(serverName).releaseSharedLock();
  }

  // ============================================================================
  //  Generic Helpers
  // ============================================================================
  private static interface QueueInterface {
    boolean isAvailable();
    boolean isEmpty();
    int size();

    void add(Procedure proc, boolean addFront);
    boolean requireExclusiveLock(Procedure proc);
    Procedure peek();
    Procedure poll();

    boolean isSuspended();
  }

  private static abstract class Queue<TKey extends Comparable<TKey>> implements QueueInterface {
    private Queue<TKey> avlRight = null;
    private Queue<TKey> avlLeft = null;
    private int avlHeight = 1;

    private Queue<TKey> iterNext = null;
    private Queue<TKey> iterPrev = null;
    private boolean suspended = false;

    private long exclusiveLockProcIdOwner = Long.MIN_VALUE;
    private int sharedLock = 0;

    private final TKey key;
    private final int priority;

    public Queue(TKey key) {
      this(key, 1);
    }

    public Queue(TKey key, int priority) {
      this.key = key;
      this.priority = priority;
    }

    protected TKey getKey() {
      return key;
    }

    protected int getPriority() {
      return priority;
    }

    /**
     * True if the queue is not in the run-queue and it is owned by an event.
     */
    public boolean isSuspended() {
      return suspended;
    }

    protected boolean setSuspended(boolean isSuspended) {
      if (this.suspended == isSuspended) return false;
      this.suspended = isSuspended;
      return true;
    }

    // ======================================================================
    //  Read/Write Locking helpers
    // ======================================================================
    public synchronized boolean isLocked() {
      return hasExclusiveLock() || sharedLock > 0;
    }

    public synchronized boolean hasExclusiveLock() {
      return this.exclusiveLockProcIdOwner != Long.MIN_VALUE;
    }

    public synchronized boolean trySharedLock() {
      if (hasExclusiveLock()) return false;
      sharedLock++;
      return true;
    }

    public synchronized void releaseSharedLock() {
      sharedLock--;
    }

    protected synchronized boolean isSingleSharedLock() {
      return sharedLock == 1;
    }

    public synchronized boolean tryExclusiveLock(long procIdOwner) {
      assert procIdOwner != Long.MIN_VALUE;
      if (isLocked()) return false;
      exclusiveLockProcIdOwner = procIdOwner;
      return true;
    }

    public synchronized void releaseExclusiveLock() {
      exclusiveLockProcIdOwner = Long.MIN_VALUE;
    }

    // This should go away when we have the new AM and its events
    // and we move xlock to the lock-event-queue.
    public synchronized boolean isAvailable() {
      return !hasExclusiveLock() && !isEmpty();
    }

    // ======================================================================
    //  Generic Helpers
    // ======================================================================
    public int compareKey(TKey cmpKey) {
      return key.compareTo(cmpKey);
    }

    public int compareTo(Queue<TKey> other) {
      return compareKey(other.key);
    }

    @Override
    public String toString() {
      return String.format("%s(%s)", getClass().getSimpleName(), key);
    }
  }

  // ======================================================================
  //  Helper Data Structures
  // ======================================================================
  private static abstract class QueueImpl<TKey extends Comparable<TKey>> extends Queue<TKey> {
    private final ArrayDeque<Procedure> runnables = new ArrayDeque<Procedure>();

    public QueueImpl(TKey key) {
      super(key);
    }

    public QueueImpl(TKey key, int priority) {
      super(key, priority);
    }

    public void add(final Procedure proc, final boolean addToFront) {
      if (addToFront) {
        addFront(proc);
      } else {
        addBack(proc);
      }
    }

    protected void addFront(final Procedure proc) {
      runnables.addFirst(proc);
    }

    protected void addBack(final Procedure proc) {
      runnables.addLast(proc);
    }

    public Procedure peek() {
      return runnables.peek();
    }

    @Override
    public Procedure poll() {
      return runnables.poll();
    }

    @Override
    public boolean isEmpty() {
      return runnables.isEmpty();
    }

    public int size() {
      return runnables.size();
    }
  }

  private static class FairQueue<T extends Comparable<T>> {
    private final int quantum;

    private Queue<T> currentQueue = null;
    private Queue<T> queueHead = null;
    private int currentQuantum = 0;

    public FairQueue() {
      this(1);
    }

    public FairQueue(int quantum) {
      this.quantum = quantum;
    }

    public void add(Queue<T> queue) {
      queueHead = IterableList.append(queueHead, queue);
      if (currentQueue == null) setNextQueue(queueHead);
    }

    public void remove(Queue<T> queue) {
      Queue<T> nextQueue = queue.iterNext;
      queueHead = IterableList.remove(queueHead, queue);
      if (currentQueue == queue) {
        setNextQueue(queueHead != null ? nextQueue : null);
      }
    }

    public Queue<T> poll() {
      if (currentQuantum == 0) {
        if (!nextQueue()) {
          return null; // nothing here
        }
        currentQuantum = calculateQuantum(currentQueue) - 1;
      } else {
        currentQuantum--;
      }

      // This should go away when we have the new AM and its events
      if (!currentQueue.isAvailable()) {
        Queue<T> lastQueue = currentQueue;
        do {
          if (!nextQueue())
            return null;
        } while (currentQueue != lastQueue && !currentQueue.isAvailable());

        currentQuantum = calculateQuantum(currentQueue) - 1;
      }
      return currentQueue;
    }

    private boolean nextQueue() {
      if (currentQueue == null) return false;
      currentQueue = currentQueue.iterNext;
      return currentQueue != null;
    }

    private void setNextQueue(Queue<T> queue) {
      currentQueue = queue;
      if (queue != null) {
        currentQuantum = calculateQuantum(currentQueue);
      } else {
        currentQuantum = 0;
      }
    }

    private int calculateQuantum(final Queue queue) {
      return Math.max(1, queue.getPriority() * quantum); // TODO
    }
  }

  private static class AvlTree {
    public static <T extends Comparable<T>> Queue<T> get(Queue<T> root, T key) {
      while (root != null) {
        int cmp = root.compareKey(key);
        if (cmp > 0) {
          root = root.avlLeft;
        } else if (cmp < 0) {
          root = root.avlRight;
        } else {
          return root;
        }
      }
      return null;
    }

    public static <T extends Comparable<T>> Queue<T> getFirst(Queue<T> root) {
      if (root != null) {
        while (root.avlLeft != null) {
          root = root.avlLeft;
        }
      }
      return root;
    }

    public static <T extends Comparable<T>> Queue<T> getLast(Queue<T> root) {
      if (root != null) {
        while (root.avlRight != null) {
          root = root.avlRight;
        }
      }
      return root;
    }

    public static <T extends Comparable<T>> Queue<T> insert(Queue<T> root, Queue<T> node) {
      if (root == null) return node;
      if (node.compareTo(root) < 0) {
        root.avlLeft = insert(root.avlLeft, node);
      } else {
        root.avlRight = insert(root.avlRight, node);
      }
      return balance(root);
    }

    private static <T extends Comparable<T>> Queue<T> removeMin(Queue<T> p) {
      if (p.avlLeft == null)
        return p.avlRight;
      p.avlLeft = removeMin(p.avlLeft);
      return balance(p);
    }

    public static <T extends Comparable<T>> Queue<T> remove(Queue<T> root, T key) {
      if (root == null) return null;

      int cmp = root.compareKey(key);
      if (cmp == 0) {
        Queue<T> q = root.avlLeft;
        Queue<T> r = root.avlRight;
        if (r == null) return q;
        Queue<T> min = getFirst(r);
        min.avlRight = removeMin(r);
        min.avlLeft = q;
        return balance(min);
      } else if (cmp > 0) {
        root.avlLeft = remove(root.avlLeft, key);
      } else /* if (cmp < 0) */ {
        root.avlRight = remove(root.avlRight, key);
      }
      return balance(root);
    }

    private static <T extends Comparable<T>> Queue<T> balance(Queue<T> p) {
      fixHeight(p);
      int balance = balanceFactor(p);
      if (balance == 2) {
        if (balanceFactor(p.avlRight) < 0) {
          p.avlRight = rotateRight(p.avlRight);
        }
        return rotateLeft(p);
      } else if (balance == -2) {
        if (balanceFactor(p.avlLeft) > 0) {
          p.avlLeft = rotateLeft(p.avlLeft);
        }
        return rotateRight(p);
      }
      return p;
    }

    private static <T extends Comparable<T>> Queue<T> rotateRight(Queue<T> p) {
      Queue<T> q = p.avlLeft;
      p.avlLeft = q.avlRight;
      q.avlRight = p;
      fixHeight(p);
      fixHeight(q);
      return q;
    }

    private static <T extends Comparable<T>> Queue<T> rotateLeft(Queue<T> q) {
      Queue<T> p = q.avlRight;
      q.avlRight = p.avlLeft;
      p.avlLeft = q;
      fixHeight(q);
      fixHeight(p);
      return p;
    }

    private static <T extends Comparable<T>> void fixHeight(Queue<T> node) {
      int heightLeft = height(node.avlLeft);
      int heightRight = height(node.avlRight);
      node.avlHeight = 1 + Math.max(heightLeft, heightRight);
    }

    private static <T extends Comparable<T>> int height(Queue<T> node) {
      return node != null ? node.avlHeight : 0;
    }

    private static <T extends Comparable<T>> int balanceFactor(Queue<T> node) {
      return height(node.avlRight) - height(node.avlLeft);
    }
  }

  private static class IterableList {
    public static <T extends Comparable<T>> Queue<T> prepend(Queue<T> head, Queue<T> node) {
      assert !isLinked(node) : node + " is already linked";
      if (head != null) {
        Queue<T> tail = head.iterPrev;
        tail.iterNext = node;
        head.iterPrev = node;
        node.iterNext = head;
        node.iterPrev = tail;
      } else {
        node.iterNext = node;
        node.iterPrev = node;
      }
      return node;
    }

    public static <T extends Comparable<T>> Queue<T> append(Queue<T> head, Queue<T> node) {
      assert !isLinked(node) : node + " is already linked";
      if (head != null) {
        Queue<T> tail = head.iterPrev;
        tail.iterNext = node;
        node.iterNext = head;
        node.iterPrev = tail;
        head.iterPrev = node;
        return head;
      }
      node.iterNext = node;
      node.iterPrev = node;
      return node;
    }

    public static <T extends Comparable<T>> Queue<T> appendList(Queue<T> head, Queue<T> otherHead) {
      if (head == null) return otherHead;
      if (otherHead == null) return head;

      Queue<T> tail = head.iterPrev;
      Queue<T> otherTail = otherHead.iterPrev;
      tail.iterNext = otherHead;
      otherHead.iterPrev = tail;
      otherTail.iterNext = head;
      head.iterPrev = otherTail;
      return head;
    }

    private static <T extends Comparable<T>> Queue<T> remove(Queue<T> head, Queue<T> node) {
      assert isLinked(node) : node + " is not linked";
      if (node != node.iterNext) {
        node.iterPrev.iterNext = node.iterNext;
        node.iterNext.iterPrev = node.iterPrev;
        head = (head == node) ? node.iterNext : head;
      } else {
        head = null;
      }
      node.iterNext = null;
      node.iterPrev = null;
      return head;
    }

    private static <T extends Comparable<T>> boolean isLinked(Queue<T> node) {
      return node.iterPrev != null && node.iterNext != null;
    }
  }
}