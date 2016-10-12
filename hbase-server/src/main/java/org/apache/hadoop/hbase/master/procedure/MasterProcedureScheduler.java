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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface.TableOperationType;
import org.apache.hadoop.hbase.procedure2.AbstractProcedureScheduler;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEventQueue;
import org.apache.hadoop.hbase.util.AvlUtil.AvlKeyComparator;
import org.apache.hadoop.hbase.util.AvlUtil.AvlIterableList;
import org.apache.hadoop.hbase.util.AvlUtil.AvlLinkedNode;
import org.apache.hadoop.hbase.util.AvlUtil.AvlTree;
import org.apache.hadoop.hbase.util.AvlUtil.AvlTreeIterator;

/**
 * ProcedureScheduler for the Master Procedures.
 * This ProcedureScheduler tries to provide to the ProcedureExecutor procedures
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
public class MasterProcedureScheduler extends AbstractProcedureScheduler {
  private static final Log LOG = LogFactory.getLog(MasterProcedureScheduler.class);

  private final TableLockManager lockManager;

  private final static NamespaceQueueKeyComparator NAMESPACE_QUEUE_KEY_COMPARATOR =
      new NamespaceQueueKeyComparator();
  private final static ServerQueueKeyComparator SERVER_QUEUE_KEY_COMPARATOR =
      new ServerQueueKeyComparator();
  private final static TableQueueKeyComparator TABLE_QUEUE_KEY_COMPARATOR =
      new TableQueueKeyComparator();

  private final FairQueue<ServerName> serverRunQueue = new FairQueue<ServerName>();
  private final FairQueue<TableName> tableRunQueue = new FairQueue<TableName>();

  private final ServerQueue[] serverBuckets = new ServerQueue[128];
  private NamespaceQueue namespaceMap = null;
  private TableQueue tableMap = null;

  private final int metaTablePriority;
  private final int userTablePriority;
  private final int sysTablePriority;

  public MasterProcedureScheduler(final Configuration conf, final TableLockManager lockManager) {
    this.lockManager = lockManager;

    // TODO: should this be part of the HTD?
    metaTablePriority = conf.getInt("hbase.master.procedure.queue.meta.table.priority", 3);
    sysTablePriority = conf.getInt("hbase.master.procedure.queue.system.table.priority", 2);
    userTablePriority = conf.getInt("hbase.master.procedure.queue.user.table.priority", 1);
  }

  @Override
  public void yield(final Procedure proc) {
    push(proc, isTableProcedure(proc), true);
  }

  @Override
  protected void enqueue(final Procedure proc, final boolean addFront) {
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
        "RQs for non-table/non-server procedures are not implemented yet: " + proc);
    }
  }

  private <T extends Comparable<T>> void doAdd(final FairQueue<T> fairq,
      final Queue<T> queue, final Procedure proc, final boolean addFront) {
    if (proc.isSuspended()) return;

    queue.add(proc, addFront);
    if (!queue.hasExclusiveLock() || queue.isLockOwner(proc.getProcId())) {
      // if the queue was not remove for an xlock execution
      // or the proc is the lock owner, put the queue back into execution
      addToRunQueue(fairq, queue);
    } else if (queue.hasParentLock(proc)) {
      assert addFront : "expected to add a child in the front";
      assert !queue.isSuspended() : "unexpected suspended state for the queue";
      // our (proc) parent has the xlock,
      // so the queue is not in the fairq (run-queue)
      // add it back to let the child run (inherit the lock)
      addToRunQueue(fairq, queue);
    }
  }

  @Override
  protected boolean queueHasRunnables() {
    return tableRunQueue.hasRunnables() || serverRunQueue.hasRunnables();
  }

  @Override
  protected Procedure dequeue() {
    // For now, let server handling have precedence over table handling; presumption is that it
    // is more important handling crashed servers than it is running the
    // enabling/disabling tables, etc.
    Procedure pollResult = doPoll(serverRunQueue);
    if (pollResult == null) {
      pollResult = doPoll(tableRunQueue);
    }
    return pollResult;
  }

  private <T extends Comparable<T>> Procedure doPoll(final FairQueue<T> fairq) {
    final Queue<T> rq = fairq.poll();
    if (rq == null || !rq.isAvailable()) {
      return null;
    }

    assert !rq.isSuspended() : "rq=" + rq + " is suspended";
    final Procedure pollResult = rq.peek();
    final boolean xlockReq = rq.requireExclusiveLock(pollResult);
    if (xlockReq && rq.isLocked() && !rq.hasLockAccess(pollResult)) {
      // someone is already holding the lock (e.g. shared lock). avoid a yield
      return null;
    }

    rq.poll();
    if (rq.isEmpty() || xlockReq) {
      removeFromRunQueue(fairq, rq);
    } else if (rq.hasParentLock(pollResult)) {
      // if the rq is in the fairq because of runnable child
      // check if the next procedure is still a child.
      // if not, remove the rq from the fairq and go back to the xlock state
      Procedure nextProc = rq.peek();
      if (nextProc != null && !Procedure.haveSameParent(nextProc, pollResult)) {
        removeFromRunQueue(fairq, rq);
      }
    }

    return pollResult;
  }

  @Override
  public void clearQueue() {
    // Remove Servers
    for (int i = 0; i < serverBuckets.length; ++i) {
      clear(serverBuckets[i], serverRunQueue, SERVER_QUEUE_KEY_COMPARATOR);
      serverBuckets[i] = null;
    }

    // Remove Tables
    clear(tableMap, tableRunQueue, TABLE_QUEUE_KEY_COMPARATOR);
    tableMap = null;

    assert size() == 0 : "expected queue size to be 0, got " + size();
  }

  private <T extends Comparable<T>, TNode extends Queue<T>> void clear(TNode treeMap,
      final FairQueue<T> fairq, final AvlKeyComparator<TNode> comparator) {
    while (treeMap != null) {
      Queue<T> node = AvlTree.getFirst(treeMap);
      assert !node.isSuspended() : "can't clear suspended " + node.getKey();
      treeMap = AvlTree.remove(treeMap, node.getKey(), comparator);
      removeFromRunQueue(fairq, node);
    }
  }

  @Override
  public int queueSize() {
    int count = 0;

    // Server queues
    final AvlTreeIterator<ServerQueue> serverIter = new AvlTreeIterator<ServerQueue>();
    for (int i = 0; i < serverBuckets.length; ++i) {
      serverIter.seekFirst(serverBuckets[i]);
      while (serverIter.hasNext()) {
        count += serverIter.next().size();
      }
    }

    // Table queues
    final AvlTreeIterator<TableQueue> tableIter = new AvlTreeIterator<TableQueue>(tableMap);
    while (tableIter.hasNext()) {
      count += tableIter.next().size();
    }
    return count;
  }

  @Override
  public void completionCleanup(final Procedure proc) {
    if (proc instanceof TableProcedureInterface) {
      TableProcedureInterface iProcTable = (TableProcedureInterface)proc;
      boolean tableDeleted;
      if (proc.hasException()) {
        Exception procEx = proc.getException().unwrapRemoteException();
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
        markTableAsDeleted(iProcTable.getTableName(), proc);
        return;
      }
    } else {
      // No cleanup for ServerProcedureInterface types, yet.
      return;
    }
  }

  private <T extends Comparable<T>> void addToRunQueue(FairQueue<T> fairq, Queue<T> queue) {
    if (!AvlIterableList.isLinked(queue) &&
        !queue.isEmpty() && !queue.isSuspended())  {
      fairq.add(queue);
    }
  }

  private <T extends Comparable<T>> void removeFromRunQueue(FairQueue<T> fairq, Queue<T> queue) {
    if (AvlIterableList.isLinked(queue)) {
      fairq.remove(queue);
    }
  }

  // ============================================================================
  //  Table Queue Lookup Helpers
  // ============================================================================
  private TableQueue getTableQueueWithLock(TableName tableName) {
    schedLock();
    try {
      return getTableQueue(tableName);
    } finally {
      schedUnlock();
    }
  }

  private TableQueue getTableQueue(TableName tableName) {
    TableQueue node = AvlTree.get(tableMap, tableName, TABLE_QUEUE_KEY_COMPARATOR);
    if (node != null) return node;

    NamespaceQueue nsQueue = getNamespaceQueue(tableName.getNamespaceAsString());
    node = new TableQueue(tableName, nsQueue, getTablePriority(tableName));
    tableMap = AvlTree.insert(tableMap, node);
    return node;
  }

  private void removeTableQueue(TableName tableName) {
    tableMap = AvlTree.remove(tableMap, tableName, TABLE_QUEUE_KEY_COMPARATOR);
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
    NamespaceQueue node = AvlTree.get(namespaceMap, namespace, NAMESPACE_QUEUE_KEY_COMPARATOR);
    if (node != null) return (NamespaceQueue)node;

    node = new NamespaceQueue(namespace);
    namespaceMap = AvlTree.insert(namespaceMap, node);
    return node;
  }

  // ============================================================================
  //  Server Queue Lookup Helpers
  // ============================================================================
  private ServerQueue getServerQueueWithLock(ServerName serverName) {
    schedLock();
    try {
      return getServerQueue(serverName);
    } finally {
      schedUnlock();
    }
  }

  private ServerQueue getServerQueue(ServerName serverName) {
    final int index = getBucketIndex(serverBuckets, serverName.hashCode());
    ServerQueue node = AvlTree.get(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
    if (node != null) return node;

    node = new ServerQueue(serverName);
    serverBuckets[index] = AvlTree.insert(serverBuckets[index], node);
    return (ServerQueue)node;
  }

  private void removeServerQueue(ServerName serverName) {
    final int index = getBucketIndex(serverBuckets, serverName.hashCode());
    final ServerQueue root = serverBuckets[index];
    serverBuckets[index] = AvlTree.remove(root, serverName, SERVER_QUEUE_KEY_COMPARATOR);
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
  private static class ServerQueueKeyComparator implements AvlKeyComparator<ServerQueue> {
    @Override
    public int compareKey(ServerQueue node, Object key) {
      return node.compareKey((ServerName)key);
    }
  }

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

  private static class RegionEvent extends ProcedureEventQueue {
    private final HRegionInfo regionInfo;
    private long exclusiveLockProcIdOwner = Long.MIN_VALUE;

    public RegionEvent(HRegionInfo regionInfo) {
      this.regionInfo = regionInfo;
    }

    public boolean hasExclusiveLock() {
      return exclusiveLockProcIdOwner != Long.MIN_VALUE;
    }

    public boolean isLockOwner(long procId) {
      return exclusiveLockProcIdOwner == procId;
    }

    public boolean tryExclusiveLock(final long procIdOwner) {
      assert procIdOwner != Long.MIN_VALUE;
      if (hasExclusiveLock() && !isLockOwner(procIdOwner)) return false;
      exclusiveLockProcIdOwner = procIdOwner;
      return true;
    }

    private void releaseExclusiveLock() {
      exclusiveLockProcIdOwner = Long.MIN_VALUE;
    }

    public HRegionInfo getRegionInfo() {
      return regionInfo;
    }

    @Override
    public String toString() {
      return "RegionEvent(" + regionInfo.getRegionNameAsString() + ")";
    }
  }

  private static class TableQueueKeyComparator implements AvlKeyComparator<TableQueue> {
    @Override
    public int compareKey(TableQueue node, Object key) {
      return node.compareKey((TableName)key);
    }
  }

  public static class TableQueue extends QueueImpl<TableName> {
    private final NamespaceQueue namespaceQueue;

    private HashMap<HRegionInfo, RegionEvent> regionEventMap;
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
      // if there are no items in the queue, or the namespace is locked.
      // we can't execute operation on this table
      if (isEmpty() || namespaceQueue.hasExclusiveLock()) {
        return false;
      }

      if (hasExclusiveLock()) {
        // if we have an exclusive lock already taken
        // only child of the lock owner can be executed
        final Procedure nextProc = peek();
        return nextProc != null && hasLockAccess(nextProc);
      }

      // no xlock
      return true;
    }

    public synchronized RegionEvent getRegionEvent(final HRegionInfo regionInfo) {
      if (regionEventMap == null) {
        regionEventMap = new HashMap<HRegionInfo, RegionEvent>();
      }
      RegionEvent event = regionEventMap.get(regionInfo);
      if (event == null) {
        event = new RegionEvent(regionInfo);
        regionEventMap.put(regionInfo, event);
      }
      return event;
    }

    public synchronized void removeRegionEvent(final RegionEvent event) {
      regionEventMap.remove(event.getRegionInfo());
      if (regionEventMap.isEmpty()) {
        regionEventMap = null;
      }
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
        // region operations are using the shared-lock on the table
        // and then they will grab an xlock on the region.
        case SPLIT:
        case MERGE:
        case ASSIGN:
        case UNASSIGN:
        case REGION_EDIT:
          return false;
        default:
          break;
      }
      throw new UnsupportedOperationException("unexpected type " + tpi.getTableOperationType());
    }

    private synchronized boolean tryZkSharedLock(final TableLockManager lockManager,
        final String purpose) {
      // Since we only have one lock resource.  We should only acquire zk lock if the znode
      // does not exist.
      //
      if (isSingleSharedLock()) {
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
      }
      return true;
    }

    private synchronized void releaseZkSharedLock(final TableLockManager lockManager) {
      if (isSingleSharedLock()) {
        releaseTableLock(lockManager, true);
      }
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

  private static class NamespaceQueueKeyComparator implements AvlKeyComparator<NamespaceQueue> {
    @Override
    public int compareKey(NamespaceQueue node, Object key) {
      return node.compareKey((String)key);
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
    schedLock();
    TableQueue queue = getTableQueue(table);
    if (!queue.getNamespaceQueue().trySharedLock()) {
      schedUnlock();
      return false;
    }

    if (!queue.tryExclusiveLock(procedure)) {
      queue.getNamespaceQueue().releaseSharedLock();
      schedUnlock();
      return false;
    }

    removeFromRunQueue(tableRunQueue, queue);
    boolean hasParentLock = queue.hasParentLock(procedure);
    schedUnlock();

    boolean hasXLock = true;
    if (!hasParentLock) {
      // Zk lock is expensive...
      hasXLock = queue.tryZkExclusiveLock(lockManager, procedure.toString());
      if (!hasXLock) {
        schedLock();
        if (!hasParentLock) queue.releaseExclusiveLock();
        queue.getNamespaceQueue().releaseSharedLock();
        addToRunQueue(tableRunQueue, queue);
        schedUnlock();
      }
    }
    return hasXLock;
  }

  /**
   * Release the exclusive lock taken with tryAcquireTableWrite()
   * @param procedure the procedure releasing the lock
   * @param table the name of the table that has the exclusive lock
   */
  public void releaseTableExclusiveLock(final Procedure procedure, final TableName table) {
    final TableQueue queue = getTableQueueWithLock(table);
    final boolean hasParentLock = queue.hasParentLock(procedure);

    if (!hasParentLock) {
      // Zk lock is expensive...
      queue.releaseZkExclusiveLock(lockManager);
    }

    schedLock();
    if (!hasParentLock) queue.releaseExclusiveLock();
    queue.getNamespaceQueue().releaseSharedLock();
    addToRunQueue(tableRunQueue, queue);
    schedUnlock();
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
    schedLock();
    TableQueue queue = getTableQueue(table);
    if (!queue.getNamespaceQueue().trySharedLock()) {
      return null;
    }

    if (!queue.trySharedLock()) {
      queue.getNamespaceQueue().releaseSharedLock();
      schedUnlock();
      return null;
    }

    // TODO: Zk lock is expensive and it would be perf bottleneck.  Long term solution is
    // to remove it.
    if (!queue.tryZkSharedLock(lockManager, procedure.toString())) {
      queue.releaseSharedLock();
      queue.getNamespaceQueue().releaseSharedLock();
      schedUnlock();
      return null;
    }

    schedUnlock();

    return queue;
  }

  /**
   * Release the shared lock taken with tryAcquireTableRead()
   * @param procedure the procedure releasing the lock
   * @param table the name of the table that has the shared lock
   */
  public void releaseTableSharedLock(final Procedure procedure, final TableName table) {
    final TableQueue queue = getTableQueueWithLock(table);

    schedLock();
    // Zk lock is expensive...
    queue.releaseZkSharedLock(lockManager);

    queue.getNamespaceQueue().releaseSharedLock();
    if (queue.releaseSharedLock()) {
      addToRunQueue(tableRunQueue, queue);
    }
    schedUnlock();
  }

  /**
   * Tries to remove the queue and the table-lock of the specified table.
   * If there are new operations pending (e.g. a new create),
   * the remove will not be performed.
   * @param table the name of the table that should be marked as deleted
   * @param procedure the procedure that is removing the table
   * @return true if deletion succeeded, false otherwise meaning that there are
   *     other new operations pending for that table (e.g. a new create).
   */
  @VisibleForTesting
  protected boolean markTableAsDeleted(final TableName table, final Procedure procedure) {
    schedLock();
    try {
      TableQueue queue = getTableQueue(table);
      if (queue == null) return true;

      if (queue.isEmpty() && queue.tryExclusiveLock(procedure)) {
        // remove the table from the run-queue and the map
        if (AvlIterableList.isLinked(queue)) {
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
      schedUnlock();
    }
    return true;
  }

  // ============================================================================
  //  Region Locking Helpers
  // ============================================================================
  /**
   * Suspend the procedure if the specified region is already locked.
   * @param procedure the procedure trying to acquire the lock on the region
   * @param regionInfo the region we are trying to lock
   * @return true if the procedure has to wait for the regions to be available
   */
  public boolean waitRegion(final Procedure procedure, final HRegionInfo regionInfo) {
    return waitRegions(procedure, regionInfo.getTable(), regionInfo);
  }

  /**
   * Suspend the procedure if the specified set of regions are already locked.
   * @param procedure the procedure trying to acquire the lock on the regions
   * @param table the table name of the regions we are trying to lock
   * @param regionInfo the list of regions we are trying to lock
   * @return true if the procedure has to wait for the regions to be available
   */
  public boolean waitRegions(final Procedure procedure, final TableName table,
      final HRegionInfo... regionInfo) {
    Arrays.sort(regionInfo);

    final TableQueue queue;
    if (procedure.hasParent()) {
      // the assumption is that the parent procedure have already the table xlock
      queue = getTableQueueWithLock(table);
    } else {
      // acquire the table shared-lock
      queue = tryAcquireTableQueueSharedLock(procedure, table);
      if (queue == null) return true;
    }

    // acquire region xlocks or wait
    boolean hasLock = true;
    final RegionEvent[] event = new RegionEvent[regionInfo.length];
    synchronized (queue) {
      for (int i = 0; i < regionInfo.length; ++i) {
        assert regionInfo[i].getTable().equals(table);
        assert i == 0 || regionInfo[i] != regionInfo[i-1] : "duplicate region: " + regionInfo[i];

        event[i] = queue.getRegionEvent(regionInfo[i]);
        if (!event[i].tryExclusiveLock(procedure.getProcId())) {
          suspendProcedure(event[i], procedure);
          hasLock = false;
          while (i-- > 0) {
            event[i].releaseExclusiveLock();
          }
          break;
        }
      }
    }

    if (!hasLock && !procedure.hasParent()) {
      releaseTableSharedLock(procedure, table);
    }
    return !hasLock;
  }

  /**
   * Wake the procedures waiting for the specified region
   * @param procedure the procedure that was holding the region
   * @param regionInfo the region the procedure was holding
   */
  public void wakeRegion(final Procedure procedure, final HRegionInfo regionInfo) {
    wakeRegions(procedure, regionInfo.getTable(), regionInfo);
  }

  /**
   * Wake the procedures waiting for the specified regions
   * @param procedure the procedure that was holding the regions
   * @param regionInfo the list of regions the procedure was holding
   */
  public void wakeRegions(final Procedure procedure,final TableName table,
      final HRegionInfo... regionInfo) {
    Arrays.sort(regionInfo);

    final TableQueue queue = getTableQueueWithLock(table);

    int numProcs = 0;
    final Procedure[] nextProcs = new Procedure[regionInfo.length];
    synchronized (queue) {
      HRegionInfo prevRegion = null;
      for (int i = 0; i < regionInfo.length; ++i) {
        assert regionInfo[i].getTable().equals(table);
        assert i == 0 || regionInfo[i] != regionInfo[i-1] : "duplicate region: " + regionInfo[i];

        RegionEvent event = queue.getRegionEvent(regionInfo[i]);
        event.releaseExclusiveLock();
        if (event.hasWaitingProcedures()) {
          // release one procedure at the time since regions has an xlock
          nextProcs[numProcs++] = event.popWaitingProcedure(true);
        } else {
          queue.removeRegionEvent(event);
        }
      }
    }

    // awake procedures if any
    schedLock();
    try {
      for (int i = numProcs - 1; i >= 0; --i) {
        wakeProcedure(nextProcs[i]);
      }

      wakePollIfNeeded(numProcs);

      if (!procedure.hasParent()) {
        // release the table shared-lock.
        // (if we have a parent, it is holding an xlock so we didn't take the shared-lock)
        releaseTableSharedLock(procedure, table);
      }
    } finally {
      schedUnlock();
    }
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
    schedLock();
    try {
      TableQueue tableQueue = getTableQueue(TableName.NAMESPACE_TABLE_NAME);
      if (!tableQueue.trySharedLock()) return false;

      NamespaceQueue nsQueue = getNamespaceQueue(nsName);
      boolean hasLock = nsQueue.tryExclusiveLock(procedure);
      if (!hasLock) {
        tableQueue.releaseSharedLock();
      }
      return hasLock;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Release the exclusive lock
   * @see #tryAcquireNamespaceExclusiveLock(Procedure,String)
   * @param procedure the procedure releasing the lock
   * @param nsName the namespace that has the exclusive lock
   */
  public void releaseNamespaceExclusiveLock(final Procedure procedure, final String nsName) {
    schedLock();
    try {
      final TableQueue tableQueue = getTableQueue(TableName.NAMESPACE_TABLE_NAME);
      final NamespaceQueue queue = getNamespaceQueue(nsName);

      queue.releaseExclusiveLock();
      if (tableQueue.releaseSharedLock()) {
        addToRunQueue(tableRunQueue, tableQueue);
      }
    } finally {
      schedUnlock();
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
    schedLock();
    try {
      ServerQueue queue = getServerQueue(serverName);
      if (queue.tryExclusiveLock(procedure)) {
        removeFromRunQueue(serverRunQueue, queue);
        return true;
      }
    } finally {
      schedUnlock();
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
    schedLock();
    try {
      ServerQueue queue = getServerQueue(serverName);
      queue.releaseExclusiveLock();
      addToRunQueue(serverRunQueue, queue);
    } finally {
      schedUnlock();
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

  private static abstract class Queue<TKey extends Comparable<TKey>>
      extends AvlLinkedNode<Queue<TKey>> implements QueueInterface {
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

    public synchronized boolean releaseSharedLock() {
      return --sharedLock == 0;
    }

    protected synchronized boolean isSingleSharedLock() {
      return sharedLock == 1;
    }

    public synchronized boolean isLockOwner(long procId) {
      return exclusiveLockProcIdOwner == procId;
    }

    public synchronized boolean hasParentLock(final Procedure proc) {
      return proc.hasParent() && isLockOwner(proc.getParentProcId());
    }

    public synchronized boolean hasLockAccess(final Procedure proc) {
      return isLockOwner(proc.getProcId()) || hasParentLock(proc);
    }

    public synchronized boolean tryExclusiveLock(final Procedure proc) {
      if (isLocked()) return hasLockAccess(proc);
      exclusiveLockProcIdOwner = proc.getProcId();
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

    @Override
    public int compareTo(Queue<TKey> other) {
      return compareKey(other.key);
    }

    @Override
    public String toString() {
      return String.format("%s(%s, suspended=%s xlock=%s sharedLock=%s size=%s)",
        getClass().getSimpleName(), key, isSuspended(), hasExclusiveLock(), sharedLock, size());
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
    private int size = 0;

    public FairQueue() {
      this(1);
    }

    public FairQueue(int quantum) {
      this.quantum = quantum;
    }

    public boolean hasRunnables() {
      return size > 0;
    }

    public void add(Queue<T> queue) {
      queueHead = AvlIterableList.append(queueHead, queue);
      if (currentQueue == null) setNextQueue(queueHead);
      size++;
    }

    public void remove(Queue<T> queue) {
      Queue<T> nextQueue = AvlIterableList.readNext(queue);
      queueHead = AvlIterableList.remove(queueHead, queue);
      if (currentQueue == queue) {
        setNextQueue(queueHead != null ? nextQueue : null);
      }
      size--;
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
      currentQueue = AvlIterableList.readNext(currentQueue);
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
}
