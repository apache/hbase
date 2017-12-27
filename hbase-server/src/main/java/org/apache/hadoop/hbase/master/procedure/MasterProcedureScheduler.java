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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface.PeerOperationType;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface.TableOperationType;
import org.apache.hadoop.hbase.procedure2.AbstractProcedureScheduler;
import org.apache.hadoop.hbase.procedure2.LockAndQueue;
import org.apache.hadoop.hbase.procedure2.LockStatus;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.LockedResourceType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureDeque;
import org.apache.hadoop.hbase.util.AvlUtil.AvlIterableList;
import org.apache.hadoop.hbase.util.AvlUtil.AvlKeyComparator;
import org.apache.hadoop.hbase.util.AvlUtil.AvlLinkedNode;
import org.apache.hadoop.hbase.util.AvlUtil.AvlTree;
import org.apache.hadoop.hbase.util.AvlUtil.AvlTreeIterator;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * ProcedureScheduler for the Master Procedures.
 * This ProcedureScheduler tries to provide to the ProcedureExecutor procedures
 * that can be executed without having to wait on a lock.
 * Most of the master operations can be executed concurrently, if they
 * are operating on different tables (e.g. two create table procedures can be performed
 * at the same time) or against two different servers; say two servers that crashed at
 * about the same time.
 *
 * <p>Each procedure should implement an Interface providing information for this queue.
 * For example table related procedures should implement TableProcedureInterface.
 * Each procedure will be pushed in its own queue, and based on the operation type
 * we may make smarter decisions: e.g. we can abort all the operations preceding
 * a delete table, or similar.
 *
 * <h4>Concurrency control</h4>
 * Concurrent access to member variables (tableRunQueue, serverRunQueue, locking, tableMap,
 * serverBuckets) is controlled by schedLock(). This mainly includes:<br>
 * <ul>
 *   <li>
 *     {@link #push(Procedure, boolean, boolean)}: A push will add a Queue back to run-queue
 *     when:
 *     <ol>
 *       <li>Queue was empty before push (so must have been out of run-queue)</li>
 *       <li>Child procedure is added (which means parent procedure holds exclusive lock, and it
 *           must have moved Queue out of run-queue)</li>
 *     </ol>
 *   </li>
 *   <li>
 *     {@link #poll(long)}: A poll will remove a Queue from run-queue when:
 *     <ol>
 *       <li>Queue becomes empty after poll</li>
 *       <li>Exclusive lock is requested by polled procedure and lock is available (returns the
 *           procedure)</li>
 *       <li>Exclusive lock is requested but lock is not available (returns null)</li>
 *       <li>Polled procedure is child of parent holding exclusive lock and the next procedure is
 *           not a child</li>
 *     </ol>
 *   </li>
 *   <li>
 *     Namespace/table/region locks: Queue is added back to run-queue when lock being released is:
 *     <ol>
 *       <li>Exclusive lock</li>
 *       <li>Last shared lock (in case queue was removed because next procedure in queue required
 *           exclusive lock)</li>
 *     </ol>
 *   </li>
 * </ul>
 */
@InterfaceAudience.Private
public class MasterProcedureScheduler extends AbstractProcedureScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcedureScheduler.class);

  private final static ServerQueueKeyComparator SERVER_QUEUE_KEY_COMPARATOR =
      new ServerQueueKeyComparator();
  private final static TableQueueKeyComparator TABLE_QUEUE_KEY_COMPARATOR =
      new TableQueueKeyComparator();
  private final static PeerQueueKeyComparator PEER_QUEUE_KEY_COMPARATOR =
      new PeerQueueKeyComparator();

  private final FairQueue<ServerName> serverRunQueue = new FairQueue<>();
  private final FairQueue<TableName> tableRunQueue = new FairQueue<>();
  private final FairQueue<String> peerRunQueue = new FairQueue<>();

  private final ServerQueue[] serverBuckets = new ServerQueue[128];
  private TableQueue tableMap = null;
  private PeerQueue peerMap = null;

  private final SchemaLocking locking = new SchemaLocking();

  /**
   * Table priority is used when scheduling procedures from {@link #tableRunQueue}. A TableQueue
   * with priority 2 will get its procedures scheduled at twice the rate as compared to
   * TableQueue with priority 1. This should be enough to ensure system/meta get assigned out
   * before user-space tables. HBASE-18109 is where we conclude what is here is good enough.
   * Lets open new issue if we find it not enough.
   */
  private static class TablePriorities {
    final int metaTablePriority;
    final int userTablePriority;
    final int sysTablePriority;

    TablePriorities(Configuration conf) {
      metaTablePriority = conf.getInt("hbase.master.procedure.queue.meta.table.priority", 3);
      sysTablePriority = conf.getInt("hbase.master.procedure.queue.system.table.priority", 2);
      userTablePriority = conf.getInt("hbase.master.procedure.queue.user.table.priority", 1);
    }

    int getPriority(TableName tableName) {
      if (tableName.equals(TableName.META_TABLE_NAME)) {
        return metaTablePriority;
      } else if (tableName.isSystemTable()) {
        return sysTablePriority;
      }
      return userTablePriority;
    }
  }
  private final TablePriorities tablePriorities;

  public MasterProcedureScheduler(final Configuration conf) {
    tablePriorities = new TablePriorities(conf);
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
    } else if (isPeerProcedure(proc)) {
      doAdd(peerRunQueue, getPeerQueue(getPeerId(proc)), proc, addFront);
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
      final Queue<T> queue, final Procedure<?> proc, final boolean addFront) {
    queue.add(proc, addFront);
    if (!queue.getLockStatus().hasExclusiveLock() || queue.getLockStatus().isLockOwner(proc.getProcId())) {
      // if the queue was not remove for an xlock execution
      // or the proc is the lock owner, put the queue back into execution
      addToRunQueue(fairq, queue);
    } else if (queue.getLockStatus().hasParentLock(proc)) {
      assert addFront : "expected to add a child in the front";
      // our (proc) parent has the xlock,
      // so the queue is not in the fairq (run-queue)
      // add it back to let the child run (inherit the lock)
      addToRunQueue(fairq, queue);
    }
  }

  @Override
  protected boolean queueHasRunnables() {
    return tableRunQueue.hasRunnables() || serverRunQueue.hasRunnables() ||
        peerRunQueue.hasRunnables();
  }

  @Override
  protected Procedure dequeue() {
    // For now, let server handling have precedence over table handling; presumption is that it
    // is more important handling crashed servers than it is running the
    // enabling/disabling tables, etc.
    Procedure<?> pollResult = doPoll(serverRunQueue);
    if (pollResult == null) {
      pollResult = doPoll(peerRunQueue);
    }
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

    final Procedure pollResult = rq.peek();
    if (pollResult == null) {
      return null;
    }
    final boolean xlockReq = rq.requireExclusiveLock(pollResult);
    if (xlockReq && rq.getLockStatus().isLocked() && !rq.getLockStatus().hasLockAccess(pollResult)) {
      // someone is already holding the lock (e.g. shared lock). avoid a yield
      removeFromRunQueue(fairq, rq);
      return null;
    }

    rq.poll();
    if (rq.isEmpty() || xlockReq) {
      removeFromRunQueue(fairq, rq);
    } else if (rq.getLockStatus().hasParentLock(pollResult)) {
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

  private LockedResource createLockedResource(LockedResourceType resourceType,
      String resourceName, LockAndQueue queue) {
    LockType lockType;
    Procedure<?> exclusiveLockOwnerProcedure;
    int sharedLockCount;

    if (queue.hasExclusiveLock()) {
      lockType = LockType.EXCLUSIVE;
      exclusiveLockOwnerProcedure = queue.getExclusiveLockOwnerProcedure();
      sharedLockCount = 0;
    } else {
      lockType = LockType.SHARED;
      exclusiveLockOwnerProcedure = null;
      sharedLockCount = queue.getSharedLockCount();
    }

    List<Procedure<?>> waitingProcedures = new ArrayList<>();

    for (Procedure<?> procedure : queue) {
      if (!(procedure instanceof LockProcedure)) {
        continue;
      }

      waitingProcedures.add(procedure);
    }

    return new LockedResource(resourceType, resourceName, lockType,
        exclusiveLockOwnerProcedure, sharedLockCount, waitingProcedures);
  }

  private <T> void addToLockedResources(List<LockedResource> lockedResources,
      Map<T, LockAndQueue> locks, Function<T, String> keyTransformer,
      LockedResourceType resourcesType) {
    locks.entrySet().stream().filter(e -> e.getValue().isLocked())
        .map(
          e -> createLockedResource(resourcesType, keyTransformer.apply(e.getKey()), e.getValue()))
        .forEachOrdered(lockedResources::add);
  }

  @Override
  public List<LockedResource> getLocks() {
    schedLock();
    try {
      List<LockedResource> lockedResources = new ArrayList<>();
      addToLockedResources(lockedResources, locking.serverLocks, sn -> sn.getServerName(),
        LockedResourceType.SERVER);
      addToLockedResources(lockedResources, locking.namespaceLocks, Function.identity(),
        LockedResourceType.NAMESPACE);
      addToLockedResources(lockedResources, locking.tableLocks, tn -> tn.getNameAsString(),
        LockedResourceType.TABLE);
      addToLockedResources(lockedResources, locking.regionLocks, Function.identity(),
        LockedResourceType.REGION);
      addToLockedResources(lockedResources, locking.peerLocks, Function.identity(),
        LockedResourceType.PEER);
      return lockedResources;
    } finally {
      schedUnlock();
    }
  }

  @Override
  public LockedResource getLockResource(LockedResourceType resourceType, String resourceName) {
    LockAndQueue queue = null;
    schedLock();
    try {
      switch (resourceType) {
        case SERVER:
          queue = locking.serverLocks.get(ServerName.valueOf(resourceName));
          break;
        case NAMESPACE:
          queue = locking.namespaceLocks.get(resourceName);
          break;
        case TABLE:
          queue = locking.tableLocks.get(TableName.valueOf(resourceName));
          break;
        case REGION:
          queue = locking.regionLocks.get(resourceName);
          break;
        case PEER:
          queue = locking.peerLocks.get(resourceName);
          break;
      }
      return queue != null ? createLockedResource(resourceType, resourceName, queue) : null;
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void clear() {
    schedLock();
    try {
      clearQueue();
      locking.clear();
    } finally {
      schedUnlock();
    }
  }

  protected void clearQueue() {
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
      treeMap = AvlTree.remove(treeMap, node.getKey(), comparator);
      if (fairq != null) removeFromRunQueue(fairq, node);
    }
  }

  @Override
  protected int queueSize() {
    int count = 0;

    // Server queues
    final AvlTreeIterator<ServerQueue> serverIter = new AvlTreeIterator<>();
    for (int i = 0; i < serverBuckets.length; ++i) {
      serverIter.seekFirst(serverBuckets[i]);
      while (serverIter.hasNext()) {
        count += serverIter.next().size();
      }
    }

    // Table queues
    final AvlTreeIterator<TableQueue> tableIter = new AvlTreeIterator<>(tableMap);
    while (tableIter.hasNext()) {
      count += tableIter.next().size();
    }

    // Peer queues
    final AvlTreeIterator<PeerQueue> peerIter = new AvlTreeIterator<>(peerMap);
    while (peerIter.hasNext()) {
      count += peerIter.next().size();
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
    } else if (proc instanceof PeerProcedureInterface) {
      PeerProcedureInterface iProcPeer = (PeerProcedureInterface) proc;
      if (iProcPeer.getPeerOperationType() == PeerOperationType.REMOVE) {
        removePeerQueue(iProcPeer.getPeerId());
      }
    } else {
      // No cleanup for ServerProcedureInterface types, yet.
      return;
    }
  }

  private static <T extends Comparable<T>> void addToRunQueue(FairQueue<T> fairq, Queue<T> queue) {
    if (!AvlIterableList.isLinked(queue) && !queue.isEmpty()) {
      fairq.add(queue);
    }
  }

  private static <T extends Comparable<T>> void removeFromRunQueue(
      FairQueue<T> fairq, Queue<T> queue) {
    if (AvlIterableList.isLinked(queue)) {
      fairq.remove(queue);
    }
  }

  // ============================================================================
  //  Table Queue Lookup Helpers
  // ============================================================================
  private TableQueue getTableQueue(TableName tableName) {
    TableQueue node = AvlTree.get(tableMap, tableName, TABLE_QUEUE_KEY_COMPARATOR);
    if (node != null) return node;

    node = new TableQueue(tableName, tablePriorities.getPriority(tableName),
        locking.getTableLock(tableName), locking.getNamespaceLock(tableName.getNamespaceAsString()));
    tableMap = AvlTree.insert(tableMap, node);
    return node;
  }

  private void removeTableQueue(TableName tableName) {
    tableMap = AvlTree.remove(tableMap, tableName, TABLE_QUEUE_KEY_COMPARATOR);
    locking.removeTableLock(tableName);
  }

  private static boolean isTableProcedure(Procedure<?> proc) {
    return proc instanceof TableProcedureInterface;
  }

  private static TableName getTableName(Procedure<?> proc) {
    return ((TableProcedureInterface)proc).getTableName();
  }

  // ============================================================================
  //  Server Queue Lookup Helpers
  // ============================================================================
  private ServerQueue getServerQueue(ServerName serverName) {
    final int index = getBucketIndex(serverBuckets, serverName.hashCode());
    ServerQueue node = AvlTree.get(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
    if (node != null) return node;

    node = new ServerQueue(serverName, locking.getServerLock(serverName));
    serverBuckets[index] = AvlTree.insert(serverBuckets[index], node);
    return node;
  }

  private static int getBucketIndex(Object[] buckets, int hashCode) {
    return Math.abs(hashCode) % buckets.length;
  }

  private static boolean isServerProcedure(Procedure<?> proc) {
    return proc instanceof ServerProcedureInterface;
  }

  private static ServerName getServerName(Procedure<?> proc) {
    return ((ServerProcedureInterface)proc).getServerName();
  }

  // ============================================================================
  //  Peer Queue Lookup Helpers
  // ============================================================================
  private PeerQueue getPeerQueue(String peerId) {
    PeerQueue node = AvlTree.get(peerMap, peerId, PEER_QUEUE_KEY_COMPARATOR);
    if (node != null) {
      return node;
    }
    node = new PeerQueue(peerId, locking.getPeerLock(peerId));
    peerMap = AvlTree.insert(peerMap, node);
    return node;
  }

  private void removePeerQueue(String peerId) {
    peerMap = AvlTree.remove(peerMap, peerId, PEER_QUEUE_KEY_COMPARATOR);
    locking.removePeerLock(peerId);
  }


  private static boolean isPeerProcedure(Procedure<?> proc) {
    return proc instanceof PeerProcedureInterface;
  }

  private static String getPeerId(Procedure<?> proc) {
    return ((PeerProcedureInterface) proc).getPeerId();
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

  public static class ServerQueue extends Queue<ServerName> {
    public ServerQueue(ServerName serverName, LockStatus serverLock) {
      super(serverName, serverLock);
    }

    @Override
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

  private static class TableQueueKeyComparator implements AvlKeyComparator<TableQueue> {
    @Override
    public int compareKey(TableQueue node, Object key) {
      return node.compareKey((TableName)key);
    }
  }

  public static class TableQueue extends Queue<TableName> {
    private final LockStatus namespaceLockStatus;

    public TableQueue(TableName tableName, int priority, LockStatus tableLock,
        LockStatus namespaceLockStatus) {
      super(tableName, priority, tableLock);
      this.namespaceLockStatus = namespaceLockStatus;
    }

    @Override
    public boolean isAvailable() {
      // if there are no items in the queue, or the namespace is locked.
      // we can't execute operation on this table
      if (isEmpty() || namespaceLockStatus.hasExclusiveLock()) {
        return false;
      }

      if (getLockStatus().hasExclusiveLock()) {
        // if we have an exclusive lock already taken
        // only child of the lock owner can be executed
        final Procedure nextProc = peek();
        return nextProc != null && getLockStatus().hasLockAccess(nextProc);
      }

      // no xlock
      return true;
    }

    @Override
    public boolean requireExclusiveLock(Procedure proc) {
      return requireTableExclusiveLock((TableProcedureInterface)proc);
    }
  }

  private static class PeerQueueKeyComparator implements AvlKeyComparator<PeerQueue> {

    @Override
    public int compareKey(PeerQueue node, Object key) {
      return node.compareKey((String) key);
    }
  }

  public static class PeerQueue extends Queue<String> {

    public PeerQueue(String peerId, LockStatus lockStatus) {
      super(peerId, lockStatus);
    }

    @Override
    public boolean requireExclusiveLock(Procedure proc) {
      return requirePeerExclusiveLock((PeerProcedureInterface) proc);
    }

    @Override
    public boolean isAvailable() {
      if (isEmpty()) {
        return false;
      }
      if (getLockStatus().hasExclusiveLock()) {
        // if we have an exclusive lock already taken
        // only child of the lock owner can be executed
        Procedure nextProc = peek();
        return nextProc != null && getLockStatus().hasLockAccess(nextProc);
      }
      return true;
    }
  }

  // ============================================================================
  //  Table Locking Helpers
  // ============================================================================
  /**
   * @param proc must not be null
   */
  private static boolean requireTableExclusiveLock(TableProcedureInterface proc) {
    switch (proc.getTableOperationType()) {
      case CREATE:
      case DELETE:
      case DISABLE:
      case ENABLE:
        return true;
      case EDIT:
        // we allow concurrent edit on the NS table
        return !proc.getTableName().equals(TableName.NAMESPACE_TABLE_NAME);
      case READ:
        return false;
      // region operations are using the shared-lock on the table
      // and then they will grab an xlock on the region.
      case REGION_SPLIT:
      case REGION_MERGE:
      case REGION_ASSIGN:
      case REGION_UNASSIGN:
      case REGION_EDIT:
      case REGION_GC:
      case MERGED_REGIONS_GC:
        return false;
      default:
        break;
    }
    throw new UnsupportedOperationException("unexpected type " +
        proc.getTableOperationType());
  }

  /**
   * Get lock info for a resource of specified type and name and log details
   */
  protected void logLockedResource(LockedResourceType resourceType, String resourceName) {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    LockedResource lockedResource = getLockResource(resourceType, resourceName);
    if (lockedResource != null) {
      String msg = resourceType.toString() + " '" + resourceName + "', shared lock count=" +
          lockedResource.getSharedLockCount();

      Procedure<?> proc = lockedResource.getExclusiveLockOwnerProcedure();
      if (proc != null) {
        msg += ", exclusively locked by procId=" + proc.getProcId();
      }
      LOG.debug(msg);
    }
  }

  /**
   * Suspend the procedure if the specified table is already locked.
   * Other operations in the table-queue will be executed after the lock is released.
   * @param procedure the procedure trying to acquire the lock
   * @param table Table to lock
   * @return true if the procedure has to wait for the table to be available
   */
  public boolean waitTableExclusiveLock(final Procedure procedure, final TableName table) {
    schedLock();
    try {
      final String namespace = table.getNamespaceAsString();
      final LockAndQueue namespaceLock = locking.getNamespaceLock(namespace);
      final LockAndQueue tableLock = locking.getTableLock(table);
      if (!namespaceLock.trySharedLock()) {
        waitProcedure(namespaceLock, procedure);
        logLockedResource(LockedResourceType.NAMESPACE, namespace);
        return true;
      }
      if (!tableLock.tryExclusiveLock(procedure)) {
        namespaceLock.releaseSharedLock();
        waitProcedure(tableLock, procedure);
        logLockedResource(LockedResourceType.TABLE, table.getNameAsString());
        return true;
      }
      removeFromRunQueue(tableRunQueue, getTableQueue(table));
      return false;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified table
   * @param procedure the procedure releasing the lock
   * @param table the name of the table that has the exclusive lock
   */
  public void wakeTableExclusiveLock(final Procedure procedure, final TableName table) {
    schedLock();
    try {
      final LockAndQueue namespaceLock = locking.getNamespaceLock(table.getNamespaceAsString());
      final LockAndQueue tableLock = locking.getTableLock(table);
      int waitingCount = 0;

      if (!tableLock.hasParentLock(procedure)) {
        tableLock.releaseExclusiveLock(procedure);
        waitingCount += wakeWaitingProcedures(tableLock);
      }
      if (namespaceLock.releaseSharedLock()) {
        waitingCount += wakeWaitingProcedures(namespaceLock);
      }
      addToRunQueue(tableRunQueue, getTableQueue(table));
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  /**
   * Suspend the procedure if the specified table is already locked.
   * other "read" operations in the table-queue may be executed concurrently,
   * @param procedure the procedure trying to acquire the lock
   * @param table Table to lock
   * @return true if the procedure has to wait for the table to be available
   */
  public boolean waitTableSharedLock(final Procedure procedure, final TableName table) {
    return waitTableQueueSharedLock(procedure, table) == null;
  }

  private TableQueue waitTableQueueSharedLock(final Procedure procedure, final TableName table) {
    schedLock();
    try {
      final LockAndQueue namespaceLock = locking.getNamespaceLock(table.getNamespaceAsString());
      final LockAndQueue tableLock = locking.getTableLock(table);
      if (!namespaceLock.trySharedLock()) {
        waitProcedure(namespaceLock, procedure);
        return null;
      }

      if (!tableLock.trySharedLock()) {
        namespaceLock.releaseSharedLock();
        waitProcedure(tableLock, procedure);
        return null;
      }

      return getTableQueue(table);
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified table
   * @param procedure the procedure releasing the lock
   * @param table the name of the table that has the shared lock
   */
  public void wakeTableSharedLock(final Procedure procedure, final TableName table) {
    schedLock();
    try {
      final LockAndQueue namespaceLock = locking.getNamespaceLock(table.getNamespaceAsString());
      final LockAndQueue tableLock = locking.getTableLock(table);
      int waitingCount = 0;
      if (tableLock.releaseSharedLock()) {
        addToRunQueue(tableRunQueue, getTableQueue(table));
        waitingCount += wakeWaitingProcedures(tableLock);
      }
      if (namespaceLock.releaseSharedLock()) {
        waitingCount += wakeWaitingProcedures(namespaceLock);
      }
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
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
      final TableQueue queue = getTableQueue(table);
      final LockAndQueue tableLock = locking.getTableLock(table);
      if (queue == null) return true;

      if (queue.isEmpty() && tableLock.tryExclusiveLock(procedure)) {
        // remove the table from the run-queue and the map
        if (AvlIterableList.isLinked(queue)) {
          tableRunQueue.remove(queue);
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
  public boolean waitRegion(final Procedure procedure, final RegionInfo regionInfo) {
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
      final RegionInfo... regionInfo) {
    Arrays.sort(regionInfo, RegionInfo.COMPARATOR);
    schedLock();
    try {
      // If there is parent procedure, it would have already taken xlock, so no need to take
      // shared lock here. Otherwise, take shared lock.
      if (!procedure.hasParent()
          && waitTableQueueSharedLock(procedure, table) == null) {
          return true;
      }

      // acquire region xlocks or wait
      boolean hasLock = true;
      final LockAndQueue[] regionLocks = new LockAndQueue[regionInfo.length];
      for (int i = 0; i < regionInfo.length; ++i) {
        LOG.info(procedure + " " + table + " " + regionInfo[i].getRegionNameAsString());
        assert table != null;
        assert regionInfo[i] != null;
        assert regionInfo[i].getTable() != null;
        assert regionInfo[i].getTable().equals(table): regionInfo[i] + " " + procedure;
        assert i == 0 || regionInfo[i] != regionInfo[i - 1] : "duplicate region: " + regionInfo[i];

        regionLocks[i] = locking.getRegionLock(regionInfo[i].getEncodedName());
        if (!regionLocks[i].tryExclusiveLock(procedure)) {
          waitProcedure(regionLocks[i], procedure);
          hasLock = false;
          while (i-- > 0) {
            regionLocks[i].releaseExclusiveLock(procedure);
          }
          break;
        }
      }

      if (!hasLock && !procedure.hasParent()) {
        wakeTableSharedLock(procedure, table);
      }
      return !hasLock;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified region
   * @param procedure the procedure that was holding the region
   * @param regionInfo the region the procedure was holding
   */
  public void wakeRegion(final Procedure procedure, final RegionInfo regionInfo) {
    wakeRegions(procedure, regionInfo.getTable(), regionInfo);
  }

  /**
   * Wake the procedures waiting for the specified regions
   * @param procedure the procedure that was holding the regions
   * @param regionInfo the list of regions the procedure was holding
   */
  public void wakeRegions(final Procedure procedure,final TableName table,
      final RegionInfo... regionInfo) {
    Arrays.sort(regionInfo, RegionInfo.COMPARATOR);
    schedLock();
    try {
      int numProcs = 0;
      final Procedure[] nextProcs = new Procedure[regionInfo.length];
      for (int i = 0; i < regionInfo.length; ++i) {
        assert regionInfo[i].getTable().equals(table);
        assert i == 0 || regionInfo[i] != regionInfo[i - 1] : "duplicate region: " + regionInfo[i];

        LockAndQueue regionLock = locking.getRegionLock(regionInfo[i].getEncodedName());
        if (regionLock.releaseExclusiveLock(procedure)) {
          if (!regionLock.isEmpty()) {
            // release one procedure at the time since regions has an xlock
            nextProcs[numProcs++] = regionLock.removeFirst();
          } else {
            locking.removeRegionLock(regionInfo[i].getEncodedName());
          }
        }
      }

      // awake procedures if any
      for (int i = numProcs - 1; i >= 0; --i) {
        wakeProcedure(nextProcs[i]);
      }
      wakePollIfNeeded(numProcs);
      if (!procedure.hasParent()) {
        // release the table shared-lock.
        // (if we have a parent, it is holding an xlock so we didn't take the shared-lock)
        wakeTableSharedLock(procedure, table);
      }
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  //  Namespace Locking Helpers
  // ============================================================================
  /**
   * Suspend the procedure if the specified namespace is already locked.
   * @see #wakeNamespaceExclusiveLock(Procedure,String)
   * @param procedure the procedure trying to acquire the lock
   * @param namespace Namespace to lock
   * @return true if the procedure has to wait for the namespace to be available
   */
  public boolean waitNamespaceExclusiveLock(final Procedure procedure, final String namespace) {
    schedLock();
    try {
      final LockAndQueue systemNamespaceTableLock =
          locking.getTableLock(TableName.NAMESPACE_TABLE_NAME);
      if (!systemNamespaceTableLock.trySharedLock()) {
        waitProcedure(systemNamespaceTableLock, procedure);
        logLockedResource(LockedResourceType.TABLE,
            TableName.NAMESPACE_TABLE_NAME.getNameAsString());
        return true;
      }

      final LockAndQueue namespaceLock = locking.getNamespaceLock(namespace);
      if (!namespaceLock.tryExclusiveLock(procedure)) {
        systemNamespaceTableLock.releaseSharedLock();
        waitProcedure(namespaceLock, procedure);
        logLockedResource(LockedResourceType.NAMESPACE, namespace);
        return true;
      }
      return false;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified namespace
   * @see #waitNamespaceExclusiveLock(Procedure,String)
   * @param procedure the procedure releasing the lock
   * @param namespace the namespace that has the exclusive lock
   */
  public void wakeNamespaceExclusiveLock(final Procedure procedure, final String namespace) {
    schedLock();
    try {
      final LockAndQueue namespaceLock = locking.getNamespaceLock(namespace);
      final LockAndQueue systemNamespaceTableLock =
          locking.getTableLock(TableName.NAMESPACE_TABLE_NAME);
      namespaceLock.releaseExclusiveLock(procedure);
      int waitingCount = 0;
      if (systemNamespaceTableLock.releaseSharedLock()) {
        addToRunQueue(tableRunQueue, getTableQueue(TableName.NAMESPACE_TABLE_NAME));
        waitingCount += wakeWaitingProcedures(systemNamespaceTableLock);
      }
      waitingCount += wakeWaitingProcedures(namespaceLock);
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  //  Server Locking Helpers
  // ============================================================================
  /**
   * Try to acquire the exclusive lock on the specified server.
   * @see #wakeServerExclusiveLock(Procedure,ServerName)
   * @param procedure the procedure trying to acquire the lock
   * @param serverName Server to lock
   * @return true if the procedure has to wait for the server to be available
   */
  public boolean waitServerExclusiveLock(final Procedure<?> procedure,
      final ServerName serverName) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getServerLock(serverName);
      if (lock.tryExclusiveLock(procedure)) {
        removeFromRunQueue(serverRunQueue, getServerQueue(serverName));
        return false;
      }
      waitProcedure(lock, procedure);
      logLockedResource(LockedResourceType.SERVER, serverName.getServerName());
      return true;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified server
   * @see #waitServerExclusiveLock(Procedure,ServerName)
   * @param procedure the procedure releasing the lock
   * @param serverName the server that has the exclusive lock
   */
  public void wakeServerExclusiveLock(final Procedure<?> procedure, final ServerName serverName) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getServerLock(serverName);
      lock.releaseExclusiveLock(procedure);
      addToRunQueue(serverRunQueue, getServerQueue(serverName));
      int waitingCount = wakeWaitingProcedures(lock);
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  //  Peer Locking Helpers
  // ============================================================================

  private static boolean requirePeerExclusiveLock(PeerProcedureInterface proc) {
    return proc.getPeerOperationType() != PeerOperationType.REFRESH;
  }

  /**
   * Try to acquire the exclusive lock on the specified peer.
   * @see #wakePeerExclusiveLock(Procedure, String)
   * @param procedure the procedure trying to acquire the lock
   * @param peerId peer to lock
   * @return true if the procedure has to wait for the peer to be available
   */
  public boolean waitPeerExclusiveLock(Procedure<?> procedure, String peerId) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getPeerLock(peerId);
      if (lock.tryExclusiveLock(procedure)) {
        removeFromRunQueue(peerRunQueue, getPeerQueue(peerId));
        return false;
      }
      waitProcedure(lock, procedure);
      logLockedResource(LockedResourceType.PEER, peerId);
      return true;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified peer
   * @see #waitPeerExclusiveLock(Procedure, String)
   * @param procedure the procedure releasing the lock
   * @param peerId the peer that has the exclusive lock
   */
  public void wakePeerExclusiveLock(Procedure<?> procedure, String peerId) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getPeerLock(peerId);
      lock.releaseExclusiveLock(procedure);
      addToRunQueue(peerRunQueue, getPeerQueue(peerId));
      int waitingCount = wakeWaitingProcedures(lock);
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  //  Generic Helpers
  // ============================================================================
  private static abstract class Queue<TKey extends Comparable<TKey>>
      extends AvlLinkedNode<Queue<TKey>> {

    /**
     * @param proc must not be null
     */
    abstract boolean requireExclusiveLock(Procedure proc);

    private final TKey key;
    private final int priority;
    private final ProcedureDeque runnables = new ProcedureDeque();
    // Reference to status of lock on entity this queue represents.
    private final LockStatus lockStatus;

    public Queue(TKey key, LockStatus lockStatus) {
      this(key, 1, lockStatus);
    }

    public Queue(TKey key, int priority, LockStatus lockStatus) {
      this.key = key;
      this.priority = priority;
      this.lockStatus = lockStatus;
    }

    protected TKey getKey() {
      return key;
    }

    protected int getPriority() {
      return priority;
    }

    protected LockStatus getLockStatus() {
      return lockStatus;
    }

    // This should go away when we have the new AM and its events
    // and we move xlock to the lock-event-queue.
    public boolean isAvailable() {
      return !lockStatus.hasExclusiveLock() && !isEmpty();
    }

    // ======================================================================
    //  Functions to handle procedure queue
    // ======================================================================
    public void add(final Procedure proc, final boolean addToFront) {
      if (addToFront) {
        runnables.addFirst(proc);
      } else {
        runnables.addLast(proc);
      }
    }

    public Procedure peek() {
      return runnables.peek();
    }

    public Procedure poll() {
      return runnables.poll();
    }

    public boolean isEmpty() {
      return runnables.isEmpty();
    }

    public int size() {
      return runnables.size();
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
      return String.format("%s(%s, xlock=%s sharedLock=%s size=%s)",
          getClass().getSimpleName(), key,
          lockStatus.hasExclusiveLock() ?
              "true (" + lockStatus.getExclusiveLockProcIdOwner() + ")" : "false",
          lockStatus.getSharedLockCount(), size());
    }
  }

  /**
   * Locks on namespaces, tables, and regions.
   * Since LockAndQueue implementation is NOT thread-safe, schedLock() guards all calls to these
   * locks.
   */
  private static class SchemaLocking {
    final Map<ServerName, LockAndQueue> serverLocks = new HashMap<>();
    final Map<String, LockAndQueue> namespaceLocks = new HashMap<>();
    final Map<TableName, LockAndQueue> tableLocks = new HashMap<>();
    // Single map for all regions irrespective of tables. Key is encoded region name.
    final Map<String, LockAndQueue> regionLocks = new HashMap<>();
    final Map<String, LockAndQueue> peerLocks = new HashMap<>();

    private <T> LockAndQueue getLock(Map<T, LockAndQueue> map, T key) {
      LockAndQueue lock = map.get(key);
      if (lock == null) {
        lock = new LockAndQueue();
        map.put(key, lock);
      }
      return lock;
    }

    LockAndQueue getTableLock(TableName tableName) {
      return getLock(tableLocks, tableName);
    }

    LockAndQueue removeTableLock(TableName tableName) {
      return tableLocks.remove(tableName);
    }

    LockAndQueue getNamespaceLock(String namespace) {
      return getLock(namespaceLocks, namespace);
    }

    LockAndQueue getRegionLock(String encodedRegionName) {
      return getLock(regionLocks, encodedRegionName);
    }

    LockAndQueue removeRegionLock(String encodedRegionName) {
      return regionLocks.remove(encodedRegionName);
    }

    LockAndQueue getServerLock(ServerName serverName) {
      return getLock(serverLocks, serverName);
    }

    LockAndQueue getPeerLock(String peerId) {
      return getLock(peerLocks, peerId);
    }

    LockAndQueue removePeerLock(String peerId) {
      return peerLocks.remove(peerId);
    }

    /**
     * Removes all locks by clearing the maps.
     * Used when procedure executor is stopped for failure and recovery testing.
     */
    @VisibleForTesting
    void clear() {
      serverLocks.clear();
      namespaceLocks.clear();
      tableLocks.clear();
      regionLocks.clear();
      peerLocks.clear();
    }

    @Override
    public String toString() {
      return "serverLocks=" + filterUnlocked(this.serverLocks) +
        ", namespaceLocks=" + filterUnlocked(this.namespaceLocks) +
        ", tableLocks=" + filterUnlocked(this.tableLocks) +
        ", regionLocks=" + filterUnlocked(this.regionLocks) +
        ", peerLocks=" + filterUnlocked(this.peerLocks);
    }

    private String filterUnlocked(Map<?, LockAndQueue> locks) {
      StringBuilder sb = new StringBuilder("{");
      int initialLength = sb.length();
      for (Map.Entry<?, LockAndQueue> entry: locks.entrySet()) {
        if (!entry.getValue().isLocked()) continue;
        if (sb.length() > initialLength) sb.append(", ");
          sb.append("{");
          sb.append(entry.getKey());
          sb.append("=");
          sb.append(entry.getValue());
          sb.append("}");
        }
        sb.append("}");
        return sb.toString();
     }
  }

  // ======================================================================
  //  Helper Data Structures
  // ======================================================================

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

  /**
   * For debugging. Expensive.
    * @throws IOException
    */
  @VisibleForTesting
  public String dumpLocks() throws IOException {
    schedLock();
    try {
      // TODO: Refactor so we stream out locks for case when millions; i.e. take a PrintWriter
      return this.locking.toString();
    } finally {
      schedUnlock();
    }
  }
}
