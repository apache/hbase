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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface.TableOperationType;
import org.apache.hadoop.hbase.procedure2.AbstractProcedureScheduler;
import org.apache.hadoop.hbase.procedure2.LockAndQueue;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.LockedResourceType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.util.AvlUtil.AvlIterableList;
import org.apache.hadoop.hbase.util.AvlUtil.AvlKeyComparator;
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

  private static final AvlKeyComparator<ServerQueue> SERVER_QUEUE_KEY_COMPARATOR =
    (n, k) -> n.compareKey((ServerName) k);
  private final static AvlKeyComparator<TableQueue> TABLE_QUEUE_KEY_COMPARATOR =
    (n, k) -> n.compareKey((TableName) k);
  private final static AvlKeyComparator<PeerQueue> PEER_QUEUE_KEY_COMPARATOR =
    (n, k) -> n.compareKey((String) k);
  private final static AvlKeyComparator<MetaQueue> META_QUEUE_KEY_COMPARATOR =
    (n, k) -> n.compareKey((TableName) k);

  private final FairQueue<ServerName> serverRunQueue = new FairQueue<>();
  private final FairQueue<TableName> tableRunQueue = new FairQueue<>();
  private final FairQueue<String> peerRunQueue = new FairQueue<>();
  private final FairQueue<TableName> metaRunQueue = new FairQueue<>();

  private final ServerQueue[] serverBuckets = new ServerQueue[128];
  private TableQueue tableMap = null;
  private PeerQueue peerMap = null;
  private MetaQueue metaMap = null;

  private final SchemaLocking locking = new SchemaLocking();

  @Override
  public void yield(final Procedure proc) {
    push(proc, isTableProcedure(proc), true);
  }

  @Override
  protected void enqueue(final Procedure proc, final boolean addFront) {
    if (isMetaProcedure(proc)) {
      doAdd(metaRunQueue, getMetaQueue(), proc, addFront);
    } else if (isTableProcedure(proc)) {
      doAdd(tableRunQueue, getTableQueue(getTableName(proc)), proc, addFront);
    } else if (isServerProcedure(proc)) {
      ServerProcedureInterface spi = (ServerProcedureInterface) proc;
      doAdd(serverRunQueue, getServerQueue(spi.getServerName(), spi), proc, addFront);
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
    return metaRunQueue.hasRunnables() || tableRunQueue.hasRunnables() ||
      serverRunQueue.hasRunnables() || peerRunQueue.hasRunnables();
  }

  @Override
  protected Procedure dequeue() {
    // meta procedure is always the first priority
    Procedure<?> pollResult = doPoll(metaRunQueue);
    // For now, let server handling have precedence over table handling; presumption is that it
    // is more important handling crashed servers than it is running the
    // enabling/disabling tables, etc.
    if (pollResult == null) {
      pollResult = doPoll(serverRunQueue);
    }
    if (pollResult == null) {
      pollResult = doPoll(peerRunQueue);
    }
    if (pollResult == null) {
      pollResult = doPoll(tableRunQueue);
    }
    return pollResult;
  }

  private <T extends Comparable<T>> Procedure<?> doPoll(final FairQueue<T> fairq) {
    final Queue<T> rq = fairq.poll();
    if (rq == null || !rq.isAvailable()) {
      return null;
    }

    final Procedure<?> pollResult = rq.peek();
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
      Procedure<?> nextProc = rq.peek();
      if (nextProc != null && !Procedure.haveSameParent(nextProc, pollResult)
          && nextProc.getRootProcId() != pollResult.getRootProcId()) {
        removeFromRunQueue(fairq, rq);
      }
    }

    return pollResult;
  }

  @Override
  public List<LockedResource> getLocks() {
    schedLock();
    try {
      return locking.getLocks();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public LockedResource getLockResource(LockedResourceType resourceType, String resourceName) {
    schedLock();
    try {
      return locking.getLockResource(resourceType, resourceName);
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

  private void clearQueue() {
    // Remove Servers
    for (int i = 0; i < serverBuckets.length; ++i) {
      clear(serverBuckets[i], serverRunQueue, SERVER_QUEUE_KEY_COMPARATOR);
      serverBuckets[i] = null;
    }

    // Remove Tables
    clear(tableMap, tableRunQueue, TABLE_QUEUE_KEY_COMPARATOR);
    tableMap = null;

    // Remove Peers
    clear(peerMap, peerRunQueue, PEER_QUEUE_KEY_COMPARATOR);
    peerMap = null;

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

  private int queueSize(Queue<?> head) {
    int count = 0;
    AvlTreeIterator<Queue<?>> iter = new AvlTreeIterator<Queue<?>>(head);
    while (iter.hasNext()) {
      count += iter.next().size();
    }
    return count;
  }

  @Override
  protected int queueSize() {
    int count = 0;
    for (ServerQueue serverMap : serverBuckets) {
      count += queueSize(serverMap);
    }
    count += queueSize(tableMap);
    count += queueSize(peerMap);
    count += queueSize(metaMap);
    return count;
  }

  @Override
  public void completionCleanup(final Procedure proc) {
    if (proc instanceof TableProcedureInterface) {
      TableProcedureInterface iProcTable = (TableProcedureInterface) proc;
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
      tryCleanupPeerQueue(getPeerId(proc), proc);
    } else if (proc instanceof ServerProcedureInterface) {
      tryCleanupServerQueue(getServerName(proc), proc);
    } else {
      // No cleanup for other procedure types, yet.
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

    node = new TableQueue(tableName, MasterProcedureUtil.getTablePriority(tableName),
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
  private ServerQueue getServerQueue(ServerName serverName, ServerProcedureInterface proc) {
    final int index = getBucketIndex(serverBuckets, serverName.hashCode());
    ServerQueue node = AvlTree.get(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
    if (node != null) {
      return node;
    }
    int priority;
    if (proc != null) {
      priority = MasterProcedureUtil.getServerPriority(proc);
    } else {
      LOG.warn("Usually this should not happen as proc can only be null when calling from " +
        "wait/wake lock, which means at least we should have one procedure in the queue which " +
        "wants to acquire the lock or just released the lock.");
      priority = 1;
    }
    node = new ServerQueue(serverName, priority, locking.getServerLock(serverName));
    serverBuckets[index] = AvlTree.insert(serverBuckets[index], node);
    return node;
  }

  private void removeServerQueue(ServerName serverName) {
    int index = getBucketIndex(serverBuckets, serverName.hashCode());
    serverBuckets[index] =
      AvlTree.remove(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
    locking.removeServerLock(serverName);
  }

  private void tryCleanupServerQueue(ServerName serverName, Procedure<?> proc) {
    schedLock();
    try {
      int index = getBucketIndex(serverBuckets, serverName.hashCode());
      ServerQueue node = AvlTree.get(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
      if (node == null) {
        return;
      }

      LockAndQueue lock = locking.getServerLock(serverName);
      if (node.isEmpty() && lock.tryExclusiveLock(proc)) {
        removeFromRunQueue(serverRunQueue, node);
        removeServerQueue(serverName);
      }
    } finally {
      schedUnlock();
    }
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

  private void tryCleanupPeerQueue(String peerId, Procedure<?> procedure) {
    schedLock();
    try {
      PeerQueue queue = AvlTree.get(peerMap, peerId, PEER_QUEUE_KEY_COMPARATOR);
      if (queue == null) {
        return;
      }

      final LockAndQueue lock = locking.getPeerLock(peerId);
      if (queue.isEmpty() && lock.tryExclusiveLock(procedure)) {
        removeFromRunQueue(peerRunQueue, queue);
        removePeerQueue(peerId);
      }
    } finally {
      schedUnlock();
    }
  }

  private static boolean isPeerProcedure(Procedure<?> proc) {
    return proc instanceof PeerProcedureInterface;
  }

  private static String getPeerId(Procedure<?> proc) {
    return ((PeerProcedureInterface) proc).getPeerId();
  }

  // ============================================================================
  //  Meta Queue Lookup Helpers
  // ============================================================================
  private MetaQueue getMetaQueue() {
    MetaQueue node = AvlTree.get(metaMap, TableName.META_TABLE_NAME, META_QUEUE_KEY_COMPARATOR);
    if (node != null) {
      return node;
    }
    node = new MetaQueue(locking.getMetaLock());
    metaMap = AvlTree.insert(metaMap, node);
    return node;
  }

  private static boolean isMetaProcedure(Procedure<?> proc) {
    return proc instanceof MetaProcedureInterface;
  }
  // ============================================================================
  //  Table Locking Helpers
  // ============================================================================
  /**
   * Get lock info for a resource of specified type and name and log details
   */
  private void logLockedResource(LockedResourceType resourceType, String resourceName) {
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
  public boolean waitTableExclusiveLock(final Procedure<?> procedure, final TableName table) {
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
  public void wakeTableExclusiveLock(final Procedure<?> procedure, final TableName table) {
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
  public boolean waitTableSharedLock(final Procedure<?> procedure, final TableName table) {
    return waitTableQueueSharedLock(procedure, table) == null;
  }

  private TableQueue waitTableQueueSharedLock(final Procedure<?> procedure, final TableName table) {
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
  public void wakeTableSharedLock(final Procedure<?> procedure, final TableName table) {
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
  boolean markTableAsDeleted(final TableName table, final Procedure<?> procedure) {
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
  public boolean waitRegion(final Procedure<?> procedure, final RegionInfo regionInfo) {
    return waitRegions(procedure, regionInfo.getTable(), regionInfo);
  }

  /**
   * Suspend the procedure if the specified set of regions are already locked.
   * @param procedure the procedure trying to acquire the lock on the regions
   * @param table the table name of the regions we are trying to lock
   * @param regionInfo the list of regions we are trying to lock
   * @return true if the procedure has to wait for the regions to be available
   */
  public boolean waitRegions(final Procedure<?> procedure, final TableName table,
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
        LOG.info("{} checking lock on {}", procedure, regionInfo[i].getEncodedName());
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
  public void wakeRegion(final Procedure<?> procedure, final RegionInfo regionInfo) {
    wakeRegions(procedure, regionInfo.getTable(), regionInfo);
  }

  /**
   * Wake the procedures waiting for the specified regions
   * @param procedure the procedure that was holding the regions
   * @param regionInfo the list of regions the procedure was holding
   */
  public void wakeRegions(final Procedure<?> procedure,final TableName table,
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
  public boolean waitNamespaceExclusiveLock(final Procedure<?> procedure, final String namespace) {
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
  public void wakeNamespaceExclusiveLock(final Procedure<?> procedure, final String namespace) {
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
        // We do not need to create a new queue so just pass null, as in tests we may pass
        // procedures other than ServerProcedureInterface
        removeFromRunQueue(serverRunQueue, getServerQueue(serverName, null));
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
      // We do not need to create a new queue so just pass null, as in tests we may pass procedures
      // other than ServerProcedureInterface
      addToRunQueue(serverRunQueue, getServerQueue(serverName, null));
      int waitingCount = wakeWaitingProcedures(lock);
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  //  Peer Locking Helpers
  // ============================================================================
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
  // Meta Locking Helpers
  // ============================================================================
  /**
   * Try to acquire the exclusive lock on meta.
   * @see #wakeMetaExclusiveLock(Procedure)
   * @param procedure the procedure trying to acquire the lock
   * @return true if the procedure has to wait for meta to be available
   * @deprecated only used for {@link RecoverMetaProcedure}. Should be removed along with
   *             {@link RecoverMetaProcedure}.
   */
  @Deprecated
  public boolean waitMetaExclusiveLock(Procedure<?> procedure) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getMetaLock();
      if (lock.tryExclusiveLock(procedure)) {
        removeFromRunQueue(metaRunQueue, getMetaQueue());
        return false;
      }
      waitProcedure(lock, procedure);
      logLockedResource(LockedResourceType.META, TableName.META_TABLE_NAME.getNameAsString());
      return true;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for meta.
   * @see #waitMetaExclusiveLock(Procedure)
   * @param procedure the procedure releasing the lock
   * @deprecated only used for {@link RecoverMetaProcedure}. Should be removed along with
   *             {@link RecoverMetaProcedure}.
   */
  @Deprecated
  public void wakeMetaExclusiveLock(Procedure<?> procedure) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getMetaLock();
      lock.releaseExclusiveLock(procedure);
      addToRunQueue(metaRunQueue, getMetaQueue());
      int waitingCount = wakeWaitingProcedures(lock);
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  /**
   * For debugging. Expensive.
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
