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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.procedure2.LockAndQueue;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.LockedResourceType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * <p>
 * Locks on namespaces, tables, and regions.
 * </p>
 * <p>
 * Since LockAndQueue implementation is NOT thread-safe, schedLock() guards all calls to these
 * locks.
 * </p>
 */
@InterfaceAudience.Private
class SchemaLocking {

  private final Function<Long, Procedure<?>> procedureRetriever;
  private final Map<ServerName, LockAndQueue> serverLocks = new HashMap<>();
  private final Map<String, LockAndQueue> namespaceLocks = new HashMap<>();
  private final Map<TableName, LockAndQueue> tableLocks = new HashMap<>();
  // Single map for all regions irrespective of tables. Key is encoded region name.
  private final Map<String, LockAndQueue> regionLocks = new HashMap<>();
  private final Map<String, LockAndQueue> peerLocks = new HashMap<>();
  private final LockAndQueue metaLock;

  public SchemaLocking(Function<Long, Procedure<?>> procedureRetriever) {
    this.procedureRetriever = procedureRetriever;
    this.metaLock = new LockAndQueue(procedureRetriever);
  }

  private <T> LockAndQueue getLock(Map<T, LockAndQueue> map, T key) {
    LockAndQueue lock = map.get(key);
    if (lock == null) {
      lock = new LockAndQueue(procedureRetriever);
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

  /**
   * @deprecated only used for {@link RecoverMetaProcedure}. Should be removed along with
   *             {@link RecoverMetaProcedure}.
   */
  @Deprecated
  LockAndQueue getMetaLock() {
    return metaLock;
  }

  LockAndQueue removeRegionLock(String encodedRegionName) {
    return regionLocks.remove(encodedRegionName);
  }

  LockAndQueue getServerLock(ServerName serverName) {
    return getLock(serverLocks, serverName);
  }

  LockAndQueue removeServerLock(ServerName serverName) {
    return serverLocks.remove(serverName);
  }

  LockAndQueue getPeerLock(String peerId) {
    return getLock(peerLocks, peerId);
  }

  LockAndQueue removePeerLock(String peerId) {
    return peerLocks.remove(peerId);
  }

  private LockedResource createLockedResource(LockedResourceType resourceType, String resourceName,
      LockAndQueue queue) {
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

    queue.filterWaitingQueue(p -> p instanceof LockProcedure)
      .forEachOrdered(waitingProcedures::add);

    return new LockedResource(resourceType, resourceName, lockType, exclusiveLockOwnerProcedure,
      sharedLockCount, waitingProcedures);
  }

  private <T> void addToLockedResources(List<LockedResource> lockedResources,
      Map<T, LockAndQueue> locks, Function<T, String> keyTransformer,
      LockedResourceType resourcesType) {
    locks.entrySet().stream().filter(e -> e.getValue().isLocked())
      .map(e -> createLockedResource(resourcesType, keyTransformer.apply(e.getKey()), e.getValue()))
      .forEachOrdered(lockedResources::add);
  }

  /**
   * List lock queues.
   * @return the locks
   */
  List<LockedResource> getLocks() {
    List<LockedResource> lockedResources = new ArrayList<>();
    addToLockedResources(lockedResources, serverLocks, sn -> sn.getServerName(),
      LockedResourceType.SERVER);
    addToLockedResources(lockedResources, namespaceLocks, Function.identity(),
      LockedResourceType.NAMESPACE);
    addToLockedResources(lockedResources, tableLocks, tn -> tn.getNameAsString(),
      LockedResourceType.TABLE);
    addToLockedResources(lockedResources, regionLocks, Function.identity(),
      LockedResourceType.REGION);
    addToLockedResources(lockedResources, peerLocks, Function.identity(), LockedResourceType.PEER);
    addToLockedResources(lockedResources, ImmutableMap.of(TableName.META_TABLE_NAME, metaLock),
      tn -> tn.getNameAsString(), LockedResourceType.META);
    return lockedResources;
  }

  /**
   * @return {@link LockedResource} for resource of specified type & name. null if resource is not
   *         locked.
   */
  LockedResource getLockResource(LockedResourceType resourceType, String resourceName) {
    LockAndQueue queue;
    switch (resourceType) {
      case SERVER:
        queue = serverLocks.get(ServerName.valueOf(resourceName));
        break;
      case NAMESPACE:
        queue = namespaceLocks.get(resourceName);
        break;
      case TABLE:
        queue = tableLocks.get(TableName.valueOf(resourceName));
        break;
      case REGION:
        queue = regionLocks.get(resourceName);
        break;
      case PEER:
        queue = peerLocks.get(resourceName);
        break;
      case META:
        queue = metaLock;
      default:
        queue = null;
    }
    return queue != null ? createLockedResource(resourceType, resourceName, queue) : null;
  }

  /**
   * Removes all locks by clearing the maps. Used when procedure executor is stopped for failure and
   * recovery testing.
   */
  void clear() {
    serverLocks.clear();
    namespaceLocks.clear();
    tableLocks.clear();
    regionLocks.clear();
    peerLocks.clear();
  }

  @Override
  public String toString() {
    return "serverLocks=" + filterUnlocked(this.serverLocks) + ", namespaceLocks=" +
      filterUnlocked(this.namespaceLocks) + ", tableLocks=" + filterUnlocked(this.tableLocks) +
      ", regionLocks=" + filterUnlocked(this.regionLocks) + ", peerLocks=" +
      filterUnlocked(this.peerLocks) + ", metaLocks=" +
      filterUnlocked(ImmutableMap.of(TableName.META_TABLE_NAME, metaLock));
  }

  private String filterUnlocked(Map<?, LockAndQueue> locks) {
    StringBuilder sb = new StringBuilder("{");
    int initialLength = sb.length();
    for (Map.Entry<?, LockAndQueue> entry : locks.entrySet()) {
      if (!entry.getValue().isLocked()) {
        continue;
      }
      if (sb.length() > initialLength) {
        sb.append(", ");
      }
      sb.append("{").append(entry.getKey()).append("=").append(entry.getValue()).append("}");
    }
    sb.append("}");
    return sb.toString();
  }
}
