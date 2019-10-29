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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.MetaRegionsNotAvailableException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.types.CopyOnWriteArrayMap;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * A cache of meta region location metadata. This cache is used to serve 'GetMetaLocations' RPCs
 * from clients. Registers a listener on ZK to track changes to the meta table znodes. Clients
 * are expected to retry if the meta information is stale. This class is thread-safe.
 */
@InterfaceAudience.Private
public class MetaRegionLocationCache extends ZKListener {

  private static final Logger LOG = LoggerFactory.getLogger(MetaRegionLocationCache.class);

  // Maximum number of times we retry when ZK operation times out. Should this be configurable?
  private static final int MAX_ZK_META_FETCH_RETRIES = 10;

  private ZKWatcher watcher;
  // Cached meta region locations indexed by replica ID.
  // CopyOnWriteArrayMap ensures synchronization during updates and a consistent snapshot during
  // client requests. Even though CopyOnWriteArrayMap copies the data structure for every write,
  // that should be OK since the size of the list is often small and mutations are not too often
  // and we do not need to block client requests while mutations are in progress.
  CopyOnWriteArrayMap<Integer, HRegionLocation> cachedMetaLocations;

  private enum ZNodeOpType {
    INIT,
    CREATED,
    CHANGED,
    DELETED
  };

  public MetaRegionLocationCache(ZKWatcher zkWatcher) {
    super(zkWatcher);
    watcher = zkWatcher;
    cachedMetaLocations = new CopyOnWriteArrayMap<>();
    watcher.registerListener(this);
    // Populate the initial snapshot of data from meta znodes.
    // This is needed because stand-by masters can potentially start after the initial znode
    // creation.
    populateInitialMetaLocations();
  }

  private void populateInitialMetaLocations() {
    int retries = 0;
    while (retries++ < MAX_ZK_META_FETCH_RETRIES) {
      try {
        List<String> znodes = watcher.getMetaReplicaNodes();
        for (String znode: znodes) {
          String path = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, znode);
          updateMetaLocation(path, ZNodeOpType.INIT);
        }
        break;
      } catch (KeeperException.OperationTimeoutException e) {
        LOG.warn("Timed out connecting to ZK cluster", e);

      } catch (KeeperException e) {
        LOG.warn("Error populating initial meta locations", e);
        break;
      }
    }
  }

  private void updateMetaLocation(String path, ZNodeOpType opType) {
    if (!isValidMetaZNode(path)) {
      return;
    }
    LOG.info("Meta znode for path {}: {}", path, opType.name());
    int replicaId = watcher.getZNodePaths().getMetaReplicaIdFromPath(path);
    if (opType == ZNodeOpType.DELETED) {
      cachedMetaLocations.remove(replicaId);
      return;
    }
    RegionState state = null;
    int retries = 0;
    while (retries++ < MAX_ZK_META_FETCH_RETRIES) {
      try {
        state = MetaTableLocator.getMetaRegionState(watcher, replicaId);
        break;
      } catch (KeeperException.OperationTimeoutException oe) {
        // LOG and retry.
        LOG.warn("Timed out fetching meta location information for path {}", path, oe);
      } catch (KeeperException e) {
        LOG.error("Error getting meta location for path {}", path, e);
        break;
      }
    }
    if (state == null) {
      cachedMetaLocations.put(replicaId, null);
      return;
    }
    cachedMetaLocations.put(
        replicaId, new HRegionLocation(state.getRegion(), state.getServerName()));
  }

  /**
   * Converts the current cache snapshot into a GetMetaLocations() RPC return payload.
   * @return Protobuf serialized list of cached meta HRegionLocations
   * @throws MetaRegionsNotAvailableException if the cache is not populated.
   */
  public List<HBaseProtos.RegionLocation> getCachedMetaRegionLocations()
      throws MetaRegionsNotAvailableException {
    ConcurrentNavigableMap<Integer, HRegionLocation> snapshot =
        cachedMetaLocations.tailMap(cachedMetaLocations.firstKey(), true);
    if (snapshot == null || snapshot.isEmpty()) {
      // This could be possible if the master has not successfully initialized yet or meta region
      // is stuck in some weird state.
      throw new MetaRegionsNotAvailableException("Meta cache is empty");
    }
    List<HBaseProtos.RegionLocation> result = new ArrayList<>();
    // Handle missing replicas, if any?
    snapshot.values().stream().forEach(
        location -> result.add(ProtobufUtil.toRegionLocation(location)));
    return result;
  }

  /**
   * Helper to check if the given 'path' corresponds to a meta znode. This listener is only
   * interested in changes to meta znodes.
   */
  private boolean isValidMetaZNode(String path) {
    return watcher.getZNodePaths().isAnyMetaReplicaZNode(path);
  }

  /**
   * Test helper to invalidate cached metadata for a given meta replica ID. This is done
   * synchronously with the meta region moves in tests to avoid any flaky tests.
   */
  @VisibleForTesting
  public void invalidateMetaReplica(int replicaId) {
    String path = watcher.getZNodePaths().getZNodeForReplica(replicaId);
    nodeDataChanged(path);
  }

  @Override
  public void nodeCreated(String path) {
    updateMetaLocation(path, ZNodeOpType.CREATED);
  }

  @Override
  public void nodeDeleted(String path) {
    updateMetaLocation(path, ZNodeOpType.DELETED);
  }

  @Override
  public void nodeDataChanged(String path) {
    updateMetaLocation(path, ZNodeOpType.CHANGED);
  }
}
