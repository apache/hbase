/*
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.types.CopyOnWriteArrayMap;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of meta region location metadata. Registers a listener on ZK to track changes to the
 * meta table znodes. Clients are expected to retry if the meta information is stale. This class
 * is thread-safe.
 */
@InterfaceAudience.Private
public class MetaRegionLocationCache extends ZKListener {

  private static final Logger LOG = LoggerFactory.getLogger(MetaRegionLocationCache.class);

  // Maximum number of times we retry when ZK operation times out.
  private static final int MAX_ZK_META_FETCH_RETRIES = 10;
  // Sleep interval ms between ZK operation retries.
  private static final int SLEEP_INTERVAL_MS_BETWEEN_RETRIES = 1000;
  private final RetryCounterFactory retryCounterFactory =
      new RetryCounterFactory(MAX_ZK_META_FETCH_RETRIES, SLEEP_INTERVAL_MS_BETWEEN_RETRIES);

  private final ZKWatcher watcher;
  // Cached meta region locations indexed by replica ID.
  // CopyOnWriteArrayMap ensures synchronization during updates and a consistent snapshot during
  // client requests. Even though CopyOnWriteArrayMap copies the data structure for every write,
  // that should be OK since the size of the list is often small and mutations are not too often
  // and we do not need to block client requests while mutations are in progress.
  private final CopyOnWriteArrayMap<Integer, HRegionLocation> cachedMetaLocations;

  private enum ZNodeOpType {
    INIT,
    CREATED,
    CHANGED,
    DELETED
  };

  MetaRegionLocationCache(ZKWatcher zkWatcher) {
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
    RetryCounter retryCounter = retryCounterFactory.create();
    List<String> znodes = null;
    do {
      try {
        znodes = watcher.getMetaReplicaNodes();
        break;
      } catch (KeeperException ke) {
        LOG.debug("Error populating intial meta locations", ke);
        try {
          retryCounter.sleepUntilNextRetry();
        } catch (InterruptedException ie) {
          LOG.error("Interrupted while populating intial meta locations", ie);
          return;
        }
        if (!retryCounter.shouldRetry()) {
          LOG.error("Error populating intial meta locations. Retries exhausted. Last error: ", ke);
          break;
        }
      }
    } while (retryCounter.shouldRetry());
    if (znodes == null) {
      return;
    }
    for (String znode: znodes) {
      String path = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, znode);
      updateMetaLocation(path, ZNodeOpType.INIT);
    }
  }

  /**
   * Gets the HRegionLocation for a given meta replica ID. Renews the watch on the znode for
   * future updates.
   * @param replicaId ReplicaID of the region.
   * @return HRegionLocation for the meta replica.
   * @throws KeeperException if there is any issue fetching/parsing the serialized data.
   */
  private HRegionLocation getMetaRegionLocation(int replicaId)
      throws KeeperException {
    ServerName serverName = null;
    try {
      byte[] data = ZKUtil.getDataAndWatch(watcher,
          watcher.getZNodePaths().getZNodeForReplica(replicaId));
      serverName = ProtobufUtil.parseMetaServerNameFrom(data);
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
    return new HRegionLocation(RegionReplicaUtil.getRegionInfoForReplica(
        RegionInfoBuilder.FIRST_META_REGIONINFO, replicaId), serverName);
  }

  private void updateMetaLocation(String path, ZNodeOpType opType) {
    if (!isValidMetaZNode(path)) {
      return;
    }
    LOG.debug("Updating meta znode for path {}: {}", path, opType.name());
    int replicaId = watcher.getZNodePaths().getMetaReplicaIdFromPath(path);
    RetryCounter retryCounter = retryCounterFactory.create();
    HRegionLocation location = null;
    do {
      try {
        if (opType == ZNodeOpType.DELETED) {
          if (!ZKUtil.watchAndCheckExists(watcher, path)) {
            // The path does not exist, we've set the watcher and we can break for now.
            break;
          }
          // If it is a transient error and the node appears right away, we fetch the
          // latest meta state.
        }
        location = getMetaRegionLocation(replicaId);
        break;
      } catch (KeeperException e) {
        LOG.debug("Error getting meta location for path {}", path, e);
        if (retryCounter.shouldRetry()) {
          try {
            retryCounter.sleepUntilNextRetry();
          } catch (InterruptedException ie) {
            LOG.error("Interrupted while updating meta location for path {}", path, ie);
            return;
          }
        } else {
          LOG.error("Error getting meta location for path {}. Retries exhausted.", path, e);
        }
      }
    } while (retryCounter.shouldRetry());
    if (location == null) {
      cachedMetaLocations.remove(replicaId);
      return;
    }
    cachedMetaLocations.put(replicaId, location);
  }

  /**
   * @return List of HRegionLocations for meta replica(s), null if the cache is empty.
   */
  public Optional<List<HRegionLocation>> getCachedMetaRegionLocations() {
    ConcurrentNavigableMap<Integer, HRegionLocation> snapshot =
        cachedMetaLocations.tailMap(cachedMetaLocations.firstKey());
    if (snapshot == null || snapshot.isEmpty()) {
      // This could be possible if the master has not successfully initialized yet or meta region
      // is stuck in some weird state.
      return Optional.empty();
    }
    List<HRegionLocation> result = new ArrayList<>();
    // Explicitly iterate instead of new ArrayList<>(snapshot.values()) because the underlying
    // ArrayValueCollection does not implement toArray().
    snapshot.values().forEach(location -> result.add(location));
    return Optional.of(result);
  }

  /**
   * Helper to check if the given 'path' corresponds to a meta znode. This listener is only
   * interested in changes to meta znodes.
   */
  private boolean isValidMetaZNode(String path) {
    return watcher.getZNodePaths().isAnyMetaReplicaZNode(path);
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
