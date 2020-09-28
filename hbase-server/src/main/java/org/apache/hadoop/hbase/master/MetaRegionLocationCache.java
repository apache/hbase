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
import java.util.concurrent.ThreadFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
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
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * A cache of meta region location metadata. Registers a listener on ZK to track changes to the
 * meta table znodes. Clients are expected to retry if the meta information is stale. This class
 * is thread-safe (a single instance of this class can be shared by multiple threads without race
 * conditions).
 */
@InterfaceAudience.Private
public class MetaRegionLocationCache extends ZKListener {

  private static final Logger LOG = LoggerFactory.getLogger(MetaRegionLocationCache.class);

  /**
   * Maximum number of times we retry when ZK operation times out.
   */
  private static final int MAX_ZK_META_FETCH_RETRIES = 10;
  /**
   * Sleep interval ms between ZK operation retries.
   */
  private static final int SLEEP_INTERVAL_MS_BETWEEN_RETRIES = 1000;
  private static final int SLEEP_INTERVAL_MS_MAX = 10000;
  private final RetryCounterFactory retryCounterFactory =
      new RetryCounterFactory(MAX_ZK_META_FETCH_RETRIES, SLEEP_INTERVAL_MS_BETWEEN_RETRIES);

  /**
   * Cached meta region locations indexed by replica ID.
   * CopyOnWriteArrayMap ensures synchronization during updates and a consistent snapshot during
   * client requests. Even though CopyOnWriteArrayMap copies the data structure for every write,
   * that should be OK since the size of the list is often small and mutations are not too often
   * and we do not need to block client requests while mutations are in progress.
   */
  private final CopyOnWriteArrayMap<Integer, HRegionLocation> cachedMetaLocations;

  private enum ZNodeOpType {
    INIT,
    CREATED,
    CHANGED,
    DELETED
  }

  public MetaRegionLocationCache(ZKWatcher zkWatcher) {
    super(zkWatcher);
    cachedMetaLocations = new CopyOnWriteArrayMap<>();
    watcher.registerListener(this);
    // Populate the initial snapshot of data from meta znodes.
    // This is needed because stand-by masters can potentially start after the initial znode
    // creation. It blocks forever until the initial meta locations are loaded from ZK and watchers
    // are established. Subsequent updates are handled by the registered listener. Also, this runs
    // in a separate thread in the background to not block master init.
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).build();
    RetryCounterFactory retryFactory = new RetryCounterFactory(
        Integer.MAX_VALUE, SLEEP_INTERVAL_MS_BETWEEN_RETRIES, SLEEP_INTERVAL_MS_MAX);
    threadFactory.newThread(
      ()->loadMetaLocationsFromZk(retryFactory.create(), ZNodeOpType.INIT)).start();
  }

  /**
   * Populates the current snapshot of meta locations from ZK. If no meta znodes exist, it registers
   * a watcher on base znode to check for any CREATE/DELETE events on the children.
   * @param retryCounter controls the number of retries and sleep between retries.
   */
  private void loadMetaLocationsFromZk(RetryCounter retryCounter, ZNodeOpType opType) {
    List<String> znodes = null;
    while (retryCounter.shouldRetry()) {
      try {
        znodes = watcher.getMetaReplicaNodesAndWatchChildren();
        break;
      } catch (KeeperException ke) {
        LOG.debug("Error populating initial meta locations", ke);
        if (!retryCounter.shouldRetry()) {
          // Retries exhausted and watchers not set. This is not a desirable state since the cache
          // could remain stale forever. Propagate the exception.
          watcher.abort("Error populating meta locations", ke);
          return;
        }
        try {
          retryCounter.sleepUntilNextRetry();
        } catch (InterruptedException ie) {
          LOG.error("Interrupted while loading meta locations from ZK", ie);
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    if (znodes == null || znodes.isEmpty()) {
      // No meta znodes exist at this point but we registered a watcher on the base znode to listen
      // for updates. They will be handled via nodeChildrenChanged().
      return;
    }
    if (znodes.size() == cachedMetaLocations.size()) {
      // No new meta znodes got added.
      return;
    }
    for (String znode: znodes) {
      String path = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, znode);
      updateMetaLocation(path, opType);
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
    RegionState metaRegionState;
    try {
      byte[] data = ZKUtil.getDataAndWatch(watcher,
          watcher.getZNodePaths().getZNodeForReplica(replicaId));
      metaRegionState = ProtobufUtil.parseMetaRegionStateFrom(data, replicaId);
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
    return new HRegionLocation(metaRegionState.getRegion(), metaRegionState.getServerName());
  }

  private void updateMetaLocation(String path, ZNodeOpType opType) {
    if (!isValidMetaPath(path)) {
      return;
    }
    LOG.debug("Updating meta znode for path {}: {}", path, opType.name());
    int replicaId = watcher.getZNodePaths().getMetaReplicaIdFromPath(path);
    RetryCounter retryCounter = retryCounterFactory.create();
    HRegionLocation location = null;
    while (retryCounter.shouldRetry()) {
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
        if (!retryCounter.shouldRetry()) {
          LOG.warn("Error getting meta location for path {}. Retries exhausted.", path, e);
          break;
        }
        try {
          retryCounter.sleepUntilNextRetry();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    if (location == null) {
      cachedMetaLocations.remove(replicaId);
      return;
    }
    cachedMetaLocations.put(replicaId, location);
  }

  /**
   * @return Optional list of HRegionLocations for meta replica(s), null if the cache is empty.
   *
   */
  public Optional<List<HRegionLocation>> getMetaRegionLocations() {
    ConcurrentNavigableMap<Integer, HRegionLocation> snapshot =
        cachedMetaLocations.tailMap(cachedMetaLocations.firstKey());
    if (snapshot.isEmpty()) {
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
  private boolean isValidMetaPath(String path) {
    return watcher.getZNodePaths().isMetaZNodePath(path);
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

  @Override
  public void nodeChildrenChanged(String path) {
    if (!path.equals(watcher.getZNodePaths().baseZNode)) {
      return;
    }
    loadMetaLocationsFromZk(retryCounterFactory.create(), ZNodeOpType.CHANGED);
  }
}
