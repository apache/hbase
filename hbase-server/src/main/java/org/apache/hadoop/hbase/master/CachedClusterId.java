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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Caches the cluster ID of the cluster. For standby masters, this is used to serve the client
 * RPCs that fetch the cluster ID. ClusterID is only created by an active master if one does not
 * already exist. Standby masters just read the information from the file system. This class is
 * thread-safe.
 *
 * TODO: Make it a singleton without affecting concurrent junit tests.
 */
@InterfaceAudience.Private
public class CachedClusterId {

  public static final Logger LOG = LoggerFactory.getLogger(CachedClusterId.class);
  private static final int MAX_FETCH_TIMEOUT_MS = 10000;

  private final Path rootDir;
  private final FileSystem fs;

  // When true, indicates that a FileSystem fetch of ClusterID is in progress. This is used to
  // avoid multiple fetches from FS and let only one thread fetch the information.
  AtomicBoolean fetchInProgress = new AtomicBoolean(false);

  // When true, it means that the cluster ID has been fetched successfully from fs.
  private AtomicBoolean isClusterIdSet = new AtomicBoolean(false);
  // Immutable once set and read multiple times.
  private ClusterId clusterId;

  private final Server server;

  // cache stats for testing.
  private AtomicInteger cacheMisses = new AtomicInteger(0);

  public CachedClusterId(Server server, Configuration conf) throws IOException {
    this.rootDir = CommonFSUtils.getRootDir(conf);
    this.fs = rootDir.getFileSystem(conf);
    this.server = server;
  }

  /**
   * Succeeds only once, when setting to a non-null value. Overwrites are not allowed.
   */
  private void setClusterId(ClusterId id) {
    if (id == null || isClusterIdSet.get()) {
      return;
    }
    clusterId = id;
    isClusterIdSet.set(true);
  }

  /**
   * Returns a cached copy of the cluster ID. null if the cache is not populated.
   */
  private String getClusterId() {
    if (!isClusterIdSet.get()) {
      return null;
    }
    // It is ok to read without a lock since clusterId is immutable once set.
    return clusterId.toString();
  }

  /**
   * Attempts to fetch the cluster ID from the file system. If no attempt is already in progress,
   * synchronously fetches the cluster ID and sets it. If an attempt is already in progress,
   * returns right away and the caller is expected to wait for the fetch to finish.
   * @return true if the attempt is done, false if another thread is already fetching it.
   */
  private boolean attemptFetch() {
    if (fetchInProgress.compareAndSet(false, true)) {
      // A fetch is not in progress, so try fetching the cluster ID synchronously and then notify
      // the waiting threads.
      try {
        cacheMisses.incrementAndGet();
        setClusterId(FSUtils.getClusterId(fs, rootDir));
      } catch (IOException e) {
        LOG.warn("Error fetching cluster ID", e);
      } finally {
        Preconditions.checkState(fetchInProgress.compareAndSet(true, false));
        synchronized (fetchInProgress) {
          fetchInProgress.notifyAll();
        }
      }
      return true;
    }
    return false;
  }

  private void waitForFetchToFinish() throws InterruptedException {
    synchronized (fetchInProgress) {
      while (fetchInProgress.get()) {
        // We don't want the fetches to block forever, for example if there are bugs
        // of missing notifications.
        fetchInProgress.wait(MAX_FETCH_TIMEOUT_MS);
      }
    }
  }

  /**
   * Fetches the ClusterId from FS if it is not cached locally. Atomically updates the cached
   * copy and is thread-safe. Optimized to do a single fetch when there are multiple threads are
   * trying get from a clean cache.
   *
   * @return ClusterId by reading from FileSystem or null in any error case or cluster ID does
   *     not exist on the file system or if the server initiated a tear down.
   */
  public String getFromCacheOrFetch() {
    if (server.isStopping() || server.isStopped()) {
      return null;
    }
    String id = getClusterId();
    if (id != null) {
      return id;
    }
    if (!attemptFetch()) {
      // A fetch is in progress.
      try {
        waitForFetchToFinish();
      } catch (InterruptedException e) {
        // pass and return whatever is in the cache.
      }
    }
    return getClusterId();
  }

  public int getCacheStats() {
    return cacheMisses.get();
  }
}
