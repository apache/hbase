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
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * This is an example showing how a RegionObserver could be configured via ZooKeeper in order to
 * control a Region compaction, flush, and scan policy. This also demonstrated the use of shared
 * {@link org.apache.hadoop.hbase.coprocessor.RegionObserver} state. See
 * {@link RegionCoprocessorEnvironment#getSharedData()}.
 * <p>
 * This would be useful for an incremental backup tool, which would indicate the last time of a
 * successful backup via ZK and instruct HBase that to safely delete the data which has already been
 * backup.
 */
@InterfaceAudience.Private
public class ZooKeeperScanPolicyObserver implements RegionCoprocessor, RegionObserver {

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  // The zk ensemble info is put in hbase config xml with given custom key.
  public static final String ZK_ENSEMBLE_KEY = "ZooKeeperScanPolicyObserver.zookeeper.ensemble";
  public static final String ZK_SESSION_TIMEOUT_KEY =
    "ZooKeeperScanPolicyObserver.zookeeper.session.timeout";
  public static final int ZK_SESSION_TIMEOUT_DEFAULT = 30 * 1000; // 30 secs
  public static final String NODE = "/backup/example/lastbackup";
  private static final String ZKKEY = "ZK";

  private ZKDataHolder cache;

  /**
   * Internal watcher that keep "data" up to date asynchronously.
   */
  private static final class ZKDataHolder implements Watcher {

    private final String ensemble;

    private final int sessionTimeout;

    private ZooKeeper zk;

    private int ref;

    private byte[] data;

    public ZKDataHolder(String ensemble, int sessionTimeout) {
      this.ensemble = ensemble;
      this.sessionTimeout = sessionTimeout;
    }

    private void open() throws IOException {
      if (zk == null) {
        zk = new ZooKeeper(ensemble, sessionTimeout, this);
        // In a real application, you'd probably want to create these Znodes externally,
        // and not from the coprocessor
        StringBuffer createdPath = new StringBuffer();
        byte[] empty = new byte[0];
        for (String element : NODE.split("/")) {
          if (element.isEmpty()) {
            continue;
          }
          try {
            createdPath = createdPath.append("/").append(element);
            zk.create(createdPath.toString(), empty, ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
          } catch (NodeExistsException e) {
            // That's OK
          } catch (KeeperException e) {
            throw new IOException(e);
          } catch (InterruptedException e) {
            // Restore interrupt status
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    private void close() {
      if (zk != null) {
        try {
          zk.close();
          zk = null;
        } catch (InterruptedException e) {
          // Restore interrupt status
          Thread.currentThread().interrupt();
        }
      }
    }

    public synchronized byte[] getData() {
      if (ref == 0) {
        Stat stat = null;
        try {
          stat = zk.exists(NODE, this);
        } catch (KeeperException e) {
          // Value will always be null if the initial connection fails.
          // In a real application you probably want to try to
          // periodically re-connect in this case.
        } catch (InterruptedException e) {
          // Restore interrupt status
          Thread.currentThread().interrupt();
        }
        if (stat != null) {
          refresh();
        }
      }
      ref++;
      return data;
    }

    private synchronized void refresh() {
      try {
        data = zk.getData(NODE, this, null);
      } catch (KeeperException e) {
        // Value will always be null if this fails (as we cannot set the new watcher)
        // In a real application you probably want to try to
        // periodically re-connect in this case.
      } catch (InterruptedException e) {
        // Restore interrupt status
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void process(WatchedEvent event) {
      refresh();
    }
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    RegionCoprocessorEnvironment renv = (RegionCoprocessorEnvironment) env;
    try {
      this.cache = ((ZKDataHolder) renv.getSharedData().computeIfAbsent(ZKKEY, k -> {
        String ensemble = renv.getConfiguration().get(ZK_ENSEMBLE_KEY);
        int sessionTimeout =
          renv.getConfiguration().getInt(ZK_SESSION_TIMEOUT_KEY, ZK_SESSION_TIMEOUT_DEFAULT);
        return new ZKDataHolder(ensemble, sessionTimeout);
      }));
      cache.open();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    RegionCoprocessorEnvironment renv = (RegionCoprocessorEnvironment) env;
    this.cache = null;
    ((ZKDataHolder) renv.getSharedData().get(ZKKEY)).close();
  }

  private OptionalLong getExpireBefore() {
    byte[] bytes = cache.getData();
    if (bytes == null || bytes.length != Long.BYTES) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(Bytes.toLong(bytes));
  }

  private void resetTTL(ScanOptions options) {
    OptionalLong expireBefore = getExpireBefore();
    if (!expireBefore.isPresent()) {
      return;
    }
    options.setTTL(EnvironmentEdgeManager.currentTime() - expireBefore.getAsLong());
  }

  @Override
  public void preFlushScannerOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, ScanOptions options, FlushLifeCycleTracker tracker) throws IOException {
    resetTTL(options);
  }

  @Override
  public void preCompactScannerOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c,
    Store store, ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
    CompactionRequest request) throws IOException {
    resetTTL(options);
  }
}
