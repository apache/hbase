/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.RetryForever;
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

/**
 * This is an example showing how a RegionObserver could configured via ZooKeeper in order to
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

  private NodeCache cache;

  /**
   * Internal watcher that keep "data" up to date asynchronously.
   */
  private static final class ZKDataHolder {

    private final String ensemble;

    private final int sessionTimeout;

    private CuratorFramework client;

    private NodeCache cache;

    private int ref;

    public ZKDataHolder(String ensemble, int sessionTimeout) {
      this.ensemble = ensemble;
      this.sessionTimeout = sessionTimeout;
    }

    private void create() throws Exception {
      client =
          CuratorFrameworkFactory.builder().connectString(ensemble).sessionTimeoutMs(sessionTimeout)
              .retryPolicy(new RetryForever(1000)).canBeReadOnly(true).build();
      client.start();
      cache = new NodeCache(client, NODE);
      cache.start(true);
    }

    private void close() {
      if (cache != null) {
        try {
          cache.close();
        } catch (IOException e) {
          // should not happen
          throw new AssertionError(e);
        }
        cache = null;
      }
      if (client != null) {
        client.close();
        client = null;
      }
    }

    public synchronized NodeCache acquire() throws Exception {
      if (ref == 0) {
        try {
          create();
        } catch (Exception e) {
          close();
          throw e;
        }
      }
      ref++;
      return cache;
    }

    public synchronized void release() {
      ref--;
      if (ref == 0) {
        close();
      }
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
      })).acquire();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    RegionCoprocessorEnvironment renv = (RegionCoprocessorEnvironment) env;
    this.cache = null;
    ((ZKDataHolder) renv.getSharedData().get(ZKKEY)).release();
  }

  private OptionalLong getExpireBefore() {
    ChildData data = cache.getCurrentData();
    if (data == null) {
      return OptionalLong.empty();
    }
    byte[] bytes = data.getData();
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
  public void preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      ScanOptions options, FlushLifeCycleTracker tracker) throws IOException {
    resetTTL(options);
  }

  @Override
  public void preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
      CompactionRequest request) throws IOException {
    resetTTL(options);
  }
}
