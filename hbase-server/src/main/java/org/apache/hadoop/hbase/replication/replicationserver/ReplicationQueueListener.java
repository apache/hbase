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
package org.apache.hadoop.hbase.replication.replicationserver;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Tracks changes to the WALs in the replication queue.
 */
@InterfaceAudience.Private
public class ReplicationQueueListener extends ZKListener {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationQueueListener.class);

  private final ReplicationSourceInterface source;
  private final String queueNode;
  private final Path walRootDir;
  private final Abortable abortable;
  private final ZKWatcher watcher;

  private Set<String> wals = Sets.newHashSet();

  private static final ExecutorService executor = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ReplicationQueueListener-%d")
      .build());

  public ReplicationQueueListener(ReplicationSourceInterface source,
    ZKReplicationQueueStorage zkQueueStorage, Path walRootDir, Abortable abortable) {
    super(zkQueueStorage.getZookeeper());
    this.source = source;
    this.watcher = zkQueueStorage.getZookeeper();
    this.walRootDir = walRootDir;
    this.queueNode = zkQueueStorage.getQueueNode(source.getQueueOwner(), source.getQueueId());
    this.abortable = abortable;
  }

  public synchronized void initialize(Collection<String> initialWals) {
    wals = Sets.newHashSet(initialWals);
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(queueNode)) {
      executor.submit(this::refresh);
    }
  }

  public void start(Collection<String> initialWals) {
    initialize(initialWals);
    this.watcher.registerListener(this);
    this.refresh();
    LOG.info("Register a ZKListener to track the WALs from {}'s replication queue, queueId={}",
      source.getQueueOwner(), source.getQueueId());
  }

  public void stop() {
    this.watcher.unregisterListener(this);
  }

  private synchronized void refresh() {
    List<String> names;
    try {
      names = ZKUtil.listChildrenAndWatchForNewChildren(watcher, queueNode);
    } catch (KeeperException e) {
      // here we need to abort as we failed to set watcher on the replication queue node which means
      // that we can not track the wal creation event any more.
      String msg = "Unexpected zk exception getting WAL nodes, queueNode={}" + queueNode;
      LOG.error(msg, e);
      abortable.abort(msg, e);
      return;
    }
    Set<String> currentWals = Sets.newHashSet(names);
    wals.removeIf(wal -> !currentWals.contains(wal));

    for (String wal : currentWals) {
      if (!wals.contains(wal)) {
        enqueueLog(wal);
        wals.add(wal);
      }
    }
  }

  private void enqueueLog(String wal) {
    Path walDir = new Path(walRootDir,
      AbstractFSWALProvider.getWALDirectoryName(source.getQueueOwner().toString()));
    Path walPath = new Path(walDir, wal);
    LOG.info("Detected new WAL in the replication queue {} from region server {},"
      + " wal={}", queueNode, source.getQueueOwner(), walPath);
    source.enqueueLog(walPath);
  }
}

