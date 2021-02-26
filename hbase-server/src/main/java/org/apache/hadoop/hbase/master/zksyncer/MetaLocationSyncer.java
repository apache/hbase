/*
 *
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
package org.apache.hadoop.hbase.master.zksyncer;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Tracks the meta region locations on server ZK cluster and synchronize them to client ZK cluster
 * if changed
 */
@InterfaceAudience.Private
public class MetaLocationSyncer extends ClientZKSyncer {

  private volatile int metaReplicaCount = 1;

  public MetaLocationSyncer(ZKWatcher watcher, ZKWatcher clientZkWatcher, Server server) {
    super(watcher, clientZkWatcher, server);
  }

  @Override
  protected boolean validate(String path) {
    return watcher.getZNodePaths().isMetaZNodePath(path);
  }

  @Override
  protected Set<String> getPathsToWatch() {
    return IntStream.range(0, metaReplicaCount)
      .mapToObj(watcher.getZNodePaths()::getZNodeForReplica).collect(Collectors.toSet());
  }

  public void setMetaReplicaCount(int replicaCount) {
    if (replicaCount != metaReplicaCount) {
      metaReplicaCount = replicaCount;
      refreshWatchingList();
    }
  }
}