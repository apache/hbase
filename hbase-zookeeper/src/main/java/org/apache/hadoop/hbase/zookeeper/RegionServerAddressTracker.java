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
package org.apache.hadoop.hbase.zookeeper;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Class for tracking the region servers for a cluster.
 */
@InterfaceAudience.Private
public class RegionServerAddressTracker extends ZKListener {

  private static final Logger LOG = LoggerFactory.getLogger(RegionServerAddressTracker.class);

  private volatile List<ServerName> regionServers = Collections.emptyList();

  private final Abortable abortable;

  public RegionServerAddressTracker(ZKWatcher watcher, Abortable abortable) {
    super(watcher);
    this.abortable = abortable;
    watcher.registerListener(this);
    loadRegionServerList();
  }

  private void loadRegionServerList() {
    List<String> names;
    try {
      names = ZKUtil.listChildrenAndWatchForNewChildren(watcher, watcher.getZNodePaths().rsZNode);
    } catch (KeeperException e) {
      LOG.error("failed to list region servers", e);
      abortable.abort("failed to list region servers", e);
      return;
    }
    if (CollectionUtils.isEmpty(names)) {
      regionServers = Collections.emptyList();
    } else {
      regionServers = names.stream().map(ServerName::parseServerName)
        .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.getZNodePaths().rsZNode)) {
      loadRegionServerList();
    }
  }

  public List<ServerName> getRegionServers() {
    return regionServers;
  }
}
