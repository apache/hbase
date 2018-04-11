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

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the list of draining region servers via ZK.
 *
 * <p>This class is responsible for watching for changes to the draining
 * servers list.  It handles adds/deletes in the draining RS list and
 * watches each node.
 *
 * <p>If an RS gets deleted from draining list, we call
 * {@link ServerManager#removeServerFromDrainList(ServerName)}
 *
 * <p>If an RS gets added to the draining list, we add a watcher to it and call
 * {@link ServerManager#addServerToDrainList(ServerName)}
 *
 * <p>This class is deprecated in 2.0 because decommission/draining API goes through
 * master in 2.0. Can remove this class in 3.0.
 *
 */
@InterfaceAudience.Private
public class DrainingServerTracker extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(DrainingServerTracker.class);

  private ServerManager serverManager;
  private final NavigableSet<ServerName> drainingServers = new TreeSet<>();
  private Abortable abortable;

  public DrainingServerTracker(ZKWatcher watcher,
      Abortable abortable, ServerManager serverManager) {
    super(watcher);
    this.abortable = abortable;
    this.serverManager = serverManager;
  }

  /**
   * Starts the tracking of draining RegionServers.
   *
   * <p>All Draining RSs will be tracked after this method is called.
   *
   * @throws KeeperException
   */
  public void start() throws KeeperException, IOException {
    watcher.registerListener(this);
    // Add a ServerListener to check if a server is draining when it's added.
    serverManager.registerListener(new ServerListener() {
      @Override
      public void serverAdded(ServerName sn) {
        if (drainingServers.contains(sn)){
          serverManager.addServerToDrainList(sn);
        }
      }
    });
    List<String> servers =
      ZKUtil.listChildrenAndWatchThem(watcher, watcher.getZNodePaths().drainingZNode);
    add(servers);
  }

  private void add(final List<String> servers) throws IOException {
    synchronized(this.drainingServers) {
      this.drainingServers.clear();
      for (String n: servers) {
        final ServerName sn = ServerName.valueOf(ZKUtil.getNodeName(n));
        this.drainingServers.add(sn);
        this.serverManager.addServerToDrainList(sn);
        LOG.info("Draining RS node created, adding to list [" +
            sn + "]");

      }
    }
  }

  private void remove(final ServerName sn) {
    synchronized(this.drainingServers) {
      this.drainingServers.remove(sn);
      this.serverManager.removeServerFromDrainList(sn);
    }
  }

  @Override
  public void nodeDeleted(final String path) {
    if(path.startsWith(watcher.getZNodePaths().drainingZNode)) {
      final ServerName sn = ServerName.valueOf(ZKUtil.getNodeName(path));
      LOG.info("Draining RS node deleted, removing from list [" +
          sn + "]");
      remove(sn);
    }
  }

  @Override
  public void nodeChildrenChanged(final String path) {
    if(path.equals(watcher.getZNodePaths().drainingZNode)) {
      try {
        final List<String> newNodes =
          ZKUtil.listChildrenAndWatchThem(watcher, watcher.getZNodePaths().drainingZNode);
        add(newNodes);
      } catch (KeeperException e) {
        abortable.abort("Unexpected zk exception getting RS nodes", e);
      } catch (IOException e) {
        abortable.abort("Unexpected zk exception getting RS nodes", e);
      }
    }
  }
}
