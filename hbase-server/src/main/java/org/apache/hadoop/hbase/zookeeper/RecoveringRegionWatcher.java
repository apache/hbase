/**
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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.handler.FinishRegionRecoveringHandler;
import org.apache.zookeeper.KeeperException;

/**
 * Watcher used to be notified of the recovering region coming out of recovering state
 */
@InterfaceAudience.Private
public class RecoveringRegionWatcher extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(RecoveringRegionWatcher.class);

  private HRegionServer server;

  /**
   * Construct a ZooKeeper event listener.
   */
  public RecoveringRegionWatcher(ZooKeeperWatcher watcher, HRegionServer server) {
    super(watcher);
    watcher.registerListener(this);
    this.server = server;
  }

  /**
   * Called when a node has been deleted
   * @param path full path of the deleted node
   */
  @Override
  public void nodeDeleted(String path) {
    if (this.server.isStopped() || this.server.isStopping()) {
      return;
    }

    String parentPath = path.substring(0, path.lastIndexOf('/'));
    if (!this.watcher.recoveringRegionsZNode.equalsIgnoreCase(parentPath)) {
      return;
    }

    String regionName = path.substring(parentPath.length() + 1);

    server.getExecutorService().submit(new FinishRegionRecoveringHandler(server, regionName, path));
  }

  @Override
  public void nodeDataChanged(String path) {
    registerWatcher(path);
  }

  @Override
  public void nodeChildrenChanged(String path) {
    registerWatcher(path);
  }

  /**
   * Reinstall watcher because watcher only fire once though we're only interested in nodeDeleted
   * event we need to register the watcher in case other event happens
   */
  private void registerWatcher(String path) {
    String parentPath = path.substring(0, path.lastIndexOf('/'));
    if (!this.watcher.recoveringRegionsZNode.equalsIgnoreCase(parentPath)) {
      return;
    }

    try {
      ZKUtil.getDataAndWatch(watcher, path);
    } catch (KeeperException e) {
      LOG.warn("Can't register watcher on znode " + path, e);
    }
  }
}
