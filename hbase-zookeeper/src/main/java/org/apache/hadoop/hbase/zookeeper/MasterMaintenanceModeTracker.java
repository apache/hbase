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
package org.apache.hadoop.hbase.zookeeper;

import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Tracks the master Maintenance Mode via ZK.
 *
 * Unused. Used to be set by hbck to prevent concurrent splits/merges, but those use PV2 now and
 * HBCK2 uses it's own service, so no longer an issue. Left in, in case we need to use this for
 * the incomplete parts of HBCK2...
 */
@InterfaceAudience.Private
public class MasterMaintenanceModeTracker extends ZKListener {
  private boolean hasChildren;

  public MasterMaintenanceModeTracker(ZKWatcher watcher) {
    super(watcher);
    hasChildren = false;
  }

  public boolean isInMaintenanceMode() {
    return hasChildren;
  }

  private void update(String path) {
    if (path.startsWith(watcher.getZNodePaths().masterMaintZNode)) {
      update();
    }
  }

  private void update() {
    try {
      List<String> children =
          ZKUtil.listChildrenAndWatchForNewChildren(watcher,
                  watcher.getZNodePaths().masterMaintZNode);
      hasChildren = (children != null && children.size() > 0);
    } catch (KeeperException e) {
      // Ignore the ZK keeper exception
      hasChildren = false;
    }
  }

  /**
   * Starts the tracking of whether master is in Maintenance Mode.
   */
  public void start() {
    watcher.registerListener(this);
    update();
  }

  @Override
  public void nodeCreated(String path) {
    update(path);
  }

  @Override
  public void nodeDeleted(String path) {
    update(path);
  }

  @Override
  public void nodeChildrenChanged(String path) {
    update(path);
  }
}
