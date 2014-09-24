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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * A zk watcher that watches the labels table znode. This would create a znode
 * /hbase/visibility_labels and will have a serialized form of a set of labels in the system.
 */
@InterfaceAudience.Private
public class ZKVisibilityLabelWatcher extends ZooKeeperListener {

  private static final Log LOG = LogFactory.getLog(ZKVisibilityLabelWatcher.class);
  private static final String VISIBILITY_LABEL_ZK_PATH = "zookeeper.znode.visibility.label.parent";
  private static final String DEFAULT_VISIBILITY_LABEL_NODE = "visibility/labels";
  private static final String VISIBILITY_USER_AUTHS_ZK_PATH = 
      "zookeeper.znode.visibility.user.auths.parent";
  private static final String DEFAULT_VISIBILITY_USER_AUTHS_NODE = "visibility/user_auths";

  private VisibilityLabelsCache labelsCache;
  private String labelZnode;
  private String userAuthsZnode;

  public ZKVisibilityLabelWatcher(ZooKeeperWatcher watcher, VisibilityLabelsCache labelsCache,
      Configuration conf) {
    super(watcher);
    this.labelsCache = labelsCache;
    String labelZnodeParent = conf.get(VISIBILITY_LABEL_ZK_PATH, DEFAULT_VISIBILITY_LABEL_NODE);
    String userAuthsZnodeParent = conf.get(VISIBILITY_USER_AUTHS_ZK_PATH,
        DEFAULT_VISIBILITY_USER_AUTHS_NODE);
    this.labelZnode = ZKUtil.joinZNode(watcher.baseZNode, labelZnodeParent);
    this.userAuthsZnode = ZKUtil.joinZNode(watcher.baseZNode, userAuthsZnodeParent);
  }

  public void start() throws KeeperException {
    watcher.registerListener(this);
    if (ZKUtil.watchAndCheckExists(watcher, labelZnode)) {
      byte[] data = ZKUtil.getDataAndWatch(watcher, labelZnode);
      if (data != null) {
        refreshVisibilityLabelsCache(data);
      }
    }
    if (ZKUtil.watchAndCheckExists(watcher, userAuthsZnode)) {
      byte[] data = ZKUtil.getDataAndWatch(watcher, userAuthsZnode);
      if (data != null) {
        refreshUserAuthsCache(data);
      }
    }
  }

  private void refreshVisibilityLabelsCache(byte[] data) {
    try {
      this.labelsCache.refreshLabelsCache(data);
    } catch (IOException ioe) {
      LOG.error("Failed parsing data from labels table " + " from zk", ioe);
    }
  }

  private void refreshUserAuthsCache(byte[] data) {
    try {
      this.labelsCache.refreshUserAuthsCache(data);
    } catch (IOException ioe) {
      LOG.error("Failed parsing data from labels table " + " from zk", ioe);
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (path.equals(labelZnode) || path.equals(userAuthsZnode)) {
      try {
        ZKUtil.watchAndCheckExists(watcher, path);
      } catch (KeeperException ke) {
        LOG.error("Error setting watcher on node " + path, ke);
        // only option is to abort
        watcher.abort("Zookeeper error obtaining label node children", ke);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    // There is no case of visibility labels path to get deleted.
  }

  @Override
  public void nodeDataChanged(String path) {
    if (path.equals(labelZnode) || path.equals(userAuthsZnode)) {
      try {
        watcher.sync(path);
        byte[] data = ZKUtil.getDataAndWatch(watcher, path);
        if (path.equals(labelZnode)) {
          refreshVisibilityLabelsCache(data);
        } else {
          refreshUserAuthsCache(data);
        }
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper for node " + path, ke);
        // only option is to abort
        watcher.abort("Zookeeper error getting data for node " + path, ke);
      }
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    // We are not dealing with child nodes under the label znode or userauths znode.
  }

  /**
   * Write a labels mirror or user auths mirror into zookeeper
   * 
   * @param data
   * @param labelsOrUserAuths true for writing labels and false for user auths.
   */
  public void writeToZookeeper(byte[] data, boolean labelsOrUserAuths) {
    String znode = this.labelZnode;
    if (!labelsOrUserAuths) {
      znode = this.userAuthsZnode;
    }
    try {
      ZKUtil.createWithParents(watcher, znode);
      ZKUtil.updateExistingNodeData(watcher, znode, data, -1);
    } catch (KeeperException e) {
      LOG.error("Failed writing to " + znode, e);
      watcher.abort("Failed writing node " + znode + " to zookeeper", e);
    }
  }
}
