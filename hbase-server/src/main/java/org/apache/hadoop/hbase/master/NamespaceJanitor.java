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
package org.apache.hadoop.hbase.master;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A janitor for the namespace artifacts.
 * Traverses hdfs and zk to remove orphaned directories/znodes
 */
@InterfaceAudience.Private
public class NamespaceJanitor extends Chore {
  private static final Log LOG = LogFactory.getLog(NamespaceJanitor.class.getName());
  private final MasterServices services;
  private AtomicBoolean enabled = new AtomicBoolean(true);

  public NamespaceJanitor(final MasterServices services) {
    super("NamespaceJanitor-" + services.getServerName().toShortString(),
      services.getConfiguration().getInt("hbase.namespacejanitor.interval", 300000),
      services);
    this.services = services;
  }

  @Override
  protected boolean initialChore() {
    try {
      if (this.enabled.get()) removeOrphans();
    } catch (IOException e) {
      LOG.warn("Failed NamespaceJanitor chore", e);
      return false;
    } catch (KeeperException e) {
      LOG.warn("Failed NamespaceJanitor chore", e);
      return false;
    }
    return true;
  }

  /**
   * @param enabled
   */
  public boolean setEnabled(final boolean enabled) {
    return this.enabled.getAndSet(enabled);
  }

  boolean getEnabled() {
    return this.enabled.get();
  }

  @Override
  protected void chore() {
    try {
      if (this.enabled.get()) {
        removeOrphans();
      } else {
        LOG.warn("NamepsaceJanitor disabled! Not running scan.");
      }
    } catch (IOException e) {
      LOG.warn("Failed NamespaceJanitor chore", e);
    } catch (KeeperException e) {
      LOG.warn("Failed NamespaceJanitor chore", e);
    }
  }

  private void removeOrphans() throws IOException, KeeperException {
    //cache the info so we don't need to keep the master nsLock for long
    //and not be wasteful with rpc calls
    FileSystem fs = services.getMasterFileSystem().getFileSystem();
    Set<String> descs = Sets.newHashSet();
    for(NamespaceDescriptor ns : services.listNamespaceDescriptors()) {
      descs.add(ns.getName());
    }

    //cleanup hdfs orphans
    for (FileStatus nsStatus : FSUtils.listStatus(fs,
        new Path(FSUtils.getRootDir(services.getConfiguration()), HConstants.BASE_NAMESPACE_DIR))) {
      if (!descs.contains(nsStatus.getPath().getName()) &&
          !NamespaceDescriptor.RESERVED_NAMESPACES.contains(nsStatus.getPath().getName())) {
        boolean isEmpty = true;
        for(FileStatus status : fs.listStatus(nsStatus.getPath())) {
          if (!HConstants.HBASE_NON_TABLE_DIRS.contains(status.getPath().getName())) {
            isEmpty = false;
            break;
          }
        }
        if(isEmpty) {
          try {
            if (!fs.delete(nsStatus.getPath(), true)) {
              LOG.error("Failed to remove namespace directory: " + nsStatus.getPath());
            }
          } catch (IOException ex) {
            LOG.error("Failed to remove namespace directory: " + nsStatus.getPath(),
                ex);
          }
          LOG.debug("Removed namespace directory: "+nsStatus.getPath());
        } else {
          LOG.debug("Skipping non-empty namespace directory: " + nsStatus.getPath());
        }
      }
    }

    String baseZnode = ZooKeeperWatcher.namespaceZNode;
    for(String child : ZKUtil.listChildrenNoWatch(services.getZooKeeper(), baseZnode)) {
      if (!descs.contains(child) &&
          !NamespaceDescriptor.RESERVED_NAMESPACES.contains(child)) {
        String znode = ZKUtil.joinZNode(baseZnode, child);
        try {
          ZKUtil.deleteNode(services.getZooKeeper(), znode);
          LOG.debug("Removed namespace znode: " + znode);
        } catch (KeeperException ex) {
          LOG.debug("Failed to remove namespace znode: " + znode, ex);
        }
      }
    }

  }
}
