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
package org.apache.hadoop.hbase.mob.mapreduce;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.TableLock;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Tracker on the sweep tool node in zookeeper.
 * The sweep tool node is an ephemeral one, when the process dies this node is deleted,
 * at that time MR might be still running, and if another sweep job is started, two MR
 * for the same column family will run at the same time.
 * This tracker watches this ephemeral node, if it's gone or it's not created by the
 * sweep job that owns the current MR, the current process will be aborted.
 */
@InterfaceAudience.Private
public class SweepJobNodeTracker extends ZooKeeperListener {

  private String parentNode;
  private String lockNodePrefix;
  private String owner;
  private String lockNode;

  public SweepJobNodeTracker(ZooKeeperWatcher watcher, String parentNode, String owner) {
    super(watcher);
    this.parentNode = parentNode;
    this.owner = owner;
    this.lockNodePrefix = ZKUtil.joinZNode(parentNode, "write-");
  }

  /**
   * Registers the watcher on the sweep job node.
   * If there's no such a sweep job node, or it's not created by the sweep job that
   * owns the current MR, the current process will be aborted.
   * This assumes the table lock uses the Zookeeper. It's a workaround and only used
   * in the sweep tool, and the sweep tool will be removed after the mob file compaction
   * is finished.
   */
  public void start() throws KeeperException {
    watcher.registerListener(this);
    List<String> children = ZKUtil.listChildrenNoWatch(watcher, parentNode);
    if (children != null && !children.isEmpty()) {
      // there are locks
      TreeSet<String> sortedChildren = new TreeSet<String>();
      sortedChildren.addAll(children);
      // find all the write locks
      SortedSet<String> tails = sortedChildren.tailSet(lockNodePrefix);
      if (!tails.isEmpty()) {
        for (String tail : tails) {
          String path = ZKUtil.joinZNode(parentNode, tail);
          byte[] data = ZKUtil.getDataAndWatch(watcher, path);
          TableLock lock = TableLockManager.fromBytes(data);
          ServerName serverName = lock.getLockOwner();
          org.apache.hadoop.hbase.ServerName sn = org.apache.hadoop.hbase.ServerName.valueOf(
              serverName.getHostName(), serverName.getPort(), serverName.getStartCode());
          // compare the server names (host, port and start code), make sure the lock is created
          if (owner.equals(sn.toString())) {
            lockNode = path;
            return;
          }
        }
      }
    }
    System.exit(1);
  }

  @Override
  public void nodeDeleted(String path) {
    // If the lock node is deleted, abort the current process.
    if (path.equals(lockNode)) {
      System.exit(1);
    }
  }
}
