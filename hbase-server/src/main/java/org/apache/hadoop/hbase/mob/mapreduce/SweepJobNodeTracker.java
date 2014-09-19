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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
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

  private String node;
  private String sweepJobId;

  public SweepJobNodeTracker(ZooKeeperWatcher watcher, String node, String sweepJobId) {
    super(watcher);
    this.node = node;
    this.sweepJobId = sweepJobId;
  }

  /**
   * Registers the watcher on the sweep job node.
   * If there's no such a sweep job node, or it's not created by the sweep job that
   * owns the current MR, the current process will be aborted.
   */
  public void start() throws KeeperException {
    watcher.registerListener(this);
    if (ZKUtil.watchAndCheckExists(watcher, node)) {
      byte[] data = ZKUtil.getDataAndWatch(watcher, node);
      if (data != null) {
        if (!sweepJobId.equals(Bytes.toString(data))) {
          System.exit(1);
        }
      }
    } else {
      System.exit(1);
    }
  }

  @Override
  public void nodeDeleted(String path) {
    // If the ephemeral node is deleted, abort the current process.
    if (node.equals(path)) {
      System.exit(1);
    }
  }
}
