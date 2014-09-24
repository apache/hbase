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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Callback handler for creating unassigned offline znodes
 * used during bulk assign, async setting region to offline.
 */
@InterfaceAudience.Private
public class OfflineCallback implements StringCallback {
  private final Log LOG = LogFactory.getLog(OfflineCallback.class);
  private final ExistCallback callBack;
  private final ZooKeeperWatcher zkw;
  private final ServerName destination;
  private final AtomicInteger counter;

  OfflineCallback(final ZooKeeperWatcher zkw,
      final ServerName destination, final AtomicInteger counter,
      final Map<String, Integer> offlineNodesVersions) {
    this.callBack = new ExistCallback(
      destination, counter, offlineNodesVersions);
    this.destination = destination;
    this.counter = counter;
    this.zkw = zkw;
  }

  @Override
  public void processResult(int rc, String path, Object ctx, String name) {
    if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
      LOG.warn("Node for " + path + " already exists");
    } else if (rc != 0) {
      // This is result code.  If non-zero, need to resubmit.
      LOG.warn("rc != 0 for " + path + " -- retryable connectionloss -- " +
        "FIX see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A2");
      this.counter.addAndGet(1);
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("rs=" + ctx + ", server=" + destination);
    }
    // Async exists to set a watcher so we'll get triggered when
    // unassigned node changes.
    ZooKeeper zk = this.zkw.getRecoverableZooKeeper().getZooKeeper();
    zk.exists(path, this.zkw, callBack, ctx);
  }

  /**
   * Callback handler for the exists call that sets watcher on unassigned znodes.
   * Used during bulk assign on startup.
   */
  static class ExistCallback implements StatCallback {
    private final Log LOG = LogFactory.getLog(ExistCallback.class);
    private final Map<String, Integer> offlineNodesVersions;
    private final AtomicInteger counter;
    private ServerName destination;

    ExistCallback(final ServerName destination,
        final AtomicInteger counter,
        final Map<String, Integer> offlineNodesVersions) {
      this.offlineNodesVersions = offlineNodesVersions;
      this.destination = destination;
      this.counter = counter;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (rc != 0) {
        // This is result code.  If non-zero, need to resubmit.
        LOG.warn("rc != 0 for " + path + " -- retryable connectionloss -- " +
          "FIX see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A2");
        this.counter.addAndGet(1);
        return;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("rs=" + ctx + ", server=" + destination);
      }
      HRegionInfo region = ((RegionState)ctx).getRegion();
      offlineNodesVersions.put(
        region.getEncodedName(), Integer.valueOf(stat.getVersion()));
      this.counter.addAndGet(1);
    }
  }
}
