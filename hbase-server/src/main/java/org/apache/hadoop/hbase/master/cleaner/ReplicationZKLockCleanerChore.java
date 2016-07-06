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
package org.apache.hadoop.hbase.master.cleaner;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueuesZKImpl;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;


/**
 * A cleaner that cleans replication locks on zk which is locked by dead region servers
 */
@InterfaceAudience.Private
public class ReplicationZKLockCleanerChore extends Chore {
  private static final Log LOG = LogFactory.getLog(ReplicationZKLockCleanerChore.class);
  private ZooKeeperWatcher zk;
  private ReplicationTracker tracker;
  private long ttl;
  private ReplicationQueuesZKImpl queues;

  // Wait some times before delete lock to prevent a session expired RS not dead fully.
  private static final long DEFAULT_TTL = 60 * 10 * 1000;//10 min

  @VisibleForTesting
  public static final String TTL_CONFIG_KEY = "hbase.replication.zk.deadrs.lock.ttl";

  public ReplicationZKLockCleanerChore(Stoppable stopper, Abortable abortable, int period,
      ZooKeeperWatcher zk, Configuration conf) throws Exception {
    super("ReplicationZKLockCleanerChore", period, stopper);

    this.zk = zk;
    this.ttl = conf.getLong(TTL_CONFIG_KEY, DEFAULT_TTL);
    tracker = ReplicationFactory.getReplicationTracker(zk,
        ReplicationFactory.getReplicationPeers(zk, conf, abortable), conf, abortable, stopper);
    queues = new ReplicationQueuesZKImpl(zk, conf, abortable);
  }

  @Override protected void chore() {
    try {
      List<String> regionServers = tracker.getListOfRegionServers();
      if (regionServers == null) {
        return;
      }
      Set<String> rsSet = new HashSet<String>(regionServers);
      List<String> replicators = queues.getListOfReplicators();

      for (String replicator: replicators) {
        try {
          String lockNode = queues.getLockZNode(replicator);
          byte[] data = ZKUtil.getData(zk, lockNode);
          if (data == null) {
            continue;
          }
          String rsServerNameZnode = Bytes.toString(data);
          String[] array = rsServerNameZnode.split("/");
          String znode = array[array.length - 1];
          if (!rsSet.contains(znode)) {
            Stat s = zk.getRecoverableZooKeeper().exists(lockNode, false);
            if (s != null && EnvironmentEdgeManager.currentTimeMillis() - s.getMtime() > this.ttl) {
              // server is dead, but lock is still there, we have to delete the lock.
              ZKUtil.deleteNode(zk, lockNode);
              LOG.info("Remove lock acquired by dead RS: " + lockNode + " by " + znode);
            }
            continue;
          }
          LOG.info("Skip lock acquired by live RS: " + lockNode + " by " + znode);

        } catch (KeeperException.NoNodeException ignore) {
        } catch (InterruptedException e) {
          LOG.warn("zk operation interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    } catch (KeeperException e) {
      LOG.warn("zk operation interrupted", e);
    }

  }
}