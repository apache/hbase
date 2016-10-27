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
package org.apache.hadoop.hbase.replication;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

@InterfaceAudience.Private
public class ReplicationQueuesClientZKImpl extends ReplicationStateZKBase implements
    ReplicationQueuesClient {

  Log LOG = LogFactory.getLog(ReplicationQueuesClientZKImpl.class);

  public ReplicationQueuesClientZKImpl(ReplicationQueuesClientArguments args) {
    this(args.getZk(), args.getConf(), args.getAbortable());
  }

  public ReplicationQueuesClientZKImpl(final ZooKeeperWatcher zk, Configuration conf,
      Abortable abortable) {
    super(zk, conf, abortable);
  }

  @Override
  public void init() throws ReplicationException {
    try {
      if (ZKUtil.checkExists(this.zookeeper, this.queuesZNode) < 0) {
        ZKUtil.createWithParents(this.zookeeper, this.queuesZNode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Internal error while initializing a queues client", e);
    }
  }

  @Override
  public List<String> getLogsInQueue(String serverName, String queueId) throws KeeperException {
    String znode = ZKUtil.joinZNode(this.queuesZNode, serverName);
    znode = ZKUtil.joinZNode(znode, queueId);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get list of wals for queueId=" + queueId
          + " and serverName=" + serverName, e);
      throw e;
    }
    return result;
  }

  @Override
  public List<String> getAllQueues(String serverName) throws KeeperException {
    String znode = ZKUtil.joinZNode(this.queuesZNode, serverName);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get list of queues for serverName=" + serverName, e);
      throw e;
    }
    return result;
  }

  @Override
  public Set<String> getAllWALs() throws KeeperException {
    /**
     * Load all wals in all replication queues from ZK. This method guarantees to return a
     * snapshot which contains all WALs in the zookeeper at the start of this call even there
     * is concurrent queue failover. However, some newly created WALs during the call may
     * not be included.
     */
    for (int retry = 0; ; retry++) {
      int v0 = getQueuesZNodeCversion();
      List<String> rss = getListOfReplicators();
      if (rss == null) {
        LOG.debug("Didn't find any region server that replicates, won't prevent any deletions.");
        return ImmutableSet.of();
      }
      Set<String> wals = Sets.newHashSet();
      for (String rs : rss) {
        List<String> listOfPeers = getAllQueues(rs);
        // if rs just died, this will be null
        if (listOfPeers == null) {
          continue;
        }
        for (String id : listOfPeers) {
          List<String> peersWals = getLogsInQueue(rs, id);
          if (peersWals != null) {
            wals.addAll(peersWals);
          }
        }
      }
      int v1 = getQueuesZNodeCversion();
      if (v0 == v1) {
        return wals;
      }
      LOG.info(String.format("Replication queue node cversion changed from %d to %d, retry = %d",
        v0, v1, retry));
    }
  }

  public int getQueuesZNodeCversion() throws KeeperException {
    try {
      Stat stat = new Stat();
      ZKUtil.getDataNoWatch(this.zookeeper, this.queuesZNode, stat);
      return stat.getCversion();
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get stat of replication rs node", e);
      throw e;
    }
  }

  @Override
  public int getHFileRefsNodeChangeVersion() throws KeeperException {
    Stat stat = new Stat();
    try {
      ZKUtil.getDataNoWatch(this.zookeeper, this.hfileRefsZNode, stat);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get stat of replication hfile references node.", e);
      throw e;
    }
    return stat.getCversion();
  }

  @Override
  public List<String> getAllPeersFromHFileRefsQueue() throws KeeperException {
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, this.hfileRefsZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get list of all peers in hfile references node.", e);
      throw e;
    }
    return result;
  }

  @Override
  public List<String> getReplicableHFiles(String peerId) throws KeeperException {
    String znode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get list of hfile references for peerId=" + peerId, e);
      throw e;
    }
    return result;
  }
}
