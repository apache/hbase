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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This class provides an implementation of the
 * interface using ZooKeeper. The
 * base znode that this class works at is the myQueuesZnode. The myQueuesZnode contains a list of
 * all outstanding WAL files on this region server that need to be replicated. The myQueuesZnode is
 * the regionserver name (a concatenation of the region server’s hostname, client port and start
 * code). For example:
 *
 * /hbase/replication/rs/hostname.example.org,6020,1234
 *
 * Within this znode, the region server maintains a set of WAL replication queues. These queues are
 * represented by child znodes named using there give queue id. For example:
 *
 * /hbase/replication/rs/hostname.example.org,6020,1234/1
 * /hbase/replication/rs/hostname.example.org,6020,1234/2
 *
 * Each queue has one child znode for every WAL that still needs to be replicated. The value of
 * these WAL child znodes is the latest position that has been replicated. This position is updated
 * every time a WAL entry is replicated. For example:
 *
 * /hbase/replication/rs/hostname.example.org,6020,1234/1/23522342.23422 [VALUE: 254]
 */
@InterfaceAudience.Private
public class ReplicationQueuesZKImpl extends ReplicationStateZKBase implements ReplicationQueues {

  /** Znode containing all replication queues for this region server. */
  private String myQueuesZnode;

  private static final Log LOG = LogFactory.getLog(ReplicationQueuesZKImpl.class);

  public ReplicationQueuesZKImpl(ReplicationQueuesArguments args) {
    this(args.getZk(), args.getConf(), args.getAbortable());
  }

  public ReplicationQueuesZKImpl(final ZooKeeperWatcher zk, Configuration conf,
      Abortable abortable) {
    super(zk, conf, abortable);
  }

  @Override
  public void init(String serverName) throws ReplicationException {
    this.myQueuesZnode = ZKUtil.joinZNode(this.queuesZNode, serverName);
    try {
      if (ZKUtil.checkExists(this.zookeeper, this.myQueuesZnode) < 0) {
        ZKUtil.createWithParents(this.zookeeper, this.myQueuesZnode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Could not initialize replication queues.", e);
    }
    if (conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
      HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT)) {
      try {
        if (ZKUtil.checkExists(this.zookeeper, this.hfileRefsZNode) < 0) {
          ZKUtil.createWithParents(this.zookeeper, this.hfileRefsZNode);
        }
      } catch (KeeperException e) {
        throw new ReplicationException("Could not initialize hfile references replication queue.",
            e);
      }
    }
  }

  @Override
  public void removeQueue(String queueId) {
    try {
      ZKUtil.deleteNodeRecursively(this.zookeeper, ZKUtil.joinZNode(this.myQueuesZnode, queueId));
    } catch (KeeperException e) {
      this.abortable.abort("Failed to delete queue (queueId=" + queueId + ")", e);
    }
  }

  @Override
  public void addLog(String queueId, String filename) throws ReplicationException {
    String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
    znode = ZKUtil.joinZNode(znode, filename);
    try {
      ZKUtil.createWithParents(this.zookeeper, znode);
    } catch (KeeperException e) {
      throw new ReplicationException(
          "Could not add log because znode could not be created. queueId=" + queueId
              + ", filename=" + filename);
    }
  }

  @Override
  public void removeLog(String queueId, String filename) {
    try {
      String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
      znode = ZKUtil.joinZNode(znode, filename);
      ZKUtil.deleteNode(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to remove wal from queue (queueId=" + queueId + ", filename="
          + filename + ")", e);
    }
  }

  @Override
  public void setLogPosition(String queueId, String filename, long position) {
    try {
      String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
      znode = ZKUtil.joinZNode(znode, filename);
      // Why serialize String of Long and not Long as bytes?
      ZKUtil.setData(this.zookeeper, znode, ZKUtil.positionToByteArray(position));
    } catch (KeeperException e) {
      this.abortable.abort("Failed to write replication wal position (filename=" + filename
          + ", position=" + position + ")", e);
    }
  }

  @Override
  public long getLogPosition(String queueId, String filename) throws ReplicationException {
    String clusterZnode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
    String znode = ZKUtil.joinZNode(clusterZnode, filename);
    byte[] bytes = null;
    try {
      bytes = ZKUtil.getData(this.zookeeper, znode);
    } catch (KeeperException e) {
      throw new ReplicationException("Internal Error: could not get position in log for queueId="
          + queueId + ", filename=" + filename, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return 0;
    }
    try {
      return ZKUtil.parseWALPositionFrom(bytes);
    } catch (DeserializationException de) {
      LOG.warn("Failed to parse WALPosition for queueId=" + queueId + " and wal=" + filename
          + " znode content, continuing.");
    }
    // if we can not parse the position, start at the beginning of the wal file
    // again
    return 0;
  }

  @Override
  public boolean isThisOurRegionServer(String regionserver) {
    return ZKUtil.joinZNode(this.queuesZNode, regionserver).equals(this.myQueuesZnode);
  }

  @Override
  public List<String> getUnClaimedQueueIds(String regionserver) {
    if (isThisOurRegionServer(regionserver)) {
      return null;
    }
    String rsZnodePath = ZKUtil.joinZNode(this.queuesZNode, regionserver);
    List<String> queues = null;
    try {
      queues = ZKUtil.listChildrenNoWatch(this.zookeeper, rsZnodePath);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to getUnClaimedQueueIds for RS" + regionserver, e);
    }
    return queues;
  }

  @Override
  public Pair<String, SortedSet<String>> claimQueue(String regionserver, String queueId) {
    LOG.info("Atomically moving " + regionserver + "/" + queueId + "'s WALs to my queue");
    return moveQueueUsingMulti(regionserver, queueId);
  }

  @Override
  public void removeReplicatorIfQueueIsEmpty(String regionserver) {
    String rsPath = ZKUtil.joinZNode(this.queuesZNode, regionserver);
    try {
      List<String> list = ZKUtil.listChildrenNoWatch(this.zookeeper, rsPath);
      if (list != null && list.size() == 0){
        ZKUtil.deleteNode(this.zookeeper, rsPath);
      }
    } catch (KeeperException e) {
      LOG.warn("Got error while removing replicator", e);
    }
  }

  @Override
  public void removeAllQueues() {
    try {
      ZKUtil.deleteNodeRecursively(this.zookeeper, this.myQueuesZnode);
    } catch (KeeperException e) {
      // if the znode is already expired, don't bother going further
      if (e instanceof KeeperException.SessionExpiredException) {
        return;
      }
      this.abortable.abort("Failed to delete replication queues for region server: "
          + this.myQueuesZnode, e);
    }
  }

  @Override
  public List<String> getLogsInQueue(String queueId) {
    String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get list of wals for queueId=" + queueId, e);
    }
    return result;
  }

  @Override
  public List<String> getAllQueues() {
    List<String> listOfQueues = null;
    try {
      listOfQueues = ZKUtil.listChildrenNoWatch(this.zookeeper, this.myQueuesZnode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get a list of queues for region server: "
          + this.myQueuesZnode, e);
    }
    return listOfQueues == null ? new ArrayList<String>() : listOfQueues;
  }

  /**
   * It "atomically" copies one peer's wals queue from another dead region server and returns them
   * all sorted. The new peer id is equal to the old peer id appended with the dead server's znode.
   * @param znode pertaining to the region server to copy the queues from
   * @peerId peerId pertaining to the queue need to be copied
   */
  private Pair<String, SortedSet<String>> moveQueueUsingMulti(String znode, String peerId) {
    try {
      // hbase/replication/rs/deadrs
      String deadRSZnodePath = ZKUtil.joinZNode(this.queuesZNode, znode);
      List<ZKUtilOp> listOfOps = new ArrayList<>();
      ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(peerId);

      String newPeerId = peerId + "-" + znode;
      String newPeerZnode = ZKUtil.joinZNode(this.myQueuesZnode, newPeerId);
      // check the logs queue for the old peer cluster
      String oldClusterZnode = ZKUtil.joinZNode(deadRSZnodePath, peerId);
      List<String> wals = ZKUtil.listChildrenNoWatch(this.zookeeper, oldClusterZnode);

      if (!peerExists(replicationQueueInfo.getPeerId())) {
        LOG.warn("Peer " + replicationQueueInfo.getPeerId() +
                " didn't exist, will move its queue to avoid the failure of multi op");
        for (String wal : wals) {
          String oldWalZnode = ZKUtil.joinZNode(oldClusterZnode, wal);
          listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldWalZnode));
        }
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));
        ZKUtil.multiOrSequential(this.zookeeper, listOfOps, false);
        return null;
      }

      SortedSet<String> logQueue = new TreeSet<>();
      if (wals == null || wals.size() == 0) {
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));
      } else {
        // create the new cluster znode
        ZKUtilOp op = ZKUtilOp.createAndFailSilent(newPeerZnode, HConstants.EMPTY_BYTE_ARRAY);
        listOfOps.add(op);
        // get the offset of the logs and set it to new znodes
        for (String wal : wals) {
          String oldWalZnode = ZKUtil.joinZNode(oldClusterZnode, wal);
          byte[] logOffset = ZKUtil.getData(this.zookeeper, oldWalZnode);
          LOG.debug("Creating " + wal + " with data " + Bytes.toString(logOffset));
          String newLogZnode = ZKUtil.joinZNode(newPeerZnode, wal);
          listOfOps.add(ZKUtilOp.createAndFailSilent(newLogZnode, logOffset));
          listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldWalZnode));
          logQueue.add(wal);
        }
        // add delete op for peer
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));

        if (LOG.isTraceEnabled())
          LOG.trace(" The multi list size is: " + listOfOps.size());
      }
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, false);

      LOG.info("Atomically moved " + znode + "/" + peerId + "'s WALs to my queue");
      return new Pair<>(newPeerId, logQueue);
    } catch (KeeperException e) {
      // Multi call failed; it looks like some other regionserver took away the logs.
      LOG.warn("Got exception in copyQueuesFromRSUsingMulti: ", e);
    } catch (InterruptedException e) {
      LOG.warn("Got exception in copyQueuesFromRSUsingMulti: ", e);
      Thread.currentThread().interrupt();
    }
    return null;
  }

  @Override
  public void addHFileRefs(String peerId, List<String> files) throws ReplicationException {
    String peerZnode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    boolean debugEnabled = LOG.isDebugEnabled();
    if (debugEnabled) {
      LOG.debug("Adding hfile references " + files + " in queue " + peerZnode);
    }
    List<ZKUtilOp> listOfOps = new ArrayList<ZKUtil.ZKUtilOp>();
    int size = files.size();
    for (int i = 0; i < size; i++) {
      listOfOps.add(ZKUtilOp.createAndFailSilent(ZKUtil.joinZNode(peerZnode, files.get(i)),
        HConstants.EMPTY_BYTE_ARRAY));
    }
    if (debugEnabled) {
      LOG.debug(" The multi list size for adding hfile references in zk for node " + peerZnode
          + " is " + listOfOps.size());
    }
    try {
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, true);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to create hfile reference znode=" + e.getPath(), e);
    }
  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) {
    String peerZnode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    boolean debugEnabled = LOG.isDebugEnabled();
    if (debugEnabled) {
      LOG.debug("Removing hfile references " + files + " from queue " + peerZnode);
    }
    List<ZKUtilOp> listOfOps = new ArrayList<ZKUtil.ZKUtilOp>();
    int size = files.size();
    for (int i = 0; i < size; i++) {
      listOfOps.add(ZKUtilOp.deleteNodeFailSilent(ZKUtil.joinZNode(peerZnode, files.get(i))));
    }
    if (debugEnabled) {
      LOG.debug(" The multi list size for removing hfile references in zk for node " + peerZnode
          + " is " + listOfOps.size());
    }
    try {
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, true);
    } catch (KeeperException e) {
      LOG.error("Failed to remove hfile reference znode=" + e.getPath(), e);
    }
  }

  @Override
  public void addPeerToHFileRefs(String peerId) throws ReplicationException {
    String peerZnode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    try {
      if (ZKUtil.checkExists(this.zookeeper, peerZnode) == -1) {
        LOG.info("Adding peer " + peerId + " to hfile reference queue.");
        ZKUtil.createWithParents(this.zookeeper, peerZnode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to add peer " + peerId + " to hfile reference queue.",
          e);
    }
  }

  @Override
  public void removePeerFromHFileRefs(String peerId) {
    final String peerZnode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    try {
      if (ZKUtil.checkExists(this.zookeeper, peerZnode) == -1) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Peer " + peerZnode + " not found in hfile reference queue.");
        }
        return;
      } else {
        LOG.info("Removing peer " + peerZnode + " from hfile reference queue.");
        ZKUtil.deleteNodeRecursively(this.zookeeper, peerZnode);
      }
    } catch (KeeperException e) {
      LOG.error("Ignoring the exception to remove peer " + peerId + " from hfile reference queue.",
        e);
    }
  }
}
