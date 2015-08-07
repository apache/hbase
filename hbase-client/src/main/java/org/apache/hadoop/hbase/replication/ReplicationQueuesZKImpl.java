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
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.zookeeper.KeeperException;

/**
 * This class provides an implementation of the ReplicationQueues interface using Zookeeper. The
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
  /** Name of znode we use to lock during failover */
  private final static String RS_LOCK_ZNODE = "lock";

  private static final Log LOG = LogFactory.getLog(ReplicationQueuesZKImpl.class);

  public ReplicationQueuesZKImpl(final ZooKeeperWatcher zk, Configuration conf,
      Abortable abortable) {
    super(zk, conf, abortable);
  }

  @Override
  public void init(String serverName) throws ReplicationException {
    this.myQueuesZnode = ZKUtil.joinZNode(this.queuesZNode, serverName);
    try {
      ZKUtil.createWithParents(this.zookeeper, this.myQueuesZnode);
    } catch (KeeperException e) {
      throw new ReplicationException("Could not initialize replication queues.", e);
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
          + "znode content, continuing.");
    }
    // if we can not parse the position, start at the beginning of the wal file
    // again
    return 0;
  }

  @Override
  public boolean isThisOurZnode(String znode) {
    return ZKUtil.joinZNode(this.queuesZNode, znode).equals(this.myQueuesZnode);
  }

  @Override
  public SortedMap<String, SortedSet<String>> claimQueues(String regionserverZnode) {
    SortedMap<String, SortedSet<String>> newQueues = new TreeMap<String, SortedSet<String>>();
    // check whether there is multi support. If yes, use it.
    if (conf.getBoolean(HConstants.ZOOKEEPER_USEMULTI, true)) {
      LOG.info("Atomically moving " + regionserverZnode + "'s WALs to my queue");
      newQueues = copyQueuesFromRSUsingMulti(regionserverZnode);
    } else {
      LOG.info("Moving " + regionserverZnode + "'s wals to my queue");
      if (!lockOtherRS(regionserverZnode)) {
        return newQueues;
      }
      newQueues = copyQueuesFromRS(regionserverZnode);
      deleteAnotherRSQueues(regionserverZnode);
    }
    return newQueues;
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
    return listOfQueues;
  }

  /**
   * Try to set a lock in another region server's znode.
   * @param znode the server names of the other server
   * @return true if the lock was acquired, false in every other cases
   */
  private boolean lockOtherRS(String znode) {
    try {
      String parent = ZKUtil.joinZNode(this.queuesZNode, znode);
      if (parent.equals(this.myQueuesZnode)) {
        LOG.warn("Won't lock because this is us, we're dead!");
        return false;
      }
      String p = ZKUtil.joinZNode(parent, RS_LOCK_ZNODE);
      ZKUtil.createAndWatch(this.zookeeper, p, lockToByteArray(this.myQueuesZnode));
    } catch (KeeperException e) {
      // This exception will pop up if the znode under which we're trying to
      // create the lock is already deleted by another region server, meaning
      // that the transfer already occurred.
      // NoNode => transfer is done and znodes are already deleted
      // NodeExists => lock znode already created by another RS
      if (e instanceof KeeperException.NoNodeException
          || e instanceof KeeperException.NodeExistsException) {
        LOG.info("Won't transfer the queue," + " another RS took care of it because of: "
            + e.getMessage());
      } else {
        LOG.info("Failed lock other rs", e);
      }
      return false;
    }
    return true;
  }

  /**
   * Delete all the replication queues for a given region server.
   * @param regionserverZnode The znode of the region server to delete.
   */
  private void deleteAnotherRSQueues(String regionserverZnode) {
    String fullpath = ZKUtil.joinZNode(this.queuesZNode, regionserverZnode);
    try {
      List<String> clusters = ZKUtil.listChildrenNoWatch(this.zookeeper, fullpath);
      for (String cluster : clusters) {
        // No need to delete, it will be deleted later.
        if (cluster.equals(RS_LOCK_ZNODE)) {
          continue;
        }
        String fullClusterPath = ZKUtil.joinZNode(fullpath, cluster);
        ZKUtil.deleteNodeRecursively(this.zookeeper, fullClusterPath);
      }
      // Finish cleaning up
      ZKUtil.deleteNodeRecursively(this.zookeeper, fullpath);
    } catch (KeeperException e) {
      if (e instanceof KeeperException.NoNodeException
          || e instanceof KeeperException.NotEmptyException) {
        // Testing a special case where another region server was able to
        // create a lock just after we deleted it, but then was also able to
        // delete the RS znode before us or its lock znode is still there.
        if (e.getPath().equals(fullpath)) {
          return;
        }
      }
      this.abortable.abort("Failed to delete replication queues for region server: "
          + regionserverZnode, e);
    }
  }

  /**
   * It "atomically" copies all the wals queues from another region server and returns them all
   * sorted per peer cluster (appended with the dead server's znode).
   * @param znode pertaining to the region server to copy the queues from
   * @return WAL queues sorted per peer cluster
   */
  private SortedMap<String, SortedSet<String>> copyQueuesFromRSUsingMulti(String znode) {
    SortedMap<String, SortedSet<String>> queues = new TreeMap<String, SortedSet<String>>();
    // hbase/replication/rs/deadrs
    String deadRSZnodePath = ZKUtil.joinZNode(this.queuesZNode, znode);
    List<String> peerIdsToProcess = null;
    List<ZKUtilOp> listOfOps = new ArrayList<ZKUtil.ZKUtilOp>();
    try {
      peerIdsToProcess = ZKUtil.listChildrenNoWatch(this.zookeeper, deadRSZnodePath);
      if (peerIdsToProcess == null) return queues; // node already processed
      for (String peerId : peerIdsToProcess) {
        ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(peerId);
        if (!peerExists(replicationQueueInfo.getPeerId())) {
          LOG.warn("Peer " + peerId + " didn't exist, skipping the replay");
          // Protection against moving orphaned queues
          continue;
        }
        String newPeerId = peerId + "-" + znode;
        String newPeerZnode = ZKUtil.joinZNode(this.myQueuesZnode, newPeerId);
        // check the logs queue for the old peer cluster
        String oldClusterZnode = ZKUtil.joinZNode(deadRSZnodePath, peerId);
        List<String> wals = ZKUtil.listChildrenNoWatch(this.zookeeper, oldClusterZnode);
        if (wals == null || wals.size() == 0) {
          listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));
          continue; // empty log queue.
        }
        // create the new cluster znode
        SortedSet<String> logQueue = new TreeSet<String>();
        queues.put(newPeerId, logQueue);
        ZKUtilOp op = ZKUtilOp.createAndFailSilent(newPeerZnode, HConstants.EMPTY_BYTE_ARRAY);
        listOfOps.add(op);
        // get the offset of the logs and set it to new znodes
        for (String wal : wals) {
          String oldWalZnode = ZKUtil.joinZNode(oldClusterZnode, wal);
          byte[] logOffset = ZKUtil.getData(this.zookeeper, oldWalZnode);
          LOG.debug("Creating " + wal + " with data " + Bytes.toString(logOffset));
          String newLogZnode = ZKUtil.joinZNode(newPeerZnode, wal);
          listOfOps.add(ZKUtilOp.createAndFailSilent(newLogZnode, logOffset));
          // add ops for deleting
          listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldWalZnode));
          logQueue.add(wal);
        }
        // add delete op for peer
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));
      }
      // add delete op for dead rs, this will update the cversion of the parent.
      // The reader will make optimistic locking with this to get a consistent
      // snapshot
      listOfOps.add(ZKUtilOp.deleteNodeFailSilent(deadRSZnodePath));
      if (LOG.isTraceEnabled()) LOG.trace(" The multi list size is: " + listOfOps.size());
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, false);
      if (LOG.isTraceEnabled()) LOG.trace("Atomically moved the dead regionserver logs. ");
    } catch (KeeperException e) {
      // Multi call failed; it looks like some other regionserver took away the logs.
      LOG.warn("Got exception in copyQueuesFromRSUsingMulti: ", e);
      queues.clear();
    } catch (InterruptedException e) {
      LOG.warn("Got exception in copyQueuesFromRSUsingMulti: ", e);
      queues.clear();
      Thread.currentThread().interrupt();
    }
    return queues;
  }

  /**
   * This methods copies all the wals queues from another region server and returns them all sorted
   * per peer cluster (appended with the dead server's znode)
   * @param znode server names to copy
   * @return all wals for all peers of that cluster, null if an error occurred
   */
  private SortedMap<String, SortedSet<String>> copyQueuesFromRS(String znode) {
    // TODO this method isn't atomic enough, we could start copying and then
    // TODO fail for some reason and we would end up with znodes we don't want.
    SortedMap<String, SortedSet<String>> queues = new TreeMap<String, SortedSet<String>>();
    try {
      String nodePath = ZKUtil.joinZNode(this.queuesZNode, znode);
      List<String> clusters = ZKUtil.listChildrenNoWatch(this.zookeeper, nodePath);
      // We have a lock znode in there, it will count as one.
      if (clusters == null || clusters.size() <= 1) {
        return queues;
      }
      // The lock isn't a peer cluster, remove it
      clusters.remove(RS_LOCK_ZNODE);
      for (String cluster : clusters) {
        ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(cluster);
        if (!peerExists(replicationQueueInfo.getPeerId())) {
          LOG.warn("Peer " + cluster + " didn't exist, skipping the replay");
          // Protection against moving orphaned queues
          continue;
        }
        // We add the name of the recovered RS to the new znode, we can even
        // do that for queues that were recovered 10 times giving a znode like
        // number-startcode-number-otherstartcode-number-anotherstartcode-etc
        String newCluster = cluster + "-" + znode;
        String newClusterZnode = ZKUtil.joinZNode(this.myQueuesZnode, newCluster);
        String clusterPath = ZKUtil.joinZNode(nodePath, cluster);
        List<String> wals = ZKUtil.listChildrenNoWatch(this.zookeeper, clusterPath);
        // That region server didn't have anything to replicate for this cluster
        if (wals == null || wals.size() == 0) {
          continue;
        }
        ZKUtil.createNodeIfNotExistsAndWatch(this.zookeeper, newClusterZnode,
          HConstants.EMPTY_BYTE_ARRAY);
        SortedSet<String> logQueue = new TreeSet<String>();
        queues.put(newCluster, logQueue);
        for (String wal : wals) {
          String z = ZKUtil.joinZNode(clusterPath, wal);
          byte[] positionBytes = ZKUtil.getData(this.zookeeper, z);
          long position = 0;
          try {
            position = ZKUtil.parseWALPositionFrom(positionBytes);
          } catch (DeserializationException e) {
            LOG.warn("Failed parse of wal position from the following znode: " + z
                + ", Exception: " + e);
          }
          LOG.debug("Creating " + wal + " with data " + position);
          String child = ZKUtil.joinZNode(newClusterZnode, wal);
          // Position doesn't actually change, we are just deserializing it for
          // logging, so just use the already serialized version
          ZKUtil.createAndWatch(this.zookeeper, child, positionBytes);
          logQueue.add(wal);
        }
      }
    } catch (KeeperException e) {
      this.abortable.abort("Copy queues from rs", e);
    } catch (InterruptedException e) {
      LOG.warn(e);
      Thread.currentThread().interrupt();
    }
    return queues;
  }

  /**
   * @param lockOwner
   * @return Serialized protobuf of <code>lockOwner</code> with pb magic prefix prepended suitable
   *         for use as content of an replication lock during region server fail over.
   */
  static byte[] lockToByteArray(final String lockOwner) {
    byte[] bytes =
        ZooKeeperProtos.ReplicationLock.newBuilder().setLockOwner(lockOwner).build().toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }
}
