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
package org.apache.hadoop.hbase.replication;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * ZK based replication queue storage.
 * <p>
 * The base znode for each regionserver is the regionserver name. For example:
 *
 * <pre>
 * /hbase/replication/rs/hostname.example.org,6020,1234
 * </pre>
 *
 * Within this znode, the region server maintains a set of WAL replication queues. These queues are
 * represented by child znodes named using there give queue id. For example:
 *
 * <pre>
 * /hbase/replication/rs/hostname.example.org,6020,1234/1
 * /hbase/replication/rs/hostname.example.org,6020,1234/2
 * </pre>
 *
 * Each queue has one child znode for every WAL that still needs to be replicated. The value of
 * these WAL child znodes is the latest position that has been replicated. This position is updated
 * every time a WAL entry is replicated. For example:
 *
 * <pre>
 * /hbase/replication/rs/hostname.example.org,6020,1234/1/23522342.23422 [VALUE: 254]
 * </pre>
 */
@InterfaceAudience.Private
class ZKReplicationQueueStorage extends ZKReplicationStorageBase
    implements ReplicationQueueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(ZKReplicationQueueStorage.class);

  public static final String ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_KEY =
      "zookeeper.znode.replication.hfile.refs";
  public static final String ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_DEFAULT = "hfile-refs";

  public static final String ZOOKEEPER_ZNODE_REPLICATION_REGIONS_KEY =
      "zookeeper.znode.replication.regions";
  public static final String ZOOKEEPER_ZNODE_REPLICATION_REGIONS_DEFAULT = "regions";

  /**
   * The name of the znode that contains all replication queues
   */
  private final String queuesZNode;

  /**
   * The name of the znode that contains queues of hfile references to be replicated
   */
  private final String hfileRefsZNode;

  final String regionsZNode;

  public ZKReplicationQueueStorage(ZKWatcher zookeeper, Configuration conf) {
    super(zookeeper, conf);

    String queuesZNodeName = conf.get("zookeeper.znode.replication.rs", "rs");
    String hfileRefsZNodeName = conf.get(ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_KEY,
      ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_DEFAULT);
    this.queuesZNode = ZNodePaths.joinZNode(replicationZNode, queuesZNodeName);
    this.hfileRefsZNode = ZNodePaths.joinZNode(replicationZNode, hfileRefsZNodeName);
    this.regionsZNode = ZNodePaths.joinZNode(replicationZNode, conf
        .get(ZOOKEEPER_ZNODE_REPLICATION_REGIONS_KEY, ZOOKEEPER_ZNODE_REPLICATION_REGIONS_DEFAULT));
  }

  @Override
  public String getRsNode(ServerName serverName) {
    return ZNodePaths.joinZNode(queuesZNode, serverName.getServerName());
  }

  private String getQueueNode(ServerName serverName, String queueId) {
    return ZNodePaths.joinZNode(getRsNode(serverName), queueId);
  }

  private String getFileNode(String queueNode, String fileName) {
    return ZNodePaths.joinZNode(queueNode, fileName);
  }

  private String getFileNode(ServerName serverName, String queueId, String fileName) {
    return getFileNode(getQueueNode(serverName, queueId), fileName);
  }

  /**
   * <p>
   * Put all regions under /hbase/replication/regions znode will lead to too many children because
   * of the huge number of regions in real production environment. So here we will distribute the
   * znodes to multiple directories.
   * </p>
   * <p>
   * So the final znode path will be format like this:
   *
   * <pre>
   * /hbase/replication/regions/dd/04/e76a6966d4ffa908ed0586764767-100
   * </pre>
   *
   * Here the full encoded region name is dd04e76a6966d4ffa908ed0586764767, and we use the first two
   * characters 'dd' as the first level directory name, and use the next two characters '04' as the
   * second level directory name, and the rest part as the prefix of the znode, and the suffix '100'
   * is the peer id.
   * </p>
   * @param encodedRegionName the encoded region name.
   * @param peerId peer id for replication.
   * @return ZNode path to persist the max sequence id that we've pushed for the given region and
   *         peer.
   */
  String getSerialReplicationRegionPeerNode(String encodedRegionName, String peerId) {
    if (encodedRegionName == null || encodedRegionName.length() != RegionInfo.MD5_HEX_LENGTH) {
      throw new IllegalArgumentException(
          "Invalid encoded region name: " + encodedRegionName + ", length should be 32.");
    }
    return new StringBuilder(regionsZNode).append(ZNodePaths.ZNODE_PATH_SEPARATOR)
            .append(encodedRegionName, 0, 2).append(ZNodePaths.ZNODE_PATH_SEPARATOR)
            .append(encodedRegionName, 2, 4).append(ZNodePaths.ZNODE_PATH_SEPARATOR)
            .append(encodedRegionName, 4, encodedRegionName.length()).append("-").append(peerId)
            .toString();
  }

  @Override
  public void removeQueue(ServerName serverName, String queueId) throws ReplicationException {
    try {
      ZKUtil.deleteNodeRecursively(zookeeper, getQueueNode(serverName, queueId));
    } catch (KeeperException e) {
      throw new ReplicationException(
          "Failed to delete queue (serverName=" + serverName + ", queueId=" + queueId + ")", e);
    }
  }

  @Override
  public void addWAL(ServerName serverName, String queueId, String fileName)
      throws ReplicationException {
    try {
      ZKUtil.createWithParents(zookeeper, getFileNode(serverName, queueId, fileName));
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to add wal to queue (serverName=" + serverName
          + ", queueId=" + queueId + ", fileName=" + fileName + ")", e);
    }
  }

  @Override
  public void removeWAL(ServerName serverName, String queueId, String fileName)
      throws ReplicationException {
    String fileNode = getFileNode(serverName, queueId, fileName);
    try {
      ZKUtil.deleteNode(zookeeper, fileNode);
    } catch (NoNodeException e) {
      LOG.warn("{} already deleted when removing log", fileNode);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to remove wal from queue (serverName=" + serverName +
        ", queueId=" + queueId + ", fileName=" + fileName + ")", e);
    }
  }

  private void addLastSeqIdsToOps(String queueId, Map<String, Long> lastSeqIds,
      List<ZKUtilOp> listOfOps) throws KeeperException, ReplicationException {
    String peerId = new ReplicationQueueInfo(queueId).getPeerId();
    for (Entry<String, Long> lastSeqEntry : lastSeqIds.entrySet()) {
      String path = getSerialReplicationRegionPeerNode(lastSeqEntry.getKey(), peerId);
      Pair<Long, Integer> p = getLastSequenceIdWithVersion(lastSeqEntry.getKey(), peerId);
      byte[] data = ZKUtil.positionToByteArray(lastSeqEntry.getValue());
      if (p.getSecond() < 0) { // ZNode does not exist.
        ZKUtil.createWithParents(zookeeper,
          path.substring(0, path.lastIndexOf(ZNodePaths.ZNODE_PATH_SEPARATOR)));
        listOfOps.add(ZKUtilOp.createAndFailSilent(path, data));
        continue;
      }
      // Perform CAS in a specific version v0 (HBASE-20138)
      int v0 = p.getSecond();
      long lastPushedSeqId = p.getFirst();
      if (lastSeqEntry.getValue() <= lastPushedSeqId) {
        continue;
      }
      listOfOps.add(ZKUtilOp.setData(path, data, v0));
    }
  }

  @Override
  public void setWALPosition(ServerName serverName, String queueId, String fileName, long position,
      Map<String, Long> lastSeqIds) throws ReplicationException {
    try {
      for (int retry = 0;; retry++) {
        List<ZKUtilOp> listOfOps = new ArrayList<>();
        if (position > 0) {
          listOfOps.add(ZKUtilOp.setData(getFileNode(serverName, queueId, fileName),
            ZKUtil.positionToByteArray(position)));
        }
        // Persist the max sequence id(s) of regions for serial replication atomically.
        addLastSeqIdsToOps(queueId, lastSeqIds, listOfOps);
        if (listOfOps.isEmpty()) {
          return;
        }
        try {
          ZKUtil.multiOrSequential(zookeeper, listOfOps, false);
          return;
        } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
          LOG.warn(
            "Bad version(or node exist) when persist the last pushed sequence id to zookeeper "
                + "storage, Retry = " + retry + ", serverName=" + serverName + ", queueId="
                + queueId + ", fileName=" + fileName);
        }
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to set log position (serverName=" + serverName
          + ", queueId=" + queueId + ", fileName=" + fileName + ", position=" + position + ")", e);
    }
  }

  /**
   * Return the {lastPushedSequenceId, ZNodeDataVersion} pair. if ZNodeDataVersion is -1, it means
   * that the ZNode does not exist.
   */
  protected Pair<Long, Integer> getLastSequenceIdWithVersion(String encodedRegionName,
      String peerId) throws KeeperException {
    Stat stat = new Stat();
    String path = getSerialReplicationRegionPeerNode(encodedRegionName, peerId);
    byte[] data = ZKUtil.getDataNoWatch(zookeeper, path, stat);
    if (data == null) {
      // ZNode does not exist, so just return version -1 to indicate that no node exist.
      return Pair.newPair(HConstants.NO_SEQNUM, -1);
    }
    try {
      return Pair.newPair(ZKUtil.parseWALPositionFrom(data), stat.getVersion());
    } catch (DeserializationException de) {
      LOG.warn("Failed to parse log position (region=" + encodedRegionName + ", peerId=" + peerId
          + "), data=" + Bytes.toStringBinary(data));
    }
    return Pair.newPair(HConstants.NO_SEQNUM, stat.getVersion());
  }

  @Override
  public long getLastSequenceId(String encodedRegionName, String peerId)
      throws ReplicationException {
    try {
      return getLastSequenceIdWithVersion(encodedRegionName, peerId).getFirst();
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to get last pushed sequence id (encodedRegionName="
          + encodedRegionName + ", peerId=" + peerId + ")", e);
    }
  }

  @Override
  public void setLastSequenceIds(String peerId, Map<String, Long> lastSeqIds)
      throws ReplicationException {
    try {
      // No need CAS and retry here, because it'll call setLastSequenceIds() for disabled peers
      // only, so no conflict happen.
      List<ZKUtilOp> listOfOps = new ArrayList<>();
      for (Entry<String, Long> lastSeqEntry : lastSeqIds.entrySet()) {
        String path = getSerialReplicationRegionPeerNode(lastSeqEntry.getKey(), peerId);
        ZKUtil.createWithParents(zookeeper, path);
        listOfOps.add(ZKUtilOp.setData(path, ZKUtil.positionToByteArray(lastSeqEntry.getValue())));
      }
      if (!listOfOps.isEmpty()) {
        ZKUtil.multiOrSequential(zookeeper, listOfOps, true);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to set last sequence ids, peerId=" + peerId
          + ", size of lastSeqIds=" + lastSeqIds.size(), e);
    }
  }

  @Override
  public void removeLastSequenceIds(String peerId) throws ReplicationException {
    String suffix = "-" + peerId;
    try {
      StringBuilder sb = new StringBuilder(regionsZNode);
      int regionsZNodeLength = regionsZNode.length();
      int levelOneLength = regionsZNodeLength + 3;
      int levelTwoLength = levelOneLength + 3;
      List<String> levelOneDirs = ZKUtil.listChildrenNoWatch(zookeeper, regionsZNode);
      // it is possible that levelOneDirs is null if we haven't write any last pushed sequence ids
      // yet, so we need an extra check here.
      if (CollectionUtils.isEmpty(levelOneDirs)) {
        return;
      }
      for (String levelOne : levelOneDirs) {
        sb.append(ZNodePaths.ZNODE_PATH_SEPARATOR).append(levelOne);
        for (String levelTwo : ZKUtil.listChildrenNoWatch(zookeeper, sb.toString())) {
          sb.append(ZNodePaths.ZNODE_PATH_SEPARATOR).append(levelTwo);
          for (String znode : ZKUtil.listChildrenNoWatch(zookeeper, sb.toString())) {
            if (znode.endsWith(suffix)) {
              sb.append(ZNodePaths.ZNODE_PATH_SEPARATOR).append(znode);
              ZKUtil.deleteNode(zookeeper, sb.toString());
              sb.setLength(levelTwoLength);
            }
          }
          sb.setLength(levelOneLength);
        }
        sb.setLength(regionsZNodeLength);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to remove all last sequence ids, peerId=" + peerId, e);
    }
  }

  @Override
  public void removeLastSequenceIds(String peerId, List<String> encodedRegionNames)
      throws ReplicationException {
    try {
      List<ZKUtilOp> listOfOps =
        encodedRegionNames.stream().map(n -> getSerialReplicationRegionPeerNode(n, peerId))
          .map(ZKUtilOp::deleteNodeFailSilent).collect(Collectors.toList());
      ZKUtil.multiOrSequential(zookeeper, listOfOps, true);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to remove last sequence ids, peerId=" + peerId +
        ", encodedRegionNames.size=" + encodedRegionNames.size(), e);
    }
  }

  @Override
  public long getWALPosition(ServerName serverName, String queueId, String fileName)
      throws ReplicationException {
    byte[] bytes;
    try {
      bytes = ZKUtil.getData(zookeeper, getFileNode(serverName, queueId, fileName));
    } catch (KeeperException | InterruptedException e) {
      throw new ReplicationException("Failed to get log position (serverName=" + serverName +
        ", queueId=" + queueId + ", fileName=" + fileName + ")", e);
    }
    try {
      return ZKUtil.parseWALPositionFrom(bytes);
    } catch (DeserializationException de) {
      LOG.warn("Failed parse log position (serverName={}, queueId={}, fileName={})",
          serverName, queueId, fileName);
    }
    // if we can not parse the position, start at the beginning of the wal file again
    return 0;
  }

  /**
   * This implement must update the cversion of root {@link #queuesZNode}. The optimistic lock of
   * the {@link #getAllWALs()} method is based on the cversion of root {@link #queuesZNode}.
   * @see #getAllWALs() to show the usage of the cversion of root {@link #queuesZNode} .
   */
  @Override
  public Pair<String, SortedSet<String>> claimQueue(ServerName sourceServerName, String queueId,
      ServerName destServerName) throws ReplicationException {
    LOG.info("Atomically moving {}/{}'s WALs to {}", sourceServerName, queueId, destServerName);
    try {
      ZKUtil.createWithParents(zookeeper, getRsNode(destServerName));
    } catch (KeeperException e) {
      throw new ReplicationException(
          "Claim queue queueId=" + queueId + " from " + sourceServerName + " to " + destServerName +
            " failed when creating the node for " + destServerName,
          e);
    }
    String newQueueId = queueId + "-" + sourceServerName;
    try {
      String oldQueueNode = getQueueNode(sourceServerName, queueId);
      List<String> wals = ZKUtil.listChildrenNoWatch(zookeeper, oldQueueNode);
      if (CollectionUtils.isEmpty(wals)) {
        ZKUtil.deleteNodeFailSilent(zookeeper, oldQueueNode);
        LOG.info("Removed empty {}/{}", sourceServerName, queueId);
        return new Pair<>(newQueueId, Collections.emptySortedSet());
      }
      String newQueueNode = getQueueNode(destServerName, newQueueId);
      List<ZKUtilOp> listOfOps = new ArrayList<>();
      SortedSet<String> logQueue = new TreeSet<>();
      // create the new cluster znode
      listOfOps.add(ZKUtilOp.createAndFailSilent(newQueueNode, HConstants.EMPTY_BYTE_ARRAY));
      // get the offset of the logs and set it to new znodes
      for (String wal : wals) {
        String oldWalNode = getFileNode(oldQueueNode, wal);
        byte[] logOffset = ZKUtil.getData(this.zookeeper, oldWalNode);
        LOG.debug("Creating {} with data {}", wal, Bytes.toStringBinary(logOffset));
        String newWalNode = getFileNode(newQueueNode, wal);
        listOfOps.add(ZKUtilOp.createAndFailSilent(newWalNode, logOffset));
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldWalNode));
        logQueue.add(wal);
      }
      // add delete op for peer
      listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldQueueNode));
      // Append new queue id for prevent lock competition in zookeeper server.
      String claimLockZNode = ZNodePaths.joinZNode(queuesZNode, "cversion_" + newQueueId);
      // A trick for update the cversion of root queuesZNode .
      // The optimistic lock of the getAllWALs() method is based on the cversion of root queuesZNode
      listOfOps.add(ZKUtilOp.createAndFailSilent(claimLockZNode, HConstants.EMPTY_BYTE_ARRAY));
      listOfOps.add(ZKUtilOp.deleteNodeFailSilent(claimLockZNode));

      LOG.trace("The multi list size is {}", listOfOps.size());
      ZKUtil.multiOrSequential(zookeeper, listOfOps, false);

      LOG.info("Atomically moved {}/{}'s WALs to {}", sourceServerName, queueId, destServerName);
      return new Pair<>(newQueueId, logQueue);
    } catch (NoNodeException | NodeExistsException | NotEmptyException | BadVersionException e) {
      // Multi call failed; it looks like some other regionserver took away the logs.
      // These exceptions mean that zk tells us the request can not be execute. So return an empty
      // queue to tell the upper layer that claim nothing. For other types of exception should be
      // thrown out to notify the upper layer.
      LOG.info("Claim queue queueId={} from {} to {} failed with {}, someone else took the log?",
          queueId,sourceServerName, destServerName, e.toString());
      return new Pair<>(newQueueId, Collections.emptySortedSet());
    } catch (KeeperException | InterruptedException e) {
      throw new ReplicationException("Claim queue queueId=" + queueId + " from " +
        sourceServerName + " to " + destServerName + " failed", e);
    }
  }

  @Override
  public void removeReplicatorIfQueueIsEmpty(ServerName serverName) throws ReplicationException {
    try {
      ZKUtil.deleteNodeFailSilent(zookeeper, getRsNode(serverName));
    } catch (NotEmptyException e) {
      // keep silence to avoid logging too much.
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to remove replicator for " + serverName, e);
    }
  }

  private List<ServerName> getListOfReplicators0() throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zookeeper, queuesZNode);
    if (children == null) {
      children = Collections.emptyList();
    }
    return children.stream().map(ServerName::parseServerName).collect(toList());
  }

  @Override
  public List<ServerName> getListOfReplicators() throws ReplicationException {
    try {
      return getListOfReplicators0();
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to get list of replicators", e);
    }
  }

  private List<String> getWALsInQueue0(ServerName serverName, String queueId)
      throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zookeeper, getQueueNode(serverName,
        queueId));
    return children != null ? children : Collections.emptyList();
  }

  @Override
  public List<String> getWALsInQueue(ServerName serverName, String queueId)
      throws ReplicationException {
    try {
      return getWALsInQueue0(serverName, queueId);
    } catch (KeeperException e) {
      throw new ReplicationException(
          "Failed to get wals in queue (serverName=" + serverName + ", queueId=" + queueId + ")",
          e);
    }
  }

  private List<String> getAllQueues0(ServerName serverName) throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zookeeper, getRsNode(serverName));
    return children != null ? children : Collections.emptyList();
  }

  @Override
  public List<String> getAllQueues(ServerName serverName) throws ReplicationException {
    try {
      return getAllQueues0(serverName);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to get all queues (serverName=" + serverName + ")", e);
    }
  }

  // will be overridden in UTs
  protected int getQueuesZNodeCversion() throws KeeperException {
    Stat stat = new Stat();
    ZKUtil.getDataNoWatch(this.zookeeper, this.queuesZNode, stat);
    return stat.getCversion();
  }

  /**
   * The optimistic lock of this implement is based on the cversion of root {@link #queuesZNode}.
   * Therefore, we must update the cversion of root {@link #queuesZNode} when migrate wal nodes to
   * other queues.
   * @see #claimQueue(ServerName, String, ServerName) as an example of updating root
   * {@link #queuesZNode} cversion.
   */
  @Override
  public Set<String> getAllWALs() throws ReplicationException {
    try {
      for (int retry = 0;; retry++) {
        int v0 = getQueuesZNodeCversion();
        List<ServerName> rss = getListOfReplicators0();
        if (rss.isEmpty()) {
          LOG.debug("Didn't find a RegionServer that replicates, won't prevent deletions.");
          return Collections.emptySet();
        }
        Set<String> wals = new HashSet<>();
        for (ServerName rs : rss) {
          for (String queueId : getAllQueues0(rs)) {
            wals.addAll(getWALsInQueue0(rs, queueId));
          }
        }
        int v1 = getQueuesZNodeCversion();
        if (v0 == v1) {
          return wals;
        }
        LOG.info("Replication queue node cversion changed from %d to %d, retry = %d",
            v0, v1, retry);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to get all wals", e);
    }
  }

  private String getHFileRefsPeerNode(String peerId) {
    return ZNodePaths.joinZNode(hfileRefsZNode, peerId);
  }

  private String getHFileNode(String peerNode, String fileName) {
    return ZNodePaths.joinZNode(peerNode, fileName);
  }

  @Override
  public void addPeerToHFileRefs(String peerId) throws ReplicationException {
    String peerNode = getHFileRefsPeerNode(peerId);
    try {
      if (ZKUtil.checkExists(zookeeper, peerNode) == -1) {
        LOG.info("Adding peer {} to hfile reference queue.", peerId);
        ZKUtil.createWithParents(zookeeper, peerNode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to add peer " + peerId + " to hfile reference queue.",
          e);
    }
  }

  @Override
  public void removePeerFromHFileRefs(String peerId) throws ReplicationException {
    String peerNode = getHFileRefsPeerNode(peerId);
    try {
      if (ZKUtil.checkExists(zookeeper, peerNode) == -1) {
        LOG.debug("Peer {} not found in hfile reference queue.", peerNode);
      } else {
        LOG.info("Removing peer {} from hfile reference queue.", peerNode);
        ZKUtil.deleteNodeRecursively(zookeeper, peerNode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException(
          "Failed to remove peer " + peerId + " from hfile reference queue.", e);
    }
  }

  @Override
  public void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs)
      throws ReplicationException {
    String peerNode = getHFileRefsPeerNode(peerId);
    LOG.debug("Adding hfile references {} in queue {}", pairs, peerNode);
    List<ZKUtilOp> listOfOps = pairs.stream().map(p -> p.getSecond().getName())
        .map(n -> getHFileNode(peerNode, n))
        .map(f -> ZKUtilOp.createAndFailSilent(f, HConstants.EMPTY_BYTE_ARRAY)).collect(toList());
    LOG.debug("The multi list size for adding hfile references in zk for node {} is {}",
          peerNode, listOfOps.size());
    try {
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, true);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to add hfile reference to peer " + peerId, e);
    }
  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) throws ReplicationException {
    String peerNode = getHFileRefsPeerNode(peerId);
    LOG.debug("Removing hfile references {} from queue {}", files, peerNode);

    List<ZKUtilOp> listOfOps = files.stream().map(n -> getHFileNode(peerNode, n))
        .map(ZKUtilOp::deleteNodeFailSilent).collect(toList());
    LOG.debug("The multi list size for removing hfile references in zk for node {} is {}",
        peerNode, listOfOps.size());
    try {
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, true);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to remove hfile reference from peer " + peerId, e);
    }
  }

  private List<String> getAllPeersFromHFileRefsQueue0() throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zookeeper, hfileRefsZNode);
    return children != null ? children : Collections.emptyList();
  }

  @Override
  public List<String> getAllPeersFromHFileRefsQueue() throws ReplicationException {
    try {
      return getAllPeersFromHFileRefsQueue0();
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to get list of all peers in hfile references node.",
          e);
    }
  }

  private List<String> getReplicableHFiles0(String peerId) throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(this.zookeeper,
        getHFileRefsPeerNode(peerId));
    return children != null ? children : Collections.emptyList();
  }

  @Override
  public List<String> getReplicableHFiles(String peerId) throws ReplicationException {
    try {
      return getReplicableHFiles0(peerId);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to get list of hfile references for peer " + peerId,
          e);
    }
  }

  // will be overridden in UTs
  protected int getHFileRefsZNodeCversion() throws ReplicationException {
    Stat stat = new Stat();
    try {
      ZKUtil.getDataNoWatch(zookeeper, hfileRefsZNode, stat);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to get stat of replication hfile references node.", e);
    }
    return stat.getCversion();
  }

  @Override
  public Set<String> getAllHFileRefs() throws ReplicationException {
    try {
      for (int retry = 0;; retry++) {
        int v0 = getHFileRefsZNodeCversion();
        List<String> peers = getAllPeersFromHFileRefsQueue();
        if (peers.isEmpty()) {
          LOG.debug("Didn't find any peers with hfile references, won't prevent deletions.");
          return Collections.emptySet();
        }
        Set<String> hfileRefs = new HashSet<>();
        for (String peer : peers) {
          hfileRefs.addAll(getReplicableHFiles0(peer));
        }
        int v1 = getHFileRefsZNodeCversion();
        if (v0 == v1) {
          return hfileRefs;
        }
        LOG.debug("Replication hfile references node cversion changed from %d to %d, retry = %d",
            v0, v1, retry);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to get all hfile refs", e);
    }
  }
}
