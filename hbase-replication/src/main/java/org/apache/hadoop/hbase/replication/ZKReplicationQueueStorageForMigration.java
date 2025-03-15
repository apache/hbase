/*
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

import com.google.errorprone.annotations.RestrictedApi;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Just retain a small set of the methods for the old zookeeper based replication queue storage, for
 * migrating.
 */
@InterfaceAudience.Private
public class ZKReplicationQueueStorageForMigration extends ZKReplicationStorageBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(ZKReplicationQueueStorageForMigration.class);

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

  private final String regionsZNode;

  public ZKReplicationQueueStorageForMigration(ZKWatcher zookeeper, Configuration conf) {
    super(zookeeper, conf);
    String queuesZNodeName = conf.get("zookeeper.znode.replication.rs", "rs");
    String hfileRefsZNodeName = conf.get(ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_KEY,
      ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_DEFAULT);
    this.queuesZNode = ZNodePaths.joinZNode(replicationZNode, queuesZNodeName);
    this.hfileRefsZNode = ZNodePaths.joinZNode(replicationZNode, hfileRefsZNodeName);
    this.regionsZNode = ZNodePaths.joinZNode(replicationZNode, conf
      .get(ZOOKEEPER_ZNODE_REPLICATION_REGIONS_KEY, ZOOKEEPER_ZNODE_REPLICATION_REGIONS_DEFAULT));
  }

  public interface MigrationIterator<T> {

    T next() throws Exception;
  }

  @SuppressWarnings("rawtypes")
  private static final MigrationIterator EMPTY_ITER = new MigrationIterator() {

    @Override
    public Object next() {
      return null;
    }
  };

  public static final class ZkReplicationQueueData {

    private final ReplicationQueueId queueId;

    private final Map<String, Long> walOffsets;

    public ZkReplicationQueueData(ReplicationQueueId queueId, Map<String, Long> walOffsets) {
      this.queueId = queueId;
      this.walOffsets = walOffsets;
    }

    public ReplicationQueueId getQueueId() {
      return queueId;
    }

    public Map<String, Long> getWalOffsets() {
      return walOffsets;
    }
  }

  private String getRsNode(ServerName serverName) {
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

  static final String REGION_REPLICA_REPLICATION_PEER = "region_replica_replication";

  @SuppressWarnings("unchecked")
  public MigrationIterator<Pair<ServerName, List<ZkReplicationQueueData>>> listAllQueues()
    throws KeeperException {
    List<String> replicators = ZKUtil.listChildrenNoWatch(zookeeper, queuesZNode);
    if (replicators == null || replicators.isEmpty()) {
      ZKUtil.deleteNodeRecursively(zookeeper, queuesZNode);
      return EMPTY_ITER;
    }
    Iterator<String> iter = replicators.iterator();
    return new MigrationIterator<Pair<ServerName, List<ZkReplicationQueueData>>>() {

      private ServerName previousServerName;

      private boolean hasRegionReplicaReplicationQueue;

      private void cleanupQueuesWithoutRegionReplicaReplication(ServerName serverName)
        throws Exception {
        String rsZNode = getRsNode(serverName);
        List<String> queueIdList = ZKUtil.listChildrenNoWatch(zookeeper, rsZNode);
        if (CollectionUtils.isEmpty(queueIdList)) {
          return;
        }
        for (String queueId : queueIdList) {
          ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
          if (!queueInfo.getPeerId().equals(REGION_REPLICA_REPLICATION_PEER)) {
            ZKUtil.deleteNodeRecursively(zookeeper, getQueueNode(serverName, queueId));
          }
        }
      }

      @Override
      public Pair<ServerName, List<ZkReplicationQueueData>> next() throws Exception {
        if (previousServerName != null) {
          if (hasRegionReplicaReplicationQueue) {
            // if there are region_replica_replication queues, we can not delete it, just delete
            // other queues, see HBASE-29169.
            cleanupQueuesWithoutRegionReplicaReplication(previousServerName);
          } else {
            ZKUtil.deleteNodeRecursively(zookeeper, getRsNode(previousServerName));
          }
        }
        if (!iter.hasNext()) {
          // If there are region_replica_replication queues then we can not delete the data right
          // now, otherwise we may crash the old region servers, see HBASE-29169.
          // The migration procedure has a special step to cleanup everything
          return null;
        }
        hasRegionReplicaReplicationQueue = false;
        String replicator = iter.next();
        ServerName serverName = ServerName.parseServerName(replicator);
        previousServerName = serverName;
        List<String> queueIdList = ZKUtil.listChildrenNoWatch(zookeeper, getRsNode(serverName));
        if (CollectionUtils.isEmpty(queueIdList)) {
          return Pair.newPair(serverName, Collections.emptyList());
        }
        List<ZkReplicationQueueData> queueDataList = new ArrayList<>(queueIdList.size());
        for (String queueIdStr : queueIdList) {
          ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueIdStr);
          if (queueInfo.getPeerId().equals(REGION_REPLICA_REPLICATION_PEER)) {
            // we do not need to migrate the data for this queue, skip
            LOG.debug("Found region replica replication queue {}, skip", queueInfo);
            hasRegionReplicaReplicationQueue = true;
            continue;
          }
          ReplicationQueueId queueId;
          if (queueInfo.getDeadRegionServers().isEmpty()) {
            queueId = new ReplicationQueueId(serverName, queueInfo.getPeerId());
          } else {
            queueId = new ReplicationQueueId(serverName, queueInfo.getPeerId(),
              queueInfo.getDeadRegionServers().get(0));
          }
          List<String> wals =
            ZKUtil.listChildrenNoWatch(zookeeper, getQueueNode(serverName, queueIdStr));
          ZkReplicationQueueData queueData;
          if (wals == null || wals.isEmpty()) {
            queueData = new ZkReplicationQueueData(queueId, Collections.emptyMap());
          } else {
            Map<String, Long> walOffsets = new HashMap<>();
            for (String wal : wals) {
              byte[] data = ZKUtil.getData(zookeeper, getFileNode(serverName, queueIdStr, wal));
              if (data == null || data.length == 0) {
                walOffsets.put(wal, 0L);
              } else {
                walOffsets.put(wal, ZKUtil.parseWALPositionFrom(data));
              }
            }
            queueData = new ZkReplicationQueueData(queueId, walOffsets);
          }
          queueDataList.add(queueData);
        }
        return Pair.newPair(serverName, queueDataList);
      }
    };
  }

  public static final class ZkLastPushedSeqId {

    private final String encodedRegionName;

    private final String peerId;

    private final long lastPushedSeqId;

    ZkLastPushedSeqId(String encodedRegionName, String peerId, long lastPushedSeqId) {
      this.encodedRegionName = encodedRegionName;
      this.peerId = peerId;
      this.lastPushedSeqId = lastPushedSeqId;
    }

    public String getEncodedRegionName() {
      return encodedRegionName;
    }

    public String getPeerId() {
      return peerId;
    }

    public long getLastPushedSeqId() {
      return lastPushedSeqId;
    }

  }

  @SuppressWarnings("unchecked")
  public MigrationIterator<List<ZkLastPushedSeqId>> listAllLastPushedSeqIds()
    throws KeeperException {
    List<String> level1Prefixs = ZKUtil.listChildrenNoWatch(zookeeper, regionsZNode);
    if (level1Prefixs == null || level1Prefixs.isEmpty()) {
      ZKUtil.deleteNodeRecursively(zookeeper, regionsZNode);
      return EMPTY_ITER;
    }
    Iterator<String> level1Iter = level1Prefixs.iterator();
    return new MigrationIterator<List<ZkLastPushedSeqId>>() {

      private String level1Prefix;

      private Iterator<String> level2Iter;

      private String level2Prefix;

      @Override
      public List<ZkLastPushedSeqId> next() throws Exception {
        for (;;) {
          if (level2Iter == null || !level2Iter.hasNext()) {
            if (!level1Iter.hasNext()) {
              ZKUtil.deleteNodeRecursively(zookeeper, regionsZNode);
              return null;
            }
            if (level1Prefix != null) {
              // this will also delete the previous level2Prefix which is under this level1Prefix
              ZKUtil.deleteNodeRecursively(zookeeper,
                ZNodePaths.joinZNode(regionsZNode, level1Prefix));
            }
            level1Prefix = level1Iter.next();
            List<String> level2Prefixes = ZKUtil.listChildrenNoWatch(zookeeper,
              ZNodePaths.joinZNode(regionsZNode, level1Prefix));
            if (level2Prefixes != null) {
              level2Iter = level2Prefixes.iterator();
              // reset level2Prefix as we have switched level1Prefix, otherwise the below delete
              // level2Prefix section will delete the znode with this level2Prefix under the new
              // level1Prefix
              level2Prefix = null;
            }
          } else {
            if (level2Prefix != null) {
              ZKUtil.deleteNodeRecursively(zookeeper,
                ZNodePaths.joinZNode(regionsZNode, level1Prefix, level2Prefix));
            }
            level2Prefix = level2Iter.next();
            List<String> encodedRegionNameAndPeerIds = ZKUtil.listChildrenNoWatch(zookeeper,
              ZNodePaths.joinZNode(regionsZNode, level1Prefix, level2Prefix));
            if (encodedRegionNameAndPeerIds == null || encodedRegionNameAndPeerIds.isEmpty()) {
              return Collections.emptyList();
            }
            List<ZkLastPushedSeqId> lastPushedSeqIds = new ArrayList<>();
            for (String encodedRegionNameAndPeerId : encodedRegionNameAndPeerIds) {
              byte[] data = ZKUtil.getData(zookeeper, ZNodePaths.joinZNode(regionsZNode,
                level1Prefix, level2Prefix, encodedRegionNameAndPeerId));
              long lastPushedSeqId = ZKUtil.parseWALPositionFrom(data);
              Iterator<String> iter = Splitter.on('-').split(encodedRegionNameAndPeerId).iterator();
              String encodedRegionName = level1Prefix + level2Prefix + iter.next();
              String peerId = iter.next();
              lastPushedSeqIds
                .add(new ZkLastPushedSeqId(encodedRegionName, peerId, lastPushedSeqId));
            }
            return Collections.unmodifiableList(lastPushedSeqIds);
          }
        }
      }
    };
  }

  private String getHFileRefsPeerNode(String peerId) {
    return ZNodePaths.joinZNode(hfileRefsZNode, peerId);
  }

  /**
   * Pair&lt;PeerId, List&lt;HFileRefs&gt;&gt;
   */
  @SuppressWarnings("unchecked")
  public MigrationIterator<Pair<String, List<String>>> listAllHFileRefs() throws KeeperException {
    List<String> peerIds = ZKUtil.listChildrenNoWatch(zookeeper, hfileRefsZNode);
    if (peerIds == null || peerIds.isEmpty()) {
      ZKUtil.deleteNodeRecursively(zookeeper, hfileRefsZNode);
      return EMPTY_ITER;
    }
    Iterator<String> iter = peerIds.iterator();
    return new MigrationIterator<Pair<String, List<String>>>() {

      private String previousPeerId;

      @Override
      public Pair<String, List<String>> next() throws KeeperException {
        if (previousPeerId != null) {
          ZKUtil.deleteNodeRecursively(zookeeper, getHFileRefsPeerNode(previousPeerId));
        }
        if (!iter.hasNext()) {
          ZKUtil.deleteNodeRecursively(zookeeper, hfileRefsZNode);
          return null;
        }
        String peerId = iter.next();
        List<String> refs = ZKUtil.listChildrenNoWatch(zookeeper, getHFileRefsPeerNode(peerId));
        previousPeerId = peerId;
        return Pair.newPair(peerId, refs != null ? refs : Collections.emptyList());
      }
    };
  }

  public boolean hasData() throws KeeperException {
    return ZKUtil.checkExists(zookeeper, queuesZNode) != -1
      || ZKUtil.checkExists(zookeeper, regionsZNode) != -1
      || ZKUtil.checkExists(zookeeper, hfileRefsZNode) != -1;
  }

  public void deleteAllData() throws KeeperException {
    ZKUtil.deleteNodeRecursively(zookeeper, queuesZNode);
    ZKUtil.deleteNodeRecursively(zookeeper, regionsZNode);
    ZKUtil.deleteNodeRecursively(zookeeper, hfileRefsZNode);
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  String getQueuesZNode() {
    return queuesZNode;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  String getHfileRefsZNode() {
    return hfileRefsZNode;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  String getRegionsZNode() {
    return regionsZNode;
  }
}
