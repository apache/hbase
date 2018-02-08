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
package org.apache.hadoop.hbase.zookeeper;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_META_REPLICA_NUM;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;
import static org.apache.hadoop.hbase.HConstants.META_REPLICAS_NUM;
import static org.apache.hadoop.hbase.HConstants.SPLIT_LOGDIR_NAME;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_ZNODE_PARENT;
import static org.apache.hadoop.hbase.client.RegionInfo.DEFAULT_REPLICA_ID;

import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Class that hold all the paths of znode for HBase.
 */
@InterfaceAudience.Private
public class ZNodePaths {
  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  public static final char ZNODE_PATH_SEPARATOR = '/';

  public final static String META_ZNODE_PREFIX = "meta-region-server";

  // base znode for this cluster
  public final String baseZNode;
  // the prefix of meta znode, does not include baseZNode.
  public final String metaZNodePrefix;
  // znodes containing the locations of the servers hosting the meta replicas
  public final ImmutableMap<Integer, String> metaReplicaZNodes;
  // znode containing ephemeral nodes of the regionservers
  public final String rsZNode;
  // znode containing ephemeral nodes of the draining regionservers
  public final String drainingZNode;
  // znode of currently active master
  public final String masterAddressZNode;
  // znode of this master in backup master directory, if not the active master
  public final String backupMasterAddressesZNode;
  // znode containing the current cluster state
  public final String clusterStateZNode;
  // znode used for table disabling/enabling
  // Still used in hbase2 by MirroringTableStateManager; it mirrors internal table state out to
  // zookeeper for hbase1 clients to make use of. If no hbase1 clients disable. See
  // MirroringTableStateManager. To be removed in hbase3.
  @Deprecated
  public final String tableZNode;
  // znode containing the unique cluster ID
  public final String clusterIdZNode;
  // znode used for log splitting work assignment
  public final String splitLogZNode;
  // znode containing the state of the load balancer
  public final String balancerZNode;
  // znode containing the state of region normalizer
  public final String regionNormalizerZNode;
  // znode containing the state of all switches, currently there are split and merge child node.
  public final String switchZNode;
  // znode containing the lock for the tables
  public final String tableLockZNode;
  // znode containing namespace descriptors
  public final String namespaceZNode;
  // znode of indicating master maintenance mode
  public final String masterMaintZNode;

  // znode containing all replication state.
  public final String replicationZNode;
  // znode containing a list of all remote slave (i.e. peer) clusters.
  public final String peersZNode;
  // znode containing all replication queues
  public final String queuesZNode;
  // znode containing queues of hfile references to be replicated
  public final String hfileRefsZNode;

  public ZNodePaths(Configuration conf) {
    baseZNode = conf.get(ZOOKEEPER_ZNODE_PARENT, DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builder();
    metaZNodePrefix = conf.get("zookeeper.znode.metaserver", META_ZNODE_PREFIX);
    String defaultMetaReplicaZNode = ZNodePaths.joinZNode(baseZNode, metaZNodePrefix);
    builder.put(DEFAULT_REPLICA_ID, defaultMetaReplicaZNode);
    int numMetaReplicas = conf.getInt(META_REPLICAS_NUM, DEFAULT_META_REPLICA_NUM);
    IntStream.range(1, numMetaReplicas)
        .forEachOrdered(i -> builder.put(i, defaultMetaReplicaZNode + "-" + i));
    metaReplicaZNodes = builder.build();
    rsZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.rs", "rs"));
    drainingZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.draining.rs", "draining"));
    masterAddressZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.master", "master"));
    backupMasterAddressesZNode =
        joinZNode(baseZNode, conf.get("zookeeper.znode.backup.masters", "backup-masters"));
    clusterStateZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.state", "running"));
    tableZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.tableEnableDisable", "table"));
    clusterIdZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.clusterId", "hbaseid"));
    splitLogZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.splitlog", SPLIT_LOGDIR_NAME));
    balancerZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.balancer", "balancer"));
    regionNormalizerZNode =
        joinZNode(baseZNode, conf.get("zookeeper.znode.regionNormalizer", "normalizer"));
    switchZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.switch", "switch"));
    tableLockZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.tableLock", "table-lock"));
    namespaceZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.namespace", "namespace"));
    masterMaintZNode =
        joinZNode(baseZNode, conf.get("zookeeper.znode.masterMaintenance", "master-maintenance"));
    replicationZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.replication", "replication"));
    peersZNode =
        joinZNode(replicationZNode, conf.get("zookeeper.znode.replication.peers", "peers"));
    queuesZNode = joinZNode(replicationZNode, conf.get("zookeeper.znode.replication.rs", "rs"));
    hfileRefsZNode = joinZNode(replicationZNode,
      conf.get("zookeeper.znode.replication.hfile.refs", "hfile-refs"));
  }

  @Override
  public String toString() {
    return "ZNodePaths [baseZNode=" + baseZNode + ", metaReplicaZNodes=" + metaReplicaZNodes
        + ", rsZNode=" + rsZNode + ", drainingZNode=" + drainingZNode + ", masterAddressZNode="
        + masterAddressZNode + ", backupMasterAddressesZNode=" + backupMasterAddressesZNode
        + ", clusterStateZNode=" + clusterStateZNode + ", tableZNode=" + tableZNode
        + ", clusterIdZNode=" + clusterIdZNode + ", splitLogZNode=" + splitLogZNode
        + ", balancerZNode=" + balancerZNode + ", regionNormalizerZNode=" + regionNormalizerZNode
        + ", switchZNode=" + switchZNode + ", tableLockZNode=" + tableLockZNode
        + ", namespaceZNode=" + namespaceZNode + ", masterMaintZNode=" + masterMaintZNode
        + ", replicationZNode=" + replicationZNode + ", peersZNode=" + peersZNode
        + ", queuesZNode=" + queuesZNode + ", hfileRefsZNode=" + hfileRefsZNode + "]";
  }

  /**
   * Is the znode of any meta replica
   * @param node
   * @return true or false
   */
  public boolean isAnyMetaReplicaZNode(String node) {
    if (metaReplicaZNodes.containsValue(node)) {
      return true;
    }
    return false;
  }

  /**
   * Get the znode string corresponding to a replicaId
   * @param replicaId
   * @return znode
   */
  public String getZNodeForReplica(int replicaId) {
    // return a newly created path but don't update the cache of paths
    // This is mostly needed for tests that attempt to create meta replicas
    // from outside the master
    return Optional.ofNullable(metaReplicaZNodes.get(replicaId))
        .orElseGet(() -> metaReplicaZNodes.get(DEFAULT_REPLICA_ID) + "-" + replicaId);
  }

  /**
   * Parse the meta replicaId from the passed znode
   * @param znode
   * @return replicaId
   */
  public int getMetaReplicaIdFromZnode(String znode) {
    if (znode.equals(metaZNodePrefix)) {
      return RegionInfo.DEFAULT_REPLICA_ID;
    }
    return Integer.parseInt(znode.substring(metaZNodePrefix.length() + 1));
  }

  /**
   * Is it the default meta replica's znode
   * @param znode
   * @return true or false
   */
  public boolean isDefaultMetaReplicaZnode(String znode) {
    return metaReplicaZNodes.get(DEFAULT_REPLICA_ID).equals(znode);
  }

  /**
   * Returns whether the znode is supposed to be readable by the client and DOES NOT contain
   * sensitive information (world readable).
   */
  public boolean isClientReadable(String node) {
    // Developer notice: These znodes are world readable. DO NOT add more znodes here UNLESS
    // all clients need to access this data to work. Using zk for sharing data to clients (other
    // than service lookup case is not a recommended design pattern.
    return node.equals(baseZNode) || isAnyMetaReplicaZNode(node) ||
      node.equals(masterAddressZNode) || node.equals(clusterIdZNode) || node.equals(rsZNode) ||
      // /hbase/table and /hbase/table/foo is allowed, /hbase/table-lock is not
      node.equals(tableZNode) || node.startsWith(tableZNode + "/");
  }

  /**
   * Join the prefix znode name with the suffix znode name to generate a proper full znode name.
   * <p>
   * Assumes prefix does not end with slash and suffix does not begin with it.
   * @param prefix beginning of znode name
   * @param suffix ending of znode name
   * @return result of properly joining prefix with suffix
   */
  public static String joinZNode(String prefix, String suffix) {
    return prefix + ZNodePaths.ZNODE_PATH_SEPARATOR + suffix;
  }
}
