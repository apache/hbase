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
import static org.apache.hadoop.hbase.HRegionInfo.DEFAULT_REPLICA_ID;

import com.google.common.collect.ImmutableMap;

import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Class that hold all the paths of znode for HBase.
 */
@InterfaceAudience.Private
public class ZNodePaths {

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
  // znode containing the state of recovering regions
  public final String recoveringRegionsZNode;
  // znode containing namespace descriptors
  public final String namespaceZNode;
  // znode of indicating master maintenance mode
  public final String masterMaintZNode;

  public ZNodePaths(Configuration conf) {
    baseZNode = conf.get(ZOOKEEPER_ZNODE_PARENT, DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builder();
    metaZNodePrefix = conf.get("zookeeper.znode.metaserver", META_ZNODE_PREFIX);
    String defaultMetaReplicaZNode = ZKUtil.joinZNode(baseZNode, metaZNodePrefix);
    builder.put(DEFAULT_REPLICA_ID, defaultMetaReplicaZNode);
    int numMetaReplicas = conf.getInt(META_REPLICAS_NUM, DEFAULT_META_REPLICA_NUM);
    IntStream.range(1, numMetaReplicas)
        .forEachOrdered(i -> builder.put(i, defaultMetaReplicaZNode + "-" + i));
    metaReplicaZNodes = builder.build();
    rsZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.rs", "rs"));
    drainingZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.draining.rs", "draining"));
    masterAddressZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.master", "master"));
    backupMasterAddressesZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.backup.masters", "backup-masters"));
    clusterStateZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.state", "running"));
    tableZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.tableEnableDisable", "table"));
    clusterIdZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.clusterId", "hbaseid"));
    splitLogZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.splitlog", SPLIT_LOGDIR_NAME));
    balancerZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.balancer", "balancer"));
    regionNormalizerZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.regionNormalizer", "normalizer"));
    switchZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.switch", "switch"));
    tableLockZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.tableLock", "table-lock"));
    recoveringRegionsZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.recovering.regions", "recovering-regions"));
    namespaceZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.namespace", "namespace"));
    masterMaintZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.masterMaintenance", "master-maintenance"));
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
        + ", recoveringRegionsZNode=" + recoveringRegionsZNode + ", namespaceZNode="
        + namespaceZNode + ", masterMaintZNode=" + masterMaintZNode + "]";
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
      return HRegionInfo.DEFAULT_REPLICA_ID;
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
}
