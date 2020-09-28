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
package org.apache.hadoop.hbase.zookeeper;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;
import static org.apache.hadoop.hbase.HConstants.SPLIT_LOGDIR_NAME;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_ZNODE_PARENT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class that hold all the paths of znode for HBase.
 */
@InterfaceAudience.Private
public class ZNodePaths {
  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  public static final char ZNODE_PATH_SEPARATOR = '/';

  public static final String META_ZNODE_PREFIX_CONF_KEY = "zookeeper.znode.metaserver";
  public static final String META_ZNODE_PREFIX = "meta-region-server";
  private static final String DEFAULT_SNAPSHOT_CLEANUP_ZNODE = "snapshot-cleanup";

  // base znode for this cluster
  public final String baseZNode;

  /**
   * The prefix of meta znode. Does not include baseZNode.
   * Its a 'prefix' because meta replica id integer can be tagged on the end (if
   * no number present, it is 'default' replica).
   */
  private final String metaZNodePrefix;

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
  // znode containing the state of the snapshot auto-cleanup
  final String snapshotCleanupZNode;

  public ZNodePaths(Configuration conf) {
    baseZNode = conf.get(ZOOKEEPER_ZNODE_PARENT, DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    metaZNodePrefix = conf.get(META_ZNODE_PREFIX_CONF_KEY, META_ZNODE_PREFIX);
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
    namespaceZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.namespace", "namespace"));
    masterMaintZNode =
        joinZNode(baseZNode, conf.get("zookeeper.znode.masterMaintenance", "master-maintenance"));
    replicationZNode = joinZNode(baseZNode, conf.get("zookeeper.znode.replication", "replication"));
    peersZNode =
        joinZNode(replicationZNode, conf.get("zookeeper.znode.replication.peers", "peers"));
    queuesZNode = joinZNode(replicationZNode, conf.get("zookeeper.znode.replication.rs", "rs"));
    hfileRefsZNode = joinZNode(replicationZNode,
      conf.get("zookeeper.znode.replication.hfile.refs", "hfile-refs"));
    snapshotCleanupZNode = joinZNode(baseZNode,
        conf.get("zookeeper.znode.snapshot.cleanup", DEFAULT_SNAPSHOT_CLEANUP_ZNODE));
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("ZNodePaths [baseZNode=").append(baseZNode)
        .append(", rsZNode=").append(rsZNode)
        .append(", drainingZNode=").append(drainingZNode)
        .append(", masterAddressZNode=").append(masterAddressZNode)
        .append(", backupMasterAddressesZNode=").append(backupMasterAddressesZNode)
        .append(", clusterStateZNode=").append(clusterStateZNode)
        .append(", tableZNode=").append(tableZNode)
        .append(", clusterIdZNode=").append(clusterIdZNode)
        .append(", splitLogZNode=").append(splitLogZNode)
        .append(", balancerZNode=").append(balancerZNode)
        .append(", regionNormalizerZNode=").append(regionNormalizerZNode)
        .append(", switchZNode=").append(switchZNode)
        .append(", namespaceZNode=").append(namespaceZNode)
        .append(", masterMaintZNode=").append(masterMaintZNode)
        .append(", replicationZNode=").append(replicationZNode)
        .append(", peersZNode=").append(peersZNode)
        .append(", queuesZNode=").append(queuesZNode)
        .append(", hfileRefsZNode=").append(hfileRefsZNode)
        .append(", snapshotCleanupZNode=").append(snapshotCleanupZNode)
        .append("]").toString();
  }

  /**
   * @return the znode string corresponding to a replicaId
   */
  public String getZNodeForReplica(int replicaId) {
    if (RegionReplicaUtil.isDefaultReplica(replicaId)) {
      return joinZNode(baseZNode, metaZNodePrefix);
    } else {
      return joinZNode(baseZNode, metaZNodePrefix + "-" + replicaId);
    }
  }

  /**
   * Parses the meta replicaId from the passed path.
   * @param path the name of the full path which includes baseZNode.
   * @return replicaId
   */
  public int getMetaReplicaIdFromPath(String path) {
    // Extract the znode from path. The prefix is of the following format.
    // baseZNode + PATH_SEPARATOR.
    int prefixLen = baseZNode.length() + 1;
    return getMetaReplicaIdFromZNode(path.substring(prefixLen));
  }

  /**
   * Parse the meta replicaId from the passed znode
   * @param znode the name of the znode, does not include baseZNode
   * @return replicaId
   */
  public int getMetaReplicaIdFromZNode(String znode) {
    return znode.equals(metaZNodePrefix)?
        RegionInfo.DEFAULT_REPLICA_ID:
        Integer.parseInt(znode.substring(metaZNodePrefix.length() + 1));
  }

  /**
   * @return True if meta znode.
   */
  public boolean isMetaZNodePrefix(String znode) {
    return znode != null && znode.startsWith(this.metaZNodePrefix);
  }

  /**
   * @return True is the fully qualified path is for meta location
   */
  public boolean isMetaZNodePath(String path) {
    int prefixLen = baseZNode.length() + 1;
    return path.length() > prefixLen && isMetaZNodePrefix(path.substring(prefixLen));
  }

  /**
   * Returns whether the path is supposed to be readable by the client and DOES NOT contain
   * sensitive information (world readable).
   */
  public boolean isClientReadable(String path) {
    // Developer notice: These znodes are world readable. DO NOT add more znodes here UNLESS
    // all clients need to access this data to work. Using zk for sharing data to clients (other
    // than service lookup case is not a recommended design pattern.
    return path.equals(baseZNode) || isMetaZNodePath(path) || path.equals(masterAddressZNode) ||
      path.equals(clusterIdZNode) || path.equals(rsZNode) ||
      // /hbase/table and /hbase/table/foo is allowed, /hbase/table-lock is not
      path.equals(tableZNode) || path.startsWith(tableZNode + "/");
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
