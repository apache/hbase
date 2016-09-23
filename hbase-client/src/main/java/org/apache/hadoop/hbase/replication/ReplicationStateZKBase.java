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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;


/**
 * This is a base class for maintaining replication state in zookeeper.
 */
@InterfaceAudience.Private
public abstract class ReplicationStateZKBase {

  /**
   * The name of the znode that contains the replication status of a remote slave (i.e. peer)
   * cluster.
   */
  protected final String peerStateNodeName;
  /** The name of the base znode that contains all replication state. */
  protected final String replicationZNode;
  /** The name of the znode that contains a list of all remote slave (i.e. peer) clusters. */
  protected final String peersZNode;
  /** The name of the znode that contains all replication queues */
  protected final String queuesZNode;
  /** The name of the znode that contains queues of hfile references to be replicated */
  protected final String hfileRefsZNode;
  /** The cluster key of the local cluster */
  protected final String ourClusterKey;
  /** The name of the znode that contains tableCFs */
  protected final String tableCFsNodeName;

  protected final ZooKeeperWatcher zookeeper;
  protected final Configuration conf;
  protected final Abortable abortable;

  // Public for testing
  public static final byte[] ENABLED_ZNODE_BYTES =
      toByteArray(ZooKeeperProtos.ReplicationState.State.ENABLED);
  public static final byte[] DISABLED_ZNODE_BYTES =
      toByteArray(ZooKeeperProtos.ReplicationState.State.DISABLED);
  public static final String ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_KEY =
      "zookeeper.znode.replication.hfile.refs";
  public static final String ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_DEFAULT = "hfile-refs";

  public ReplicationStateZKBase(ZooKeeperWatcher zookeeper, Configuration conf,
      Abortable abortable) {
    this.zookeeper = zookeeper;
    this.conf = conf;
    this.abortable = abortable;

    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    String peersZNodeName = conf.get("zookeeper.znode.replication.peers", "peers");
    String queuesZNodeName = conf.get("zookeeper.znode.replication.rs", "rs");
    String hfileRefsZNodeName = conf.get(ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_KEY,
      ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_DEFAULT);
    this.peerStateNodeName = conf.get("zookeeper.znode.replication.peers.state", "peer-state");
    this.tableCFsNodeName = conf.get("zookeeper.znode.replication.peers.tableCFs", "tableCFs");
    this.ourClusterKey = ZKConfig.getZooKeeperClusterKey(this.conf);
    this.replicationZNode = ZKUtil.joinZNode(this.zookeeper.znodePaths.baseZNode,
      replicationZNodeName);
    this.peersZNode = ZKUtil.joinZNode(replicationZNode, peersZNodeName);
    this.queuesZNode = ZKUtil.joinZNode(replicationZNode, queuesZNodeName);
    this.hfileRefsZNode = ZKUtil.joinZNode(replicationZNode, hfileRefsZNodeName);
  }

  public List<String> getListOfReplicators() {
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, this.queuesZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get list of replicators", e);
    }
    return result;
  }

  /**
   * @param state
   * @return Serialized protobuf of <code>state</code> with pb magic prefix prepended suitable for
   *         use as content of a peer-state znode under a peer cluster id as in
   *         /hbase/replication/peers/PEER_ID/peer-state.
   */
  protected static byte[] toByteArray(final ZooKeeperProtos.ReplicationState.State state) {
    ZooKeeperProtos.ReplicationState msg =
        ZooKeeperProtos.ReplicationState.newBuilder().setState(state).build();
    // There is no toByteArray on this pb Message?
    // 32 bytes is default which seems fair enough here.
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      CodedOutputStream cos = CodedOutputStream.newInstance(baos, 16);
      msg.writeTo(cos);
      cos.flush();
      baos.flush();
      return ProtobufUtil.prependPBMagic(baos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected boolean peerExists(String id) throws KeeperException {
    return ZKUtil.checkExists(this.zookeeper, ZKUtil.joinZNode(this.peersZNode, id)) >= 0;
  }

  /**
   * Determine if a ZK path points to a peer node.
   * @param path path to be checked
   * @return true if the path points to a peer node, otherwise false
   */
  protected boolean isPeerPath(String path) {
    return path.split("/").length == peersZNode.split("/").length + 1;
  }

  @VisibleForTesting
  protected String getTableCFsNode(String id) {
    return ZKUtil.joinZNode(this.peersZNode, ZKUtil.joinZNode(id, this.tableCFsNodeName));
  }

  @VisibleForTesting
  protected String getPeerStateNode(String id) {
    return ZKUtil.joinZNode(this.peersZNode, ZKUtil.joinZNode(id, this.peerStateNodeName));
  }
  @VisibleForTesting
  protected String getPeerNode(String id) {
    return ZKUtil.joinZNode(this.peersZNode, id);
  }
}
