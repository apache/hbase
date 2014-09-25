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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;


/**
 * This is a base class for maintaining replication state in zookeeper.
 */
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
  /** The cluster key of the local cluster */
  protected final String ourClusterKey;
  protected final ZooKeeperWatcher zookeeper;
  protected final Configuration conf;
  protected final Abortable abortable;

  // Public for testing
  public static final byte[] ENABLED_ZNODE_BYTES =
      toByteArray(ZooKeeperProtos.ReplicationState.State.ENABLED);
  public static final byte[] DISABLED_ZNODE_BYTES =
      toByteArray(ZooKeeperProtos.ReplicationState.State.DISABLED);

  public ReplicationStateZKBase(ZooKeeperWatcher zookeeper, Configuration conf,
      Abortable abortable) {
    this.zookeeper = zookeeper;
    this.conf = conf;
    this.abortable = abortable;

    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    String peersZNodeName = conf.get("zookeeper.znode.replication.peers", "peers");
    String queuesZNodeName = conf.get("zookeeper.znode.replication.rs", "rs");
    this.peerStateNodeName = conf.get("zookeeper.znode.replication.peers.state", "peer-state");
    this.ourClusterKey = ZKUtil.getZooKeeperClusterKey(this.conf);
    this.replicationZNode = ZKUtil.joinZNode(this.zookeeper.baseZNode, replicationZNodeName);
    this.peersZNode = ZKUtil.joinZNode(replicationZNode, peersZNodeName);
    this.queuesZNode = ZKUtil.joinZNode(replicationZNode, queuesZNodeName);
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
    byte[] bytes =
        ZooKeeperProtos.ReplicationState.newBuilder().setState(state).build().toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
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
}
