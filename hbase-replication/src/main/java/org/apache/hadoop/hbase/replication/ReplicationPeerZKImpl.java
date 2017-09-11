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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

@InterfaceAudience.Private
public class ReplicationPeerZKImpl extends ReplicationStateZKBase
    implements ReplicationPeer, Abortable, Closeable {
  private static final Log LOG = LogFactory.getLog(ReplicationPeerZKImpl.class);

  private ReplicationPeerConfig peerConfig;
  private final String id;
  private volatile PeerState peerState;
  private volatile Map<TableName, List<String>> tableCFs = new HashMap<>();
  private final Configuration conf;
  private PeerStateTracker peerStateTracker;
  private PeerConfigTracker peerConfigTracker;


  /**
   * Constructor that takes all the objects required to communicate with the specified peer, except
   * for the region server addresses.
   * @param conf configuration object to this peer
   * @param id string representation of this peer's identifier
   * @param peerConfig configuration for the replication peer
   */
  public ReplicationPeerZKImpl(ZooKeeperWatcher zkWatcher, Configuration conf,
                               String id, ReplicationPeerConfig peerConfig,
                               Abortable abortable)
      throws ReplicationException {
    super(zkWatcher, conf, abortable);
    this.conf = conf;
    this.peerConfig = peerConfig;
    this.id = id;
  }

  /**
   * start a state tracker to check whether this peer is enabled or not
   *
   * @param peerStateNode path to zk node which stores peer state
   * @throws KeeperException
   */
  public void startStateTracker(String peerStateNode)
      throws KeeperException {
    ensurePeerEnabled(peerStateNode);
    this.peerStateTracker = new PeerStateTracker(peerStateNode, zookeeper, this);
    this.peerStateTracker.start();
    try {
      this.readPeerStateZnode();
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
  }

  private void readPeerStateZnode() throws DeserializationException {
    this.peerState =
        isStateEnabled(this.peerStateTracker.getData(false))
          ? PeerState.ENABLED
          : PeerState.DISABLED;
  }

  /**
   * start a table-cfs tracker to listen the (table, cf-list) map change
   * @param peerConfigNode path to zk node which stores table-cfs
   * @throws KeeperException
   */
  public void startPeerConfigTracker(String peerConfigNode)
    throws KeeperException {
    this.peerConfigTracker = new PeerConfigTracker(peerConfigNode, zookeeper,
        this);
    this.peerConfigTracker.start();
    this.readPeerConfig();
  }

  private ReplicationPeerConfig readPeerConfig() {
    try {
      byte[] data = peerConfigTracker.getData(false);
      if (data != null) {
        this.peerConfig = ReplicationSerDeHelper.parsePeerFrom(data);
      }
    } catch (DeserializationException e) {
      LOG.error("", e);
    }
    return this.peerConfig;
  }

  @Override
  public PeerState getPeerState() {
    return peerState;
  }

  /**
   * Get the identifier of this peer
   * @return string representation of the id (short)
   */
  @Override
  public String getId() {
    return id;
  }

  /**
   * Get the peer config object
   * @return the ReplicationPeerConfig for this peer
   */
  @Override
  public ReplicationPeerConfig getPeerConfig() {
    return peerConfig;
  }

  /**
   * Get the configuration object required to communicate with this peer
   * @return configuration object
   */
  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Get replicable (table, cf-list) map of this peer
   * @return the replicable (table, cf-list) map
   */
  @Override
  public Map<TableName, List<String>> getTableCFs() {
    this.tableCFs = peerConfig.getTableCFsMap();
    return this.tableCFs;
  }

  /**
   * Get replicable namespace set of this peer
   * @return the replicable namespaces set
   */
  @Override
  public Set<String> getNamespaces() {
    return this.peerConfig.getNamespaces();
  }

  @Override
  public long getPeerBandwidth() {
    return this.peerConfig.getBandwidth();
  }

  @Override
  public void trackPeerConfigChanges(ReplicationPeerConfigListener listener) {
    if (this.peerConfigTracker != null){
      this.peerConfigTracker.setListener(listener);
    }
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.fatal("The ReplicationPeer corresponding to peer " + peerConfig
        + " was aborted for the following reason(s):" + why, e);
  }

  @Override
  public boolean isAborted() {
    // Currently the replication peer is never "Aborted", we just log when the
    // abort method is called.
    return false;
  }

  @Override
  public void close() throws IOException {
    // TODO: stop zkw?
  }

  /**
   * Parse the raw data from ZK to get a peer's state
   * @param bytes raw ZK data
   * @return True if the passed in <code>bytes</code> are those of a pb serialized ENABLED state.
   * @throws DeserializationException
   */
  public static boolean isStateEnabled(final byte[] bytes) throws DeserializationException {
    ReplicationProtos.ReplicationState.State state = parseStateFrom(bytes);
    return ReplicationProtos.ReplicationState.State.ENABLED == state;
  }

  /**
   * @param bytes Content of a state znode.
   * @return State parsed from the passed bytes.
   * @throws DeserializationException
   */
  private static ReplicationProtos.ReplicationState.State parseStateFrom(final byte[] bytes)
      throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(bytes);
    int pblen = ProtobufUtil.lengthOfPBMagic();
    ReplicationProtos.ReplicationState.Builder builder =
        ReplicationProtos.ReplicationState.newBuilder();
    ReplicationProtos.ReplicationState state;
    try {
      ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
      state = builder.build();
      return state.getState();
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * Utility method to ensure an ENABLED znode is in place; if not present, we create it.
   * @param path Path to znode to check
   * @return True if we created the znode.
   * @throws NodeExistsException
   * @throws KeeperException
   */
  private boolean ensurePeerEnabled(final String path)
      throws NodeExistsException, KeeperException {
    if (ZKUtil.checkExists(zookeeper, path) == -1) {
      // There is a race b/w PeerWatcher and ReplicationZookeeper#add method to create the
      // peer-state znode. This happens while adding a peer.
      // The peer state data is set as "ENABLED" by default.
      ZKUtil.createNodeIfNotExistsAndWatch(zookeeper, path,
        ReplicationStateZKBase.ENABLED_ZNODE_BYTES);
      return true;
    }
    return false;
  }

  /**
   * Tracker for state of this peer
   */
  public class PeerStateTracker extends ZooKeeperNodeTracker {

    public PeerStateTracker(String peerStateZNode, ZooKeeperWatcher watcher,
        Abortable abortable) {
      super(watcher, peerStateZNode, abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      if (path.equals(node)) {
        super.nodeDataChanged(path);
        try {
          readPeerStateZnode();
        } catch (DeserializationException e) {
          LOG.warn("Failed deserializing the content of " + path, e);
        }
      }
    }
  }

  /**
   * Tracker for PeerConfigNode of this peer
   */
  public class PeerConfigTracker extends ZooKeeperNodeTracker {

    ReplicationPeerConfigListener listener;

    public PeerConfigTracker(String peerConfigNode, ZooKeeperWatcher watcher,
        Abortable abortable) {
      super(watcher, peerConfigNode, abortable);
    }

    public synchronized void setListener(ReplicationPeerConfigListener listener){
      this.listener = listener;
    }

    @Override
    public synchronized void nodeCreated(String path) {
      if (path.equals(node)) {
        super.nodeCreated(path);
        ReplicationPeerConfig config = readPeerConfig();
        if (listener != null){
          listener.peerConfigUpdated(config);
        }
      }
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      //superclass calls nodeCreated
      if (path.equals(node)) {
        super.nodeDataChanged(path);
      }

    }

  }
}
