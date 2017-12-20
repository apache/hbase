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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

@InterfaceAudience.Private
public class ReplicationPeerZKImpl extends ReplicationStateZKBase
    implements ReplicationPeer, Abortable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationPeerZKImpl.class);

  private volatile ReplicationPeerConfig peerConfig;
  private final String id;
  private volatile PeerState peerState;
  private volatile Map<TableName, List<String>> tableCFs = new HashMap<>();
  private final Configuration conf;

  private final List<ReplicationPeerConfigListener> peerConfigListeners;

  /**
   * Constructor that takes all the objects required to communicate with the specified peer, except
   * for the region server addresses.
   * @param conf configuration object to this peer
   * @param id string representation of this peer's identifier
   * @param peerConfig configuration for the replication peer
   */
  public ReplicationPeerZKImpl(ZKWatcher zkWatcher, Configuration conf, String id,
      ReplicationPeerConfig peerConfig, Abortable abortable) throws ReplicationException {
    super(zkWatcher, conf, abortable);
    this.conf = conf;
    this.peerConfig = peerConfig;
    this.id = id;
    this.peerConfigListeners = new ArrayList<>();
  }

  private PeerState readPeerState() throws ReplicationException {
    try {
      byte[] data = ZKUtil.getData(zookeeper, this.getPeerStateNode(id));
      this.peerState = isStateEnabled(data) ? PeerState.ENABLED : PeerState.DISABLED;
    } catch (DeserializationException | KeeperException | InterruptedException e) {
      throw new ReplicationException("Get and deserialize peer state data from zookeeper failed: ",
          e);
    }
    return this.peerState;
  }

  private ReplicationPeerConfig readPeerConfig() throws ReplicationException {
    try {
      byte[] data = ZKUtil.getData(zookeeper, this.getPeerNode(id));
      if (data != null) {
        this.peerConfig = ReplicationPeerConfigUtil.parsePeerFrom(data);
      }
    } catch (DeserializationException | KeeperException | InterruptedException e) {
      throw new ReplicationException("Get and deserialize peer config date from zookeeper failed: ",
          e);
    }
    return this.peerConfig;
  }

  @Override
  public PeerState getPeerState() {
    return peerState;
  }

  @Override
  public PeerState getPeerState(boolean loadFromBackingStore) throws ReplicationException {
    if (loadFromBackingStore) {
      return readPeerState();
    } else {
      return peerState;
    }
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

  @Override
  public ReplicationPeerConfig getPeerConfig(boolean loadFromBackingStore)
      throws ReplicationException {
    if (loadFromBackingStore) {
      return readPeerConfig();
    } else {
      return peerConfig;
    }
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
  public void registerPeerConfigListener(ReplicationPeerConfigListener listener) {
    this.peerConfigListeners.add(listener);
  }

  @Override
  public void triggerPeerConfigChange(ReplicationPeerConfig newPeerConfig) {
    for (ReplicationPeerConfigListener listener : this.peerConfigListeners) {
      listener.peerConfigUpdated(newPeerConfig);
    }
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.error(HBaseMarkers.FATAL, "The ReplicationPeer corresponding to peer " +
        peerConfig + " was aborted for the following reason(s):" + why, e);
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
    int pbLen = ProtobufUtil.lengthOfPBMagic();
    ReplicationProtos.ReplicationState.Builder builder =
        ReplicationProtos.ReplicationState.newBuilder();
    ReplicationProtos.ReplicationState state;
    try {
      ProtobufUtil.mergeFrom(builder, bytes, pbLen, bytes.length - pbLen);
      state = builder.build();
      return state.getState();
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
  }
}
