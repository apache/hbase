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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This class acts as a wrapper for all the objects used to identify and
 * communicate with remote peers and is responsible for answering to expired
 * sessions and re-establishing the ZK connections.
 */
@InterfaceAudience.Private
public class ReplicationPeer implements Abortable, Closeable {
  private static final Log LOG = LogFactory.getLog(ReplicationPeer.class);

  private final String clusterKey;
  private final String id;
  private List<ServerName> regionServers = new ArrayList<ServerName>(0);
  private final AtomicBoolean peerEnabled = new AtomicBoolean();
  private volatile Map<String, List<String>> tableCFs = new HashMap<String, List<String>>();
  // Cannot be final since a new object needs to be recreated when session fails
  private ZooKeeperWatcher zkw;
  private final Configuration conf;
  private long lastRegionserverUpdate;

  private PeerStateTracker peerStateTracker;
  private TableCFsTracker tableCFsTracker;

  /**
   * Constructor that takes all the objects required to communicate with the
   * specified peer, except for the region server addresses.
   * @param conf configuration object to this peer
   * @param key cluster key used to locate the peer
   * @param id string representation of this peer's identifier
   */
  public ReplicationPeer(Configuration conf, String id) throws ReplicationException {
    this.conf = conf;
    this.clusterKey = ZKUtil.getZooKeeperClusterKey(conf);
    this.id = id;
    try {
      this.reloadZkWatcher();
    } catch (IOException e) {
      throw new ReplicationException("Error connecting to peer cluster with peerId=" + id, e);
    }
  }

  /**
   * start a state tracker to check whether this peer is enabled or not
   *
   * @param zookeeper zk watcher for the local cluster
   * @param peerStateNode path to zk node which stores peer state
   * @throws KeeperException
   */
  public void startStateTracker(ZooKeeperWatcher zookeeper, String peerStateNode)
      throws KeeperException {
    ensurePeerEnabled(zookeeper, peerStateNode);
    this.peerStateTracker = new PeerStateTracker(peerStateNode, zookeeper, this);
    this.peerStateTracker.start();
    try {
      this.readPeerStateZnode();
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
  }

  private void readPeerStateZnode() throws DeserializationException {
    this.peerEnabled.set(isStateEnabled(this.peerStateTracker.getData(false)));
  }

  /**
   * start a table-cfs tracker to listen the (table, cf-list) map change
   *
   * @param zookeeper zk watcher for the local cluster
   * @param tableCFsNode path to zk node which stores table-cfs
   * @throws KeeperException
   */
  public void startTableCFsTracker(ZooKeeperWatcher zookeeper, String tableCFsNode)
    throws KeeperException {
    this.tableCFsTracker = new TableCFsTracker(tableCFsNode, zookeeper,
        this);
    this.tableCFsTracker.start();
    this.readTableCFsZnode();
  }

  static Map<String, List<String>> parseTableCFsFromConfig(String tableCFsConfig) {
    if (tableCFsConfig == null || tableCFsConfig.trim().length() == 0) {
      return null;
    }

    Map<String, List<String>> tableCFsMap = null;

    // parse out (table, cf-list) pairs from tableCFsConfig
    // format: "table1:cf1,cf2;table2:cfA,cfB"
    String[] tables = tableCFsConfig.split(";");
    for (String tab : tables) {
      // 1 ignore empty table config
      tab = tab.trim();
      if (tab.length() == 0) {
        continue;
      }
      // 2 split to "table" and "cf1,cf2"
      //   for each table: "table:cf1,cf2" or "table"
      String[] pair = tab.split(":");
      String tabName = pair[0].trim();
      if (pair.length > 2 || tabName.length() == 0) {
        LOG.error("ignore invalid tableCFs setting: " + tab);
        continue;
      }

      // 3 parse "cf1,cf2" part to List<cf>
      List<String> cfs = null;
      if (pair.length == 2) {
        String[] cfsList = pair[1].split(",");
        for (String cf : cfsList) {
          String cfName = cf.trim();
          if (cfName.length() > 0) {
            if (cfs == null) {
              cfs = new ArrayList<String>();
            }
            cfs.add(cfName);
          }
        }
      }

      // 4 put <table, List<cf>> to map
      if (tableCFsMap == null) {
        tableCFsMap = new HashMap<String, List<String>>();
      }
      tableCFsMap.put(tabName, cfs);
    }

    return tableCFsMap;
  }

  private void readTableCFsZnode() {
    String currentTableCFs = Bytes.toString(tableCFsTracker.getData(false));
    this.tableCFs = parseTableCFsFromConfig(currentTableCFs);
  }

  /**
   * Get the cluster key of that peer
   * @return string consisting of zk ensemble addresses, client port
   * and root znode
   */
  public String getClusterKey() {
    return clusterKey;
  }

  /**
   * Get the state of this peer
   * @return atomic boolean that holds the status
   */
  public AtomicBoolean getPeerEnabled() {
    return peerEnabled;
  }

  /**
   * Get replicable (table, cf-list) map of this peer
   * @return the replicable (table, cf-list) map
   */
  public Map<String, List<String>> getTableCFs() {
    return this.tableCFs;
  }

  /**
   * Get a list of all the addresses of all the region servers
   * for this peer cluster
   * @return list of addresses
   */
  public List<ServerName> getRegionServers() {
    return regionServers;
  }

  /**
   * Set the list of region servers for that peer
   * @param regionServers list of addresses for the region servers
   */
  public void setRegionServers(List<ServerName> regionServers) {
    this.regionServers = regionServers;
    lastRegionserverUpdate = System.currentTimeMillis();
  }

  /**
   * Get the ZK connection to this peer
   * @return zk connection
   */
  public ZooKeeperWatcher getZkw() {
    return zkw;
  }

  /**
   * Get the timestamp at which the last change occurred to the list of region servers to replicate
   * to.
   * @return The System.currentTimeMillis at the last time the list of peer region servers changed.
   */
  public long getLastRegionserverUpdate() {
    return lastRegionserverUpdate;
  }

  /**
   * Get the identifier of this peer
   * @return string representation of the id (short)
   */
  public String getId() {
    return id;
  }

  /**
   * Get the configuration object required to communicate with this peer
   * @return configuration object
   */
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.fatal("The ReplicationPeer coresponding to peer " + clusterKey
        + " was aborted for the following reason(s):" + why, e);
  }

  /**
   * Closes the current ZKW (if not null) and creates a new one
   * @throws IOException If anything goes wrong connecting
   */
  public void reloadZkWatcher() throws IOException {
    if (zkw != null) zkw.close();
    zkw = new ZooKeeperWatcher(conf,
        "connection to cluster: " + id, this);
  }

  @Override
  public boolean isAborted() {
    // Currently the replication peer is never "Aborted", we just log when the
    // abort method is called.
    return false;
  }

  @Override
  public void close() throws IOException {
    if (zkw != null){
      zkw.close();
    }
  }

  /**
   * Parse the raw data from ZK to get a peer's state
   * @param bytes raw ZK data
   * @return True if the passed in <code>bytes</code> are those of a pb serialized ENABLED state.
   * @throws DeserializationException
   */
  public static boolean isStateEnabled(final byte[] bytes) throws DeserializationException {
    ZooKeeperProtos.ReplicationState.State state = parseStateFrom(bytes);
    return ZooKeeperProtos.ReplicationState.State.ENABLED == state;
  }

  /**
   * @param bytes Content of a state znode.
   * @return State parsed from the passed bytes.
   * @throws DeserializationException
   */
  private static ZooKeeperProtos.ReplicationState.State parseStateFrom(final byte[] bytes)
      throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(bytes);
    int pblen = ProtobufUtil.lengthOfPBMagic();
    ZooKeeperProtos.ReplicationState.Builder builder =
        ZooKeeperProtos.ReplicationState.newBuilder();
    ZooKeeperProtos.ReplicationState state;
    try {
      state = builder.mergeFrom(bytes, pblen, bytes.length - pblen).build();
      return state.getState();
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * Utility method to ensure an ENABLED znode is in place; if not present, we create it.
   * @param zookeeper
   * @param path Path to znode to check
   * @return True if we created the znode.
   * @throws NodeExistsException
   * @throws KeeperException
   */
  private static boolean ensurePeerEnabled(final ZooKeeperWatcher zookeeper, final String path)
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
   * Tracker for (table, cf-list) map of this peer
   */
  public class TableCFsTracker extends ZooKeeperNodeTracker {

    public TableCFsTracker(String tableCFsZNode, ZooKeeperWatcher watcher,
        Abortable abortable) {
      super(watcher, tableCFsZNode, abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      if (path.equals(node)) {
        super.nodeDataChanged(path);
        readTableCFsZnode();
      }
    }
  }
}
