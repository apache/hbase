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
package org.apache.hadoop.hbase.client.replication;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.Integer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;

/**
 * <p>
 * This class provides the administrative interface to HBase cluster
 * replication. In order to use it, the cluster and the client using
 * ReplicationAdmin must be configured with <code>hbase.replication</code>
 * set to true.
 * </p>
 * <p>
 * Adding a new peer results in creating new outbound connections from every
 * region server to a subset of region servers on the slave cluster. Each
 * new stream of replication will start replicating from the beginning of the
 * current HLog, meaning that edits from that past will be replicated.
 * </p>
 * <p>
 * Removing a peer is a destructive and irreversible operation that stops
 * all the replication streams for the given cluster and deletes the metadata
 * used to keep track of the replication state.
 * </p>
 * <p>
 * Enabling and disabling peers is currently not supported.
 * </p>
 * <p>
 * As cluster replication is still experimental, a kill switch is provided
 * in order to stop all replication-related operations, see
 * {@link #setReplicating(boolean)}. When setting it back to true, the new
 * state of all the replication streams will be unknown and may have holes.
 * Use at your own risk.
 * </p>
 * <p>
 * To see which commands are available in the shell, type
 * <code>replication</code>.
 * </p>
 */
public class ReplicationAdmin implements Closeable {

  public static final String TNAME = "tableName";
  public static final String CFNAME = "columnFamlyName";

  // only Global for now, can add other type
  // such as, 1) no global replication, or 2) the table is replicated to this cluster, etc.
  public static final String REPLICATIONTYPE = "replicationType";
  public static final String REPLICATIONGLOBAL = Integer
      .toString(HConstants.REPLICATION_SCOPE_GLOBAL);
      
  private final ReplicationZookeeper replicationZk;
  private final HConnection connection;

  /**
   * Constructor that creates a connection to the local ZooKeeper ensemble.
   * @param conf Configuration to use
   * @throws IOException if the connection to ZK cannot be made
   * @throws RuntimeException if replication isn't enabled.
   */
  public ReplicationAdmin(Configuration conf) throws IOException {
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false)) {
      throw new RuntimeException("hbase.replication isn't true, please " +
          "enable it in order to use replication");
    }
    this.connection = HConnectionManager.getConnection(conf);
    ZooKeeperWatcher zkw = this.connection.getZooKeeperWatcher();
    try {
      this.replicationZk = new ReplicationZookeeper(this.connection, conf, zkw);
    } catch (Exception exception) {
      if (connection != null) {
        connection.close();
      }
      if (exception instanceof IOException) {
        throw (IOException) exception;
      } else if (exception instanceof RuntimeException) {
        throw (RuntimeException) exception;
      } else {
        throw new IOException("Unable setup the ZooKeeper connection", exception);
      }
    }
  }

  /**
   * Add a new peer cluster to replicate to.
   * @param id a short that identifies the cluster
   * @param clusterKey the concatenation of the slave cluster's
   * <code>hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent</code>
   * @throws IllegalStateException if there's already one slave since
   * multi-slave isn't supported yet.
   */
  public void addPeer(String id, String clusterKey) throws IOException {
    this.replicationZk.addPeer(id, clusterKey);
  }

  /**
   * Removes a peer cluster and stops the replication to it.
   * @param id a short that identifies the cluster
   */
  public void removePeer(String id) throws IOException {
    this.replicationZk.removePeer(id);
  }

  /**
   * Restart the replication stream to the specified peer.
   * @param id a short that identifies the cluster
   */
  public void enablePeer(String id) throws IOException {
    this.replicationZk.enablePeer(id);
  }

  /**
   * Stop the replication stream to the specified peer.
   * @param id a short that identifies the cluster
   */
  public void disablePeer(String id) throws IOException {
    this.replicationZk.disablePeer(id);
  }

  /**
   * Get the number of slave clusters the local cluster has.
   * @return number of slave clusters
   */
  public int getPeersCount() {
    return this.replicationZk.listPeersIdsAndWatch().size();
  }

  /**
   * Map of this cluster's peers for display.
   * @return A map of peer ids to peer cluster keys
   */
  public Map<String, String> listPeers() {
    return this.replicationZk.listPeers();
  }

  /**
   * Get state of the peer
   *
   * @param id peer's identifier
   * @return current state of the peer
   */
  public String getPeerState(String id) throws IOException {
    try {
      return this.replicationZk.getPeerState(id).name();
    } catch (KeeperException e) {
      throw new IOException("Couldn't get the state of the peer " + id, e);
    }
  }

  /**
   * Get the current status of the kill switch, if the cluster is replicating
   * or not.
   * @return true if the cluster is replicated, otherwise false
   */
  public boolean getReplicating() throws IOException {
    try {
      return this.replicationZk.getReplication();
    } catch (KeeperException e) {
      throw new IOException("Couldn't get the replication status");
    }
  }

  /**
   * Kill switch for all replication-related features
   * @param newState true to start replication, false to stop it.
   * completely
   * @return the previous state
   */
  public boolean setReplicating(boolean newState) throws IOException {
    boolean prev = true;
    try {
      prev = getReplicating();
      this.replicationZk.setReplicating(newState);
    } catch (KeeperException e) {
      throw new IOException("Unable to set the replication state", e);
    }
    return prev;
  }

  /**
   * Get the ZK-support tool created and used by this object for replication.
   * @return the ZK-support tool
   */
  ReplicationZookeeper getReplicationZk() {
    return replicationZk;
  }

  @Override
  public void close() throws IOException {
    if (this.connection != null) {
      this.connection.close();
    }
  }
  
  /**
   * Find all column families that are replicated from this cluster
   * @return the full list of the replicated column families of this cluster as:
   *        tableName, family name, replicationType
   *
   * Currently replicationType is Global. In the future, more replication
   * types may be extended here. For example
   *  1) the replication may only apply to selected peers instead of all peers
   *  2) the replicationType may indicate the host Cluster servers as Slave
   *     for the table:columnFam.         
   */
  public List<HashMap<String, String>> listReplicated() throws IOException {
    List<HashMap<String, String>> replicationColFams = new ArrayList<HashMap<String, String>>();
    HTableDescriptor[] tables = this.connection.listTables();
  
    for (HTableDescriptor table : tables) {
      HColumnDescriptor[] columns = table.getColumnFamilies();
      String tableName = table.getNameAsString();
      for (HColumnDescriptor column : columns) {
        if (column.getScope() != HConstants.REPLICATION_SCOPE_LOCAL) {
          // At this moment, the columfam is replicated to all peers
          HashMap<String, String> replicationEntry = new HashMap<String, String>();
          replicationEntry.put(TNAME, tableName);
          replicationEntry.put(CFNAME, column.getNameAsString());
          replicationEntry.put(REPLICATIONTYPE, REPLICATIONGLOBAL);
          replicationColFams.add(replicationEntry);
        }
      }
    }
 
    return replicationColFams;
  } 
}
