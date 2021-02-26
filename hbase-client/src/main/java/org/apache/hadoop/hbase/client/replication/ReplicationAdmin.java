/**
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
package org.apache.hadoop.hbase.client.replication;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This class provides the administrative interface to HBase cluster
 * replication.
 * </p>
 * <p>
 * Adding a new peer results in creating new outbound connections from every
 * region server to a subset of region servers on the slave cluster. Each
 * new stream of replication will start replicating from the beginning of the
 * current WAL, meaning that edits from that past will be replicated.
 * </p>
 * <p>
 * Removing a peer is a destructive and irreversible operation that stops
 * all the replication streams for the given cluster and deletes the metadata
 * used to keep track of the replication state.
 * </p>
 * <p>
 * To see which commands are available in the shell, type
 * <code>replication</code>.
 * </p>
 *
 * @deprecated use {@link org.apache.hadoop.hbase.client.Admin} instead.
 */
@InterfaceAudience.Public
@Deprecated
public class ReplicationAdmin implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationAdmin.class);

  public static final String TNAME = "tableName";
  public static final String CFNAME = "columnFamilyName";

  // only Global for now, can add other type
  // such as, 1) no global replication, or 2) the table is replicated to this cluster, etc.
  public static final String REPLICATIONTYPE = "replicationType";
  public static final String REPLICATIONGLOBAL = Integer
      .toString(HConstants.REPLICATION_SCOPE_GLOBAL);

  private final Connection connection;
  private Admin admin;

  /**
   * Constructor that creates a connection to the local ZooKeeper ensemble.
   * @param conf Configuration to use
   * @throws IOException if an internal replication error occurs
   * @throws RuntimeException if replication isn't enabled.
   */
  public ReplicationAdmin(Configuration conf) throws IOException {
    this.connection = ConnectionFactory.createConnection(conf);
    admin = connection.getAdmin();
  }

  /**
   * Add a new remote slave cluster for replication.
   * @param id a short name that identifies the cluster
   * @param peerConfig configuration for the replication slave cluster
   * @param tableCfs the table and column-family list which will be replicated for this peer.
   * A map from tableName to column family names. An empty collection can be passed
   * to indicate replicating all column families. Pass null for replicating all table and column
   * families
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0,
   * use {@link #addPeer(String, ReplicationPeerConfig)} instead.
   */
  @Deprecated
  public void addPeer(String id, ReplicationPeerConfig peerConfig,
      Map<TableName, ? extends Collection<String>> tableCfs) throws ReplicationException,
      IOException {
    if (tableCfs != null) {
      peerConfig.setTableCFsMap(tableCfs);
    }
    this.admin.addReplicationPeer(id, peerConfig);
  }

  /**
   * Add a new remote slave cluster for replication.
   * @param id a short name that identifies the cluster
   * @param peerConfig configuration for the replication slave cluster
   * @deprecated use
   *             {@link org.apache.hadoop.hbase.client.Admin#addReplicationPeer(String, ReplicationPeerConfig)}
   *             instead
   */
  @Deprecated
  public void addPeer(String id, ReplicationPeerConfig peerConfig) throws ReplicationException,
      IOException {
    this.admin.addReplicationPeer(id, peerConfig);
  }

  /**
   *  @deprecated as release of 2.0.0, and it will be removed in 3.0.0
   * */
  @Deprecated
  public static Map<TableName, List<String>> parseTableCFsFromConfig(String tableCFsConfig) {
    return ReplicationPeerConfigUtil.parseTableCFsFromConfig(tableCFsConfig);
  }

  /**
   * @deprecated use
   *             {@link org.apache.hadoop.hbase.client.Admin#updateReplicationPeerConfig(String, ReplicationPeerConfig)}
   *             instead
   */
  @Deprecated
  public void updatePeerConfig(String id, ReplicationPeerConfig peerConfig) throws IOException {
    this.admin.updateReplicationPeerConfig(id, peerConfig);
  }

  /**
   * Removes a peer cluster and stops the replication to it.
   * @param id a short name that identifies the cluster
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#removeReplicationPeer(String)} instead
   */
  @Deprecated
  public void removePeer(String id) throws IOException {
    this.admin.removeReplicationPeer(id);
  }

  /**
   * Restart the replication stream to the specified peer.
   * @param id a short name that identifies the cluster
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#enableReplicationPeer(String)}
   *             instead
   */
  @Deprecated
  public void enablePeer(String id) throws IOException {
    this.admin.enableReplicationPeer(id);
  }

  /**
   * Stop the replication stream to the specified peer.
   * @param id a short name that identifies the cluster
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#disableReplicationPeer(String)}
   *             instead
   */
  @Deprecated
  public void disablePeer(String id) throws IOException {
    this.admin.disableReplicationPeer(id);
  }

  /**
   * Get the number of slave clusters the local cluster has.
   * @return number of slave clusters
   * @throws IOException
   * @deprecated
   */
  @Deprecated
  public int getPeersCount() throws IOException {
    return this.admin.listReplicationPeers().size();
  }

  /**
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#listReplicationPeers()} instead
   */
  @Deprecated
  public Map<String, ReplicationPeerConfig> listPeerConfigs() throws IOException {
    List<ReplicationPeerDescription> peers = this.admin.listReplicationPeers();
    Map<String, ReplicationPeerConfig> result = new TreeMap<>();
    for (ReplicationPeerDescription peer : peers) {
      result.put(peer.getPeerId(), peer.getPeerConfig());
    }
    return result;
  }

  /**
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#getReplicationPeerConfig(String)}
   *             instead
   */
  @Deprecated
  public ReplicationPeerConfig getPeerConfig(String id) throws IOException {
    return admin.getReplicationPeerConfig(id);
  }

  /**
   * Get the replicable table-cf config of the specified peer.
   * @param id a short name that identifies the cluster
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0,
   * use {@link #getPeerConfig(String)} instead.
   * */
  @Deprecated
  public String getPeerTableCFs(String id) throws IOException {
    ReplicationPeerConfig peerConfig = admin.getReplicationPeerConfig(id);
    return ReplicationPeerConfigUtil.convertToString(peerConfig.getTableCFsMap());
  }

  /**
   * Append the replicable table-cf config of the specified peer
   * @param id a short that identifies the cluster
   * @param tableCfs table-cfs config str
   * @throws ReplicationException
   * @throws IOException
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0,
   * use {@link #appendPeerTableCFs(String, Map)} instead.
   */
  @Deprecated
  public void appendPeerTableCFs(String id, String tableCfs) throws ReplicationException,
      IOException {
    appendPeerTableCFs(id, ReplicationPeerConfigUtil.parseTableCFsFromConfig(tableCfs));
  }

  /**
   * Append the replicable table-cf config of the specified peer
   * @param id a short that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   * @throws ReplicationException
   * @throws IOException
   */
  @Deprecated
  public void appendPeerTableCFs(String id, Map<TableName, ? extends Collection<String>> tableCfs)
      throws ReplicationException, IOException {
    this.admin.appendReplicationPeerTableCFs(id, copyTableCFs(tableCfs));
  }

  /**
   * Remove some table-cfs from table-cfs config of the specified peer
   * @param id a short name that identifies the cluster
   * @param tableCf table-cfs config str
   * @throws ReplicationException
   * @throws IOException
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0,
   * use {@link #removePeerTableCFs(String, Map)} instead.
   */
  @Deprecated
  public void removePeerTableCFs(String id, String tableCf) throws ReplicationException,
      IOException {
    removePeerTableCFs(id, ReplicationPeerConfigUtil.parseTableCFsFromConfig(tableCf));
  }

  /**
   * Remove some table-cfs from config of the specified peer
   * @param id a short name that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   * @throws ReplicationException
   * @throws IOException
   */
  @Deprecated
  public void removePeerTableCFs(String id, Map<TableName, ? extends Collection<String>> tableCfs)
      throws ReplicationException, IOException {
    this.admin.removeReplicationPeerTableCFs(id, copyTableCFs(tableCfs));
  }

  private Map<TableName, List<String>>
      copyTableCFs(Map<TableName, ? extends Collection<String>> tableCfs) {
    Map<TableName, List<String>> newTableCfs = new HashMap<>();
    if (tableCfs != null) {
      tableCfs.forEach(
        (table, cfs) -> newTableCfs.put(table, cfs != null ? Lists.newArrayList(cfs) : null));
    }
    return newTableCfs;
  }

  /**
   * Set the replicable table-cf config of the specified peer
   * @param id a short name that identifies the cluster
   * @param tableCfs the table and column-family list which will be replicated for this peer.
   * A map from tableName to column family names. An empty collection can be passed
   * to indicate replicating all column families. Pass null for replicating all table and column
   * families
   */
  @Deprecated
  public void setPeerTableCFs(String id, Map<TableName, ? extends Collection<String>> tableCfs)
      throws IOException {
    ReplicationPeerConfig peerConfig = getPeerConfig(id);
    peerConfig.setTableCFsMap(tableCfs);
    updatePeerConfig(id, peerConfig);
  }

  /**
   * Get the state of the specified peer cluster
   * @param id String format of the Short name that identifies the peer,
   * an IllegalArgumentException is thrown if it doesn't exist
   * @return true if replication is enabled to that peer, false if it isn't
   */
  @Deprecated
  public boolean getPeerState(String id) throws ReplicationException, IOException {
    List<ReplicationPeerDescription> peers = admin.listReplicationPeers(Pattern.compile(id));
    if (peers.isEmpty() || !id.equals(peers.get(0).getPeerId())) {
      throw new ReplicationPeerNotFoundException(id);
    }
    return peers.get(0).isEnabled();
  }

  @Override
  public void close() throws IOException {
    if (this.connection != null) {
      this.connection.close();
    }
    admin.close();
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
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#listReplicatedTableCFs()} instead
   */
  @Deprecated
  public List<HashMap<String, String>> listReplicated() throws IOException {
    List<HashMap<String, String>> replicationColFams = new ArrayList<>();
    admin.listReplicatedTableCFs().forEach(
      (tableCFs) -> {
        String table = tableCFs.getTable().getNameAsString();
        tableCFs.getColumnFamilyMap()
            .forEach(
              (cf, scope) -> {
                HashMap<String, String> replicationEntry = new HashMap<>();
                replicationEntry.put(TNAME, table);
                replicationEntry.put(CFNAME, cf);
                replicationEntry.put(REPLICATIONTYPE, REPLICATIONGLOBAL);
                replicationColFams.add(replicationEntry);
              });
      });
    return replicationColFams;
  }

  /**
   * Enable a table's replication switch.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#enableTableReplication(TableName)}
   *             instead
   */
  @Deprecated
  public void enableTableRep(final TableName tableName) throws IOException {
    admin.enableTableReplication(tableName);
  }

  /**
   * Disable a table's replication switch.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#disableTableReplication(TableName)}
   *             instead
   */
  @Deprecated
  public void disableTableRep(final TableName tableName) throws IOException {
    admin.disableTableReplication(tableName);
  }

  /**
   * @deprecated use {@link org.apache.hadoop.hbase.client.Admin#listReplicationPeers()} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
    return admin.listReplicationPeers();
  }
}
