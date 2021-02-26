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

package org.apache.hadoop.hbase.replication;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * A configuration for the replication peer cluster.
 */
@InterfaceAudience.Public
public class ReplicationPeerConfig {

  private String clusterKey;
  private String replicationEndpointImpl;
  private final Map<byte[], byte[]> peerData;
  private final Map<String, String> configuration;
  private Map<TableName, ? extends Collection<String>> tableCFsMap = null;
  private Set<String> namespaces = null;
  // Default value is true, means replicate all user tables to peer cluster.
  private boolean replicateAllUserTables = true;
  private Map<TableName, ? extends Collection<String>> excludeTableCFsMap = null;
  private Set<String> excludeNamespaces = null;
  private long bandwidth = 0;
  private final boolean serial;

  private ReplicationPeerConfig(ReplicationPeerConfigBuilderImpl builder) {
    this.clusterKey = builder.clusterKey;
    this.replicationEndpointImpl = builder.replicationEndpointImpl;
    this.peerData = Collections.unmodifiableMap(builder.peerData);
    this.configuration = Collections.unmodifiableMap(builder.configuration);
    this.tableCFsMap =
        builder.tableCFsMap != null ? unmodifiableTableCFsMap(builder.tableCFsMap) : null;
    this.namespaces =
        builder.namespaces != null ? Collections.unmodifiableSet(builder.namespaces) : null;
    this.replicateAllUserTables = builder.replicateAllUserTables;
    this.excludeTableCFsMap =
        builder.excludeTableCFsMap != null ? unmodifiableTableCFsMap(builder.excludeTableCFsMap)
            : null;
    this.excludeNamespaces =
        builder.excludeNamespaces != null ? Collections.unmodifiableSet(builder.excludeNamespaces)
            : null;
    this.bandwidth = builder.bandwidth;
    this.serial = builder.serial;
  }

  private Map<TableName, List<String>>
      unmodifiableTableCFsMap(Map<TableName, List<String>> tableCFsMap) {
    Map<TableName, List<String>> newTableCFsMap = new HashMap<>();
    tableCFsMap.forEach((table, cfs) -> newTableCFsMap.put(table,
      cfs != null ? Collections.unmodifiableList(cfs) : null));
    return Collections.unmodifiableMap(newTableCFsMap);
  }

  /**
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder} to create new ReplicationPeerConfig.
   */
  @Deprecated
  public ReplicationPeerConfig() {
    this.peerData = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    this.configuration = new HashMap<>(0);
    this.serial = false;
  }

  /**
   * Set the clusterKey which is the concatenation of the slave cluster's:
   * hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder#setClusterKey(String)} instead.
   */
  @Deprecated
  public ReplicationPeerConfig setClusterKey(String clusterKey) {
    this.clusterKey = clusterKey;
    return this;
  }

  /**
   * Sets the ReplicationEndpoint plugin class for this peer.
   * @param replicationEndpointImpl a class implementing ReplicationEndpoint
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder#setReplicationEndpointImpl(String)} instead.
   */
  @Deprecated
  public ReplicationPeerConfig setReplicationEndpointImpl(String replicationEndpointImpl) {
    this.replicationEndpointImpl = replicationEndpointImpl;
    return this;
  }

  public String getClusterKey() {
    return clusterKey;
  }

  public String getReplicationEndpointImpl() {
    return replicationEndpointImpl;
  }

  public Map<byte[], byte[]> getPeerData() {
    return peerData;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  public Map<TableName, List<String>> getTableCFsMap() {
    return (Map<TableName, List<String>>) tableCFsMap;
  }

  /**
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder#setTableCFsMap(Map)} instead.
   */
  @Deprecated
  public ReplicationPeerConfig setTableCFsMap(Map<TableName,
                                              ? extends Collection<String>> tableCFsMap) {
    this.tableCFsMap = tableCFsMap;
    return this;
  }

  public Set<String> getNamespaces() {
    return this.namespaces;
  }

  /**
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder#setNamespaces(Set)} instead.
   */
  @Deprecated
  public ReplicationPeerConfig setNamespaces(Set<String> namespaces) {
    this.namespaces = namespaces;
    return this;
  }

  public long getBandwidth() {
    return this.bandwidth;
  }

  /**
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder#setBandwidth(long)} instead.
   */
  @Deprecated
  public ReplicationPeerConfig setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
    return this;
  }

  public boolean replicateAllUserTables() {
    return this.replicateAllUserTables;
  }

  /**
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder#setReplicateAllUserTables(boolean)} instead.
   */
  @Deprecated
  public ReplicationPeerConfig setReplicateAllUserTables(boolean replicateAllUserTables) {
    this.replicateAllUserTables = replicateAllUserTables;
    return this;
  }

  public Map<TableName, List<String>> getExcludeTableCFsMap() {
    return (Map<TableName, List<String>>) excludeTableCFsMap;
  }

  /**
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder#setExcludeTableCFsMap(Map)} instead.
   */
  @Deprecated
  public ReplicationPeerConfig setExcludeTableCFsMap(Map<TableName,
                                              ? extends Collection<String>> tableCFsMap) {
    this.excludeTableCFsMap = tableCFsMap;
    return this;
  }

  public Set<String> getExcludeNamespaces() {
    return this.excludeNamespaces;
  }

  /**
   * @deprecated as release of 2.0.0, and it will be removed in 3.0.0. Use
   *             {@link ReplicationPeerConfigBuilder#setExcludeNamespaces(Set)} instead.
   */
  @Deprecated
  public ReplicationPeerConfig setExcludeNamespaces(Set<String> namespaces) {
    this.excludeNamespaces = namespaces;
    return this;
  }

  public static ReplicationPeerConfigBuilder newBuilder() {
    return new ReplicationPeerConfigBuilderImpl();
  }

  public boolean isSerial() {
    return serial;
  }

  public static ReplicationPeerConfigBuilder newBuilder(ReplicationPeerConfig peerConfig) {
    ReplicationPeerConfigBuilderImpl builder = new ReplicationPeerConfigBuilderImpl();
    builder.setClusterKey(peerConfig.getClusterKey())
      .setReplicationEndpointImpl(peerConfig.getReplicationEndpointImpl())
      .putAllPeerData(peerConfig.getPeerData()).putAllConfiguration(peerConfig.getConfiguration())
      .setTableCFsMap(peerConfig.getTableCFsMap()).setNamespaces(peerConfig.getNamespaces())
      .setReplicateAllUserTables(peerConfig.replicateAllUserTables())
      .setExcludeTableCFsMap(peerConfig.getExcludeTableCFsMap())
      .setExcludeNamespaces(peerConfig.getExcludeNamespaces())
      .setBandwidth(peerConfig.getBandwidth()).setSerial(peerConfig.isSerial());
    return builder;
  }

  static class ReplicationPeerConfigBuilderImpl implements ReplicationPeerConfigBuilder {

    private String clusterKey;

    private String replicationEndpointImpl;

    private Map<byte[], byte[]> peerData = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    private Map<String, String> configuration = new HashMap<>();

    private Map<TableName, List<String>> tableCFsMap = null;

    private Set<String> namespaces = null;

    // Default value is true, means replicate all user tables to peer cluster.
    private boolean replicateAllUserTables = true;

    private Map<TableName, List<String>> excludeTableCFsMap = null;

    private Set<String> excludeNamespaces = null;

    private long bandwidth = 0;

    private boolean serial = false;

    @Override
    public ReplicationPeerConfigBuilder setClusterKey(String clusterKey) {
      this.clusterKey = clusterKey;
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder setReplicationEndpointImpl(String replicationEndpointImpl) {
      this.replicationEndpointImpl = replicationEndpointImpl;
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder putConfiguration(String key, String value) {
      this.configuration.put(key, value);
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder removeConfiguration(String key) {
      this.configuration.remove(key);
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder putPeerData(byte[] key, byte[] value) {
      this.peerData.put(key, value);
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder
        setTableCFsMap(Map<TableName, List<String>> tableCFsMap) {
      this.tableCFsMap = tableCFsMap;
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder setNamespaces(Set<String> namespaces) {
      this.namespaces = namespaces;
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder setReplicateAllUserTables(boolean replicateAllUserTables) {
      this.replicateAllUserTables = replicateAllUserTables;
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder
        setExcludeTableCFsMap(Map<TableName, List<String>> excludeTableCFsMap) {
      this.excludeTableCFsMap = excludeTableCFsMap;
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder setExcludeNamespaces(Set<String> excludeNamespaces) {
      this.excludeNamespaces = excludeNamespaces;
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder setBandwidth(long bandwidth) {
      this.bandwidth = bandwidth;
      return this;
    }

    @Override
    public ReplicationPeerConfigBuilder setSerial(boolean serial) {
      this.serial = serial;
      return this;
    }

    @Override
    public ReplicationPeerConfig build() {
      // It would be nice to validate the configuration, but we have to work with "old" data
      // from ZK which makes it much more difficult.
      return new ReplicationPeerConfig(this);
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("clusterKey=").append(clusterKey).append(",");
    builder.append("replicationEndpointImpl=").append(replicationEndpointImpl).append(",");
    builder.append("replicateAllUserTables=").append(replicateAllUserTables).append(",");
    if (replicateAllUserTables) {
      if (excludeNamespaces != null) {
        builder.append("excludeNamespaces=").append(excludeNamespaces.toString()).append(",");
      }
      if (excludeTableCFsMap != null) {
        builder.append("excludeTableCFsMap=").append(excludeTableCFsMap.toString()).append(",");
      }
    } else {
      if (namespaces != null) {
        builder.append("namespaces=").append(namespaces.toString()).append(",");
      }
      if (tableCFsMap != null) {
        builder.append("tableCFs=").append(tableCFsMap.toString()).append(",");
      }
    }
    builder.append("bandwidth=").append(bandwidth).append(",");
    builder.append("serial=").append(serial);
    return builder.toString();
  }

  /**
   * Decide whether the table need replicate to the peer cluster
   * @param table name of the table
   * @return true if the table need replicate to the peer cluster
   */
  public boolean needToReplicate(TableName table) {
    return needToReplicate(table, null);
  }

  /**
   * Decide whether the passed family of the table need replicate to the peer cluster according to
   * this peer config.
   * @param table name of the table
   * @param family family name
   * @return true if (the family of) the table need replicate to the peer cluster.
   *         If passed family is null, return true if any CFs of the table need replicate;
   *         If passed family is not null, return true if the passed family need replicate.
   */
  public boolean needToReplicate(TableName table, byte[] family) {
    String namespace = table.getNamespaceAsString();
    if (replicateAllUserTables) {
      // replicate all user tables, but filter by exclude namespaces and table-cfs config
      if (excludeNamespaces != null && excludeNamespaces.contains(namespace)) {
        return false;
      }
      // trap here, must check existence first since HashMap allows null value.
      if (excludeTableCFsMap == null || !excludeTableCFsMap.containsKey(table)) {
        return true;
      }
      Collection<String> cfs = excludeTableCFsMap.get(table);
      // If cfs is null or empty then we can make sure that we do not need to replicate this table,
      // otherwise, we may still need to replicate the table but filter out some families.
      return cfs != null && !cfs.isEmpty()
        // If exclude-table-cfs contains passed family then we make sure that we do not need to
        // replicate this family.
        && (family == null || !cfs.contains(Bytes.toString(family)));
    } else {
      // Not replicate all user tables, so filter by namespaces and table-cfs config
      if (namespaces == null && tableCFsMap == null) {
        return false;
      }
      // First filter by namespaces config
      // If table's namespace in peer config, all the tables data are applicable for replication
      if (namespaces != null && namespaces.contains(namespace)) {
        return true;
      }
      // If table-cfs contains this table then we can make sure that we need replicate some CFs of
      // this table. Further we need all CFs if tableCFsMap.get(table) is null or empty.
      return tableCFsMap != null && tableCFsMap.containsKey(table)
        && (family == null || CollectionUtils.isEmpty(tableCFsMap.get(table))
        // If table-cfs must contain passed family then we need to replicate this family.
        || tableCFsMap.get(table).contains(Bytes.toString(family)));
    }
  }
}
