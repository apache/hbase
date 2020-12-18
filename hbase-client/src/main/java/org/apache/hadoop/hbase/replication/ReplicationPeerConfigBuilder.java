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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * For creating {@link ReplicationPeerConfig}.
 */
@InterfaceAudience.Public
public interface ReplicationPeerConfigBuilder {

  /**
   * Set the clusterKey which is the concatenation of the slave cluster's:
   * hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
   */
  ReplicationPeerConfigBuilder setClusterKey(String clusterKey);

  /**
   * Sets the ReplicationEndpoint plugin class for this peer.
   * @param replicationEndpointImpl a class implementing ReplicationEndpoint
   */
  ReplicationPeerConfigBuilder setReplicationEndpointImpl(String replicationEndpointImpl);

  /**
   * Sets a "raw" configuration property for this replication peer. For experts only.
   * @param key Configuration property key
   * @param value Configuration property value
   * @return {@code this}
   */
  @InterfaceAudience.Private
  ReplicationPeerConfigBuilder putConfiguration(String key, String value);

  /**
   * Removes a "raw" configuration property for this replication peer. For experts only.
   * @param key Configuration property key to ve removed
   * @return {@code this}
   */
  @InterfaceAudience.Private
  ReplicationPeerConfigBuilder removeConfiguration(String key);


  /**
   * Adds all of the provided "raw" configuration entries to {@code this}.
   * @param configuration A collection of raw configuration entries
   * @return {@code this}
   */
  @InterfaceAudience.Private
  default ReplicationPeerConfigBuilder putAllConfiguration(Map<String, String> configuration) {
    configuration.forEach(this::putConfiguration);
    return this;
  }

  /**
   * Sets the serialized peer configuration data
   * @return {@code this}
   */
  @InterfaceAudience.Private
  ReplicationPeerConfigBuilder putPeerData(byte[] key, byte[] value);

  /**
   * Sets all of the provided serialized peer configuration data.
   * @return {@code this}
   */
  @InterfaceAudience.Private
  default ReplicationPeerConfigBuilder putAllPeerData(Map<byte[], byte[]> peerData) {
    peerData.forEach(this::putPeerData);
    return this;
  }

  /**
   * Sets an explicit map of tables and column families in those tables that should be replicated
   * to the given peer. Use {@link #setReplicateAllUserTables(boolean)} to replicate all tables
   * to a peer.
   *
   * @param tableCFsMap A map from tableName to column family names. An empty collection can be
   *    passed to indicate replicating all column families.
   * @return {@code this}
   * @see #setReplicateAllUserTables(boolean)
   */
  ReplicationPeerConfigBuilder
      setTableCFsMap(Map<TableName, List<String>> tableCFsMap);

  /**
   * Sets a unique collection of HBase namespaces that should be replicated to this peer.
   * @param namespaces A set of namespaces to be replicated to this peer.
   * @return {@code this}
   */
  ReplicationPeerConfigBuilder setNamespaces(Set<String> namespaces);

  /**
   * Sets the speed, in bytes per second, for any one RegionServer to replicate data to the peer.
   * @param bandwidth Bytes per second
   * @return {@code this}.
   */
  ReplicationPeerConfigBuilder setBandwidth(long bandwidth);

  /**
   * Configures HBase to replicate all user tables (not system tables) to the peer. Default is
   * {@code true}.
   * @param replicateAllUserTables True if all user tables should be replicated, else false.
   * @return {@code this}
   */
  ReplicationPeerConfigBuilder setReplicateAllUserTables(boolean replicateAllUserTables);

  /**
   * Sets the mapping of table name to column families which should not be replicated. This
   * method sets state which is mutually exclusive to {@link #setTableCFsMap(Map)}. Invoking this
   * method is only relevant when all user tables are being replicated.
   *
   * @param tableCFsMap A mapping of table names to column families which should not be
   *    replicated. An empty list of column families implies all families for the table.
   * @return {@code this}.
   */
  ReplicationPeerConfigBuilder setExcludeTableCFsMap(Map<TableName, List<String>> tableCFsMap);

  /**
   * Sets the collection of namespaces which should not be replicated when all user tables are
   * configured to be replicated. This method sets state which is mutually exclusive to
   * {@link #setNamespaces(Set)}. Invoking this method is only relevant when all user tables are
   * being replicated.
   *
   * @param namespaces A set of namespaces whose tables should not be replicated.
   * @return {@code this}
   */
  ReplicationPeerConfigBuilder setExcludeNamespaces(Set<String> namespaces);

  /**
   * <p>
   * Sets whether we should preserve order when replicating, i.e, serial replication.
   * </p>
   * <p>
   * Default {@code false}.
   * </p>
   * @param serial {@code true} means preserve order, otherwise {@code false}.
   * @return {@code this}
   */
  ReplicationPeerConfigBuilder setSerial(boolean serial);

  /**
   * Builds the configuration object from the current state of {@code this}.
   * @return A {@link ReplicationPeerConfig} instance.
   */
  ReplicationPeerConfig build();
}
