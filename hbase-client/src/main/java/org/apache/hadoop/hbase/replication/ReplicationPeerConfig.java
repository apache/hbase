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

import com.google.common.base.Splitter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A configuration for the replication peer cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ReplicationPeerConfig {

  private String clusterKey;
  private String replicationEndpointImpl;
  private final Map<byte[], byte[]> peerData;
  private final Map<String, String> configuration;
  private Map<TableName, ? extends Collection<String>> tableCFsMap = null;
  private long bandwidth = 0;

  public static final String HBASE_REPLICATION_PEER_BASE_CONFIG =
    "hbase.replication.peer.default.config";

  public ReplicationPeerConfig() {
    this.peerData = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    this.configuration = new HashMap<String, String>(0);
  }

  /**
   * Set the clusterKey which is the concatenation of the slave cluster's:
   *          hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
   */
  public ReplicationPeerConfig setClusterKey(String clusterKey) {
    this.clusterKey = clusterKey;
    return this;
  }

  /**
   * Sets the ReplicationEndpoint plugin class for this peer.
   * @param replicationEndpointImpl a class implementing ReplicationEndpoint
   */
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

  public void setTableCFsMap(Map<TableName, ? extends Collection<String>> tableCFsMap) {
    this.tableCFsMap = tableCFsMap;
  }

  public long getBandwidth() {
    return this.bandwidth;
  }

  public ReplicationPeerConfig setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
    return this;
  }

  /**
   * Helper method to add base peer configs from Configuration to ReplicationPeerConfig
   * if not present in latter.
   *
   * This merges the user supplied peer configuration
   * {@link org.apache.hadoop.hbase.replication.ReplicationPeerConfig} with peer configs
   * provided as property hbase.replication.peer.base.configs in hbase configuration.
   * Expected format for this hbase configuration is "k1=v1;k2=v2,v2_1". Original value
   * of conf is retained if already present in ReplicationPeerConfig.
   *
   * @param conf Configuration
   */
  public void addBasePeerConfigsIfNotPresent(Configuration conf) {
    String basePeerConfigs = conf.get(HBASE_REPLICATION_PEER_BASE_CONFIG, "");

    if (basePeerConfigs.length() != 0) {
      Map<String, String> basePeerConfigMap = Splitter.on(';').trimResults().omitEmptyStrings()
        .withKeyValueSeparator("=").split(basePeerConfigs);
      for (Map.Entry<String,String> entry : basePeerConfigMap.entrySet()) {
        String configName = entry.getKey();
        String configValue = entry.getValue();
        // Only override if base config does not exist in existing replication peer configs
        if (!this.getConfiguration().containsKey(configName)) {
          this.getConfiguration().put(configName, configValue);
        }
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("clusterKey=").append(clusterKey).append(",");
    builder.append("replicationEndpointImpl=").append(replicationEndpointImpl);
    if (tableCFsMap != null) {
      builder.append(tableCFsMap.toString()).append(",");
    }
    builder.append("bandwidth=").append(bandwidth);
    return builder.toString();
  }
}
