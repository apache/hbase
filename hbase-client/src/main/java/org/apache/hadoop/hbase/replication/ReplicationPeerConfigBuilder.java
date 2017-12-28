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

  ReplicationPeerConfigBuilder putConfiguration(String key, String value);

  default ReplicationPeerConfigBuilder putAllConfiguration(Map<String, String> configuration) {
    configuration.forEach(this::putConfiguration);
    return this;
  }

  ReplicationPeerConfigBuilder putPeerData(byte[] key, byte[] value);

  default ReplicationPeerConfigBuilder putAllPeerData(Map<byte[], byte[]> peerData) {
    peerData.forEach(this::putPeerData);
    return this;
  }

  ReplicationPeerConfigBuilder
      setTableCFsMap(Map<TableName, List<String>> tableCFsMap);

  ReplicationPeerConfigBuilder setNamespaces(Set<String> namespaces);

  ReplicationPeerConfigBuilder setBandwidth(long bandwidth);

  ReplicationPeerConfigBuilder setReplicateAllUserTables(boolean replicateAllUserTables);

  ReplicationPeerConfigBuilder setExcludeTableCFsMap(Map<TableName, List<String>> tableCFsMap);

  ReplicationPeerConfigBuilder setExcludeNamespaces(Set<String> namespaces);

  ReplicationPeerConfig build();
}
