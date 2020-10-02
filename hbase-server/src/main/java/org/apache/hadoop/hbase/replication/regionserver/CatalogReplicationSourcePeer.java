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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The 'peer' used internally by Catalog Region Replicas Replication Source.
 * The Replication system has 'peer' baked into its core so though we do not need 'peering', we
 * need a 'peer' and its configuration else the replication system breaks at a few locales.
 * Set "hbase.region.replica.catalog.replication" if you want to change the configured endpoint.
 */
@InterfaceAudience.Private
class CatalogReplicationSourcePeer extends ReplicationPeerImpl {
  /**
   * @param clusterKey Usually the UUID from zk passed in by caller as a String.
   */
  CatalogReplicationSourcePeer(Configuration configuration, String clusterKey, String peerId) {
    super(configuration, ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER + "_catalog",
      true,
      ReplicationPeerConfig.newBuilder().
        setClusterKey(clusterKey).
        setReplicationEndpointImpl(
          configuration.get("hbase.region.replica.catalog.replication",
            RegionReplicaReplicationEndpoint.class.getName())).
        setBandwidth(0). // '0' means no bandwidth.
        setSerial(false).
        build());
  }
}
