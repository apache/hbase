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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMasterReplicationPeerSource;
import org.apache.hadoop.hbase.master.MetricsMasterReplicationSourceFactory;
import org.apache.hadoop.hbase.replication.ReplicationExpectedTableUtil;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically counts source-cluster tables that should replicate to each peer (replication scope
 * and peer configuration) but are missing on the peer HBase cluster, and publishes the count to
 * master JMX ({@code peer.<peerId>.missingTargetTableCount}).
 */
@InterfaceAudience.Private
public class ReplicationMissingTargetTablesChore extends ScheduledChore {

  private static final Logger LOG =
    LoggerFactory.getLogger(ReplicationMissingTargetTablesChore.class);

  public static final String ENABLED_KEY =
    "hbase.master.replication.missing.target.tables.chore.enabled";
  public static final boolean DEFAULT_ENABLED = true;

  public static final String PERIOD_KEY =
    "hbase.master.replication.missing.target.tables.chore.period";
  public static final int DEFAULT_PERIOD = 5 * 60 * 1000;

  private final MasterServices master;
  private final Configuration conf;
  private final MetricsMasterReplicationSourceFactory metricsFactory;
  private Set<String> lastPeerIds = new HashSet<>();

  public ReplicationMissingTargetTablesChore(Stoppable stopper, MasterServices master,
    Configuration conf) {
    super("ReplicationMissingTargetTablesChore", stopper, conf.getInt(PERIOD_KEY, DEFAULT_PERIOD));
    this.master = master;
    this.conf = conf;
    this.metricsFactory =
      CompatibilitySingletonFactory.getInstance(MetricsMasterReplicationSourceFactory.class);
  }

  @Override
  protected void chore() {
    if (!master.isActiveMaster()) {
      return;
    }
    ReplicationPeerManager peerManager = master.getReplicationPeerManager();
    if (peerManager == null) {
      return;
    }
    try {
      Collection<TableDescriptor> tableDescriptors =
        master.getTableDescriptors().getAll().values();
      List<ReplicationPeerDescription> peers = peerManager.listPeers(null);
      Set<String> currentPeerIds = new HashSet<>();
      for (ReplicationPeerDescription peer : peers) {
        String peerId = peer.getPeerId();
        currentPeerIds.add(peerId);
        MetricsMasterReplicationPeerSource peerMetrics = metricsFactory.getPeerSource(peerId);
        if (!peer.isEnabled() || !isHBaseInterClusterReplicationPeer(peer.getPeerConfig())) {
          peerMetrics.setMissingTargetTableCount(0L);
          continue;
        }
        ReplicationPeerConfig peerConfig = peer.getPeerConfig();
        if (StringUtils.isBlank(peerConfig.getClusterKey())) {
          peerMetrics.setMissingTargetTableCount(0L);
          continue;
        }
        try {
          Configuration peerClusterConf =
            ReplicationPeerConfigUtil.getPeerClusterConfiguration(conf, peerConfig);
          Set<TableName> expected =
            ReplicationExpectedTableUtil.getExpectedTables(tableDescriptors, peerConfig);
          int missing =
            ReplicationExpectedTableUtil.countMissingTargetTables(expected, peerClusterConf);
          peerMetrics.setMissingTargetTableCount(missing);
          if (missing > 0 && LOG.isDebugEnabled()) {
            Set<TableName> missingTables =
              ReplicationExpectedTableUtil.getMissingTargetTables(expected, peerClusterConf);
            LOG.debug("Peer {} is missing {} expected target table(s): {}", peerId, missing,
              missingTables);
          }
        } catch (IOException e) {
          LOG.warn("Unable to check missing target tables for replication peer {}", peerId, e);
        }
      }
      for (String removedPeerId : lastPeerIds) {
        if (!currentPeerIds.contains(removedPeerId)) {
          metricsFactory.getPeerSource(removedPeerId).clear();
        }
      }
      lastPeerIds = currentPeerIds;
    } catch (IOException e) {
      LOG.warn("Unable to refresh missing target table metrics", e);
    }
  }

  private static boolean isHBaseInterClusterReplicationPeer(ReplicationPeerConfig peerConfig) {
    String impl = peerConfig.getReplicationEndpointImpl();
    return impl == null || impl.isEmpty()
      || HBaseInterClusterReplicationEndpoint.class.getName().equals(impl);
  }
}
