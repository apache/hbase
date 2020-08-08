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

package org.apache.hadoop.hbase.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.regionserver.RegionReplicaReplicationEndpoint;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Similar to {@link RegionReplicaUtil} but for the server side
 */
@InterfaceAudience.Private
public class ServerRegionReplicaUtil extends RegionReplicaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ServerRegionReplicaUtil.class);

  /**
   * Whether asynchronous WAL replication to the secondary region replicas is enabled or not.
   * If this is enabled, a replication peer named "region_replica_replication" will be created
   * which will tail the logs and replicate the mutatations to region replicas for tables that
   * have region replication &gt; 1. If this is enabled once, disabling this replication also
   * requires disabling the replication peer using shell or {@link Admin} java class.
   * Replication to secondary region replicas works over standard inter-cluster replication.·
   */
  public static final String REGION_REPLICA_REPLICATION_CONF_KEY
    = "hbase.region.replica.replication.enabled";
  private static final boolean DEFAULT_REGION_REPLICA_REPLICATION = false;
  private static final String REGION_REPLICA_REPLICATION_PEER = "region_replica_replication";

  /**
   * Enables or disables refreshing store files of secondary region replicas when the memory is
   * above the global memstore lower limit. Refreshing the store files means that we will do a file
   * list of the primary regions store files, and pick up new files. Also depending on the store
   * files, we can drop some memstore contents which will free up memory.
   */
  public static final String REGION_REPLICA_STORE_FILE_REFRESH
    = "hbase.region.replica.storefile.refresh";
  private static final boolean DEFAULT_REGION_REPLICA_STORE_FILE_REFRESH = true;

  /**
   * The multiplier to use when we want to refresh a secondary region instead of flushing a primary
   * region. Default value assumes that for doing the file refresh, the biggest secondary should be
   * 4 times bigger than the biggest primary.
   */
  public static final String REGION_REPLICA_STORE_FILE_REFRESH_MEMSTORE_MULTIPLIER
    = "hbase.region.replica.storefile.refresh.memstore.multiplier";
  private static final double DEFAULT_REGION_REPLICA_STORE_FILE_REFRESH_MEMSTORE_MULTIPLIER = 4;

  /**
   * Returns the regionInfo object to use for interacting with the file system.
   * @return An RegionInfo object to interact with the filesystem
   */
  public static RegionInfo getRegionInfoForFs(RegionInfo regionInfo) {
    if (regionInfo == null) {
      return null;
    }
    return RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo);
  }

  /**
   * Returns whether this region replica can accept writes.
   * @param region the HRegion object
   * @return whether the replica is read only
   */
  public static boolean isReadOnly(HRegion region) {
    return region.getTableDescriptor().isReadOnly()
      || !isDefaultReplica(region.getRegionInfo());
  }

  /**
   * Returns whether to replay the recovered edits to flush the results.
   * Currently secondary region replicas do not replay the edits, since it would
   * cause flushes which might affect the primary region. Primary regions even opened
   * in read only mode should replay the edits.
   * @param region the HRegion object
   * @return whether recovered edits should be replayed.
   */
  public static boolean shouldReplayRecoveredEdits(HRegion region) {
    return isDefaultReplica(region.getRegionInfo());
  }

  /**
   * Returns a StoreFileInfo from the given FileStatus. Secondary replicas refer to the
   * files of the primary region, so an HFileLink is used to construct the StoreFileInfo. This
   * way ensures that the secondary will be able to continue reading the store files even if
   * they are moved to archive after compaction
   * @throws IOException
   */
  public static StoreFileInfo getStoreFileInfo(Configuration conf, FileSystem fs,
      RegionInfo regionInfo, RegionInfo regionInfoForFs, String familyName, Path path)
      throws IOException {

    // if this is a primary region, just return the StoreFileInfo constructed from path
    if (RegionInfo.COMPARATOR.compare(regionInfo, regionInfoForFs) == 0) {
      return new StoreFileInfo(conf, fs, path, true);
    }

    // else create a store file link. The link file does not exists on filesystem though.
    if (HFileLink.isHFileLink(path) || StoreFileInfo.isHFile(path)) {
      HFileLink link = HFileLink
          .build(conf, regionInfoForFs.getTable(), regionInfoForFs.getEncodedName(), familyName,
              path.getName());
      return new StoreFileInfo(conf, fs, link.getFileStatus(fs), link);
    } else if (StoreFileInfo.isReference(path)) {
      Reference reference = Reference.read(fs, path);
      Path referencePath = StoreFileInfo.getReferredToFile(path);
      if (HFileLink.isHFileLink(referencePath)) {
        // HFileLink Reference
        HFileLink link = HFileLink.buildFromHFileLinkPattern(conf, referencePath);
        return new StoreFileInfo(conf, fs, link.getFileStatus(fs), reference, link);
      } else {
        // Reference
        HFileLink link = HFileLink
            .build(conf, regionInfoForFs.getTable(), regionInfoForFs.getEncodedName(), familyName,
                path.getName());
        return new StoreFileInfo(conf, fs, link.getFileStatus(fs), reference);
      }
    } else {
      throw new IOException("path=" + path + " doesn't look like a valid StoreFile");
    }
  }

  /**
   * Create replication peer for replicating to region replicas if needed.
   * @param conf configuration to use
   * @throws IOException
   */
  public static void setupRegionReplicaReplication(Configuration conf) throws IOException {
    if (!isRegionReplicaReplicationEnabled(conf)) {
      return;
    }

    try (Connection connection = ConnectionFactory.createConnection(conf);
      Admin admin = connection.getAdmin()) {
      ReplicationPeerConfig peerConfig = null;
      try {
        peerConfig = admin.getReplicationPeerConfig(REGION_REPLICA_REPLICATION_PEER);
      } catch (ReplicationPeerNotFoundException e) {
        LOG.warn(
          "Region replica replication peer id=" + REGION_REPLICA_REPLICATION_PEER + " not exist",
          e);
      }

      if (peerConfig == null) {
        LOG.info("Region replica replication peer id=" + REGION_REPLICA_REPLICATION_PEER
          + " not exist. Creating...");
        peerConfig = new ReplicationPeerConfig();
        peerConfig.setClusterKey(ZKConfig.getZooKeeperClusterKey(conf));
        peerConfig.setReplicationEndpointImpl(RegionReplicaReplicationEndpoint.class.getName());
        admin.addReplicationPeer(REGION_REPLICA_REPLICATION_PEER, peerConfig);
      }
    }
  }

  public static boolean isRegionReplicaReplicationEnabled(Configuration conf) {
    return conf.getBoolean(REGION_REPLICA_REPLICATION_CONF_KEY,
      DEFAULT_REGION_REPLICA_REPLICATION);
  }

  public static boolean isRegionReplicaWaitForPrimaryFlushEnabled(Configuration conf) {
    return conf.getBoolean(REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY,
      DEFAULT_REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH);
  }

  public static boolean isRegionReplicaStoreFileRefreshEnabled(Configuration conf) {
    return conf.getBoolean(REGION_REPLICA_STORE_FILE_REFRESH,
      DEFAULT_REGION_REPLICA_STORE_FILE_REFRESH);
  }

  public static double getRegionReplicaStoreFileRefreshMultiplier(Configuration conf) {
    return conf.getDouble(REGION_REPLICA_STORE_FILE_REFRESH_MEMSTORE_MULTIPLIER,
      DEFAULT_REGION_REPLICA_STORE_FILE_REFRESH_MEMSTORE_MULTIPLIER);
  }

  /**
   * Return the peer id used for replicating to secondary region replicas
   */
  public static String getReplicationPeerId() {
    return REGION_REPLICA_REPLICATION_PEER;
  }

}
