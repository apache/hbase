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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.regionserver.RegionReplicaReplicationEndpoint;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 * Similar to {@link RegionReplicaUtil} but for the server side
 */
public class ServerRegionReplicaUtil extends RegionReplicaUtil {

  /**
   * Whether asynchronous WAL replication to the secondary region replicas is enabled or not.
   * If this is enabled, a replication peer named "region_replica_replication" will be created
   * which will tail the logs and replicate the mutatations to region replicas for tables that
   * have region replication > 1. If this is enabled once, disabling this replication also
   * requires disabling the replication peer using shell or ReplicationAdmin java class.
   * Replication to secondary region replicas works over standard inter-cluster replication.·
   * So replication, if disabled explicitly, also has to be enabled by setting "hbase.replication"·
   * to true for this feature to work.
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
   * @return An HRegionInfo object to interact with the filesystem
   */
  public static HRegionInfo getRegionInfoForFs(HRegionInfo regionInfo) {
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
    return region.getTableDesc().isReadOnly()
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
      HRegionInfo regionInfo, HRegionInfo regionInfoForFs, String familyName, Path path)
      throws IOException {

    // if this is a primary region, just return the StoreFileInfo constructed from path
    if (regionInfo.equals(regionInfoForFs)) {
      return new StoreFileInfo(conf, fs, path);
    }

    // else create a store file link. The link file does not exists on filesystem though.
    HFileLink link = HFileLink.build(conf, regionInfoForFs.getTable(),
            regionInfoForFs.getEncodedName(), familyName, path.getName());

    if (StoreFileInfo.isReference(path)) {
      Reference reference = Reference.read(fs, path);
      return new StoreFileInfo(conf, fs, link.getFileStatus(fs), reference);
    }

    return new StoreFileInfo(conf, fs, link.getFileStatus(fs), link);
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
    ReplicationAdmin repAdmin = new ReplicationAdmin(conf);
    try {
      if (repAdmin.getPeerConfig(REGION_REPLICA_REPLICATION_PEER) == null) {
        ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
        peerConfig.setClusterKey(ZKUtil.getZooKeeperClusterKey(conf));
        peerConfig.setReplicationEndpointImpl(RegionReplicaReplicationEndpoint.class.getName());
        repAdmin.addPeer(REGION_REPLICA_REPLICATION_PEER, peerConfig, null);
      }
    } catch (ReplicationException ex) {
      throw new IOException(ex);
    } finally {
      repAdmin.close();
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
