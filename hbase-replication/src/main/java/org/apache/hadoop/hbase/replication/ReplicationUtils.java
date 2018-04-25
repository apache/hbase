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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for replication.
 */
@InterfaceAudience.Private
public final class ReplicationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationUtils.class);

  public static final String REPLICATION_ATTR_NAME = "__rep__";

  public static final String REMOTE_WAL_DIR_NAME = "remoteWALs";

  private ReplicationUtils() {
  }

  public static Configuration getPeerClusterConfiguration(ReplicationPeerConfig peerConfig,
      Configuration baseConf) throws ReplicationException {
    Configuration otherConf;
    try {
      otherConf = HBaseConfiguration.createClusterConf(baseConf, peerConfig.getClusterKey());
    } catch (IOException e) {
      throw new ReplicationException("Can't get peer configuration for peer " + peerConfig, e);
    }

    if (!peerConfig.getConfiguration().isEmpty()) {
      CompoundConfiguration compound = new CompoundConfiguration();
      compound.add(otherConf);
      compound.addStringMap(peerConfig.getConfiguration());
      return compound;
    }

    return otherConf;
  }

  public static void removeAllQueues(ReplicationQueueStorage queueStorage, String peerId)
      throws ReplicationException {
    for (ServerName replicator : queueStorage.getListOfReplicators()) {
      List<String> queueIds = queueStorage.getAllQueues(replicator);
      for (String queueId : queueIds) {
        ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
        if (queueInfo.getPeerId().equals(peerId)) {
          queueStorage.removeQueue(replicator, queueId);
        }
      }
      queueStorage.removeReplicatorIfQueueIsEmpty(replicator);
    }
  }

  private static boolean isCollectionEqual(Collection<String> c1, Collection<String> c2) {
    if (c1 == null) {
      return c2 == null;
    }
    if (c2 == null) {
      return false;
    }
    return c1.size() == c2.size() && c1.containsAll(c2);
  }

  private static boolean isNamespacesEqual(Set<String> ns1, Set<String> ns2) {
    return isCollectionEqual(ns1, ns2);
  }

  private static boolean isTableCFsEqual(Map<TableName, List<String>> tableCFs1,
      Map<TableName, List<String>> tableCFs2) {
    if (tableCFs1 == null) {
      return tableCFs2 == null;
    }
    if (tableCFs2 == null) {
      return false;
    }
    if (tableCFs1.size() != tableCFs2.size()) {
      return false;
    }
    for (Map.Entry<TableName, List<String>> entry1 : tableCFs1.entrySet()) {
      TableName table = entry1.getKey();
      if (!tableCFs2.containsKey(table)) {
        return false;
      }
      List<String> cfs1 = entry1.getValue();
      List<String> cfs2 = tableCFs2.get(table);
      if (!isCollectionEqual(cfs1, cfs2)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isNamespacesAndTableCFsEqual(ReplicationPeerConfig rpc1,
      ReplicationPeerConfig rpc2) {
    if (rpc1.replicateAllUserTables() != rpc2.replicateAllUserTables()) {
      return false;
    }
    if (rpc1.replicateAllUserTables()) {
      return isNamespacesEqual(rpc1.getExcludeNamespaces(), rpc2.getExcludeNamespaces()) &&
        isTableCFsEqual(rpc1.getExcludeTableCFsMap(), rpc2.getExcludeTableCFsMap());
    } else {
      return isNamespacesEqual(rpc1.getNamespaces(), rpc2.getNamespaces()) &&
        isTableCFsEqual(rpc1.getTableCFsMap(), rpc2.getTableCFsMap());
    }
  }

  /**
   * @param c Configuration to look at
   * @return True if replication for bulk load data is enabled.
   */
  public static boolean isReplicationForBulkLoadDataEnabled(final Configuration c) {
    return c.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
      HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
  }

  /**
   * Returns whether we should replicate the given table.
   */
  public static boolean contains(ReplicationPeerConfig peerConfig, TableName tableName) {
    String namespace = tableName.getNamespaceAsString();
    if (peerConfig.replicateAllUserTables()) {
      // replicate all user tables, but filter by exclude namespaces and table-cfs config
      Set<String> excludeNamespaces = peerConfig.getExcludeNamespaces();
      if (excludeNamespaces != null && excludeNamespaces.contains(namespace)) {
        return false;
      }
      Map<TableName, List<String>> excludedTableCFs = peerConfig.getExcludeTableCFsMap();
      // trap here, must check existence first since HashMap allows null value.
      if (excludedTableCFs == null || !excludedTableCFs.containsKey(tableName)) {
        return true;
      }
      List<String> cfs = excludedTableCFs.get(tableName);
      // if cfs is null or empty then we can make sure that we do not need to replicate this table,
      // otherwise, we may still need to replicate the table but filter out some families.
      return cfs != null && !cfs.isEmpty();
    } else {
      // Not replicate all user tables, so filter by namespaces and table-cfs config
      Set<String> namespaces = peerConfig.getNamespaces();
      Map<TableName, List<String>> tableCFs = peerConfig.getTableCFsMap();

      if (namespaces == null && tableCFs == null) {
        return false;
      }

      // First filter by namespaces config
      // If table's namespace in peer config, all the tables data are applicable for replication
      if (namespaces != null && namespaces.contains(namespace)) {
        return true;
      }
      return tableCFs != null && tableCFs.containsKey(tableName);
    }
  }

  public static FileSystem getRemoteWALFileSystem(Configuration conf, String remoteWALDir)
      throws IOException {
    return new Path(remoteWALDir).getFileSystem(conf);
  }

  public static Path getRemoteWALDirForPeer(String remoteWALDir, String peerId) {
    return new Path(remoteWALDir, peerId);
  }

  public static Path getRemoteWALDirForPeer(Path remoteWALDir, String peerId) {
    return new Path(remoteWALDir, peerId);
  }

  /**
   * Do the sleeping logic
   * @param msg Why we sleep
   * @param sleepForRetries the base sleep time.
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @param maxRetriesMultiplier the max retry multiplier
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  public static boolean sleepForRetries(String msg, long sleepForRetries, int sleepMultiplier,
      int maxRetriesMultiplier) {
    try {
      LOG.trace("{}, sleeping {} times {}", msg, sleepForRetries, sleepMultiplier);
      Thread.sleep(sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping between retries");
      Thread.currentThread().interrupt();
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }
}
