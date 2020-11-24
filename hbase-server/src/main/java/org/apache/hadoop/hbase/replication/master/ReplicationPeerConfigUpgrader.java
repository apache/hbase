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
package org.apache.hadoop.hbase.replication.master;

import static org.apache.hadoop.hbase.replication.ZKReplicationPeerStorage.PEERS_ZNODE;
import static org.apache.hadoop.hbase.replication.ZKReplicationPeerStorage.PEERS_ZNODE_DEFAULT;
import static org.apache.hadoop.hbase.replication.ZKReplicationStorageBase.REPLICATION_ZNODE;
import static org.apache.hadoop.hbase.replication.ZKReplicationStorageBase.REPLICATION_ZNODE_DEFAULT;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to upgrade TableCFs from HBase 1.0, 1.1, 1.2, 1.3 to HBase 1.4 or 2.x. It will
 * be removed in HBase 3.x. See HBASE-11393
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReplicationPeerConfigUpgrader{

  private static final String TABLE_CFS_ZNODE = "zookeeper.znode.replication.peers.tableCFs";
  private static final String TABLE_CFS_ZNODE_DEFAULT = "tableCFs";

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationPeerConfigUpgrader.class);
  private final Configuration conf;
  private final ZKWatcher zookeeper;
  private final ReplicationPeerStorage peerStorage;

  public ReplicationPeerConfigUpgrader(ZKWatcher zookeeper, Configuration conf) {
    this.zookeeper = zookeeper;
    this.conf = conf;
    this.peerStorage = ReplicationStorageFactory.getReplicationPeerStorage(zookeeper, conf);
  }

  public void upgrade() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      Admin admin = conn.getAdmin();
      admin.listReplicationPeers().forEach((peerDesc) -> {
        String peerId = peerDesc.getPeerId();
        ReplicationPeerConfig peerConfig = peerDesc.getPeerConfig();
        if ((peerConfig.getNamespaces() != null && !peerConfig.getNamespaces().isEmpty())
            || (peerConfig.getTableCFsMap() != null && !peerConfig.getTableCFsMap().isEmpty())) {
          peerConfig.setReplicateAllUserTables(false);
          try {
            admin.updateReplicationPeerConfig(peerId, peerConfig);
          } catch (Exception e) {
            LOG.error("Failed to upgrade replication peer config for peerId=" + peerId, e);
          }
        }
      });
    }
  }

  public void copyTableCFs() throws ReplicationException {
    for (String peerId : peerStorage.listPeerIds()) {
      if (!copyTableCFs(peerId)) {
        LOG.error("upgrade tableCFs failed for peerId=" + peerId);
      }
    }
  }

  protected String getTableCFsNode(String peerId) {
    String replicationZNode = ZNodePaths.joinZNode(zookeeper.getZNodePaths().baseZNode,
      conf.get(REPLICATION_ZNODE, REPLICATION_ZNODE_DEFAULT));
    String peersZNode =
        ZNodePaths.joinZNode(replicationZNode, conf.get(PEERS_ZNODE, PEERS_ZNODE_DEFAULT));
    return ZNodePaths.joinZNode(peersZNode,
      ZNodePaths.joinZNode(peerId, conf.get(TABLE_CFS_ZNODE, TABLE_CFS_ZNODE_DEFAULT)));
  }

  public boolean copyTableCFs(String peerId) throws ReplicationException {
    String tableCFsNode = getTableCFsNode(peerId);
    try {
      if (ZKUtil.checkExists(zookeeper, tableCFsNode) != -1) {
        ReplicationPeerConfig rpc = peerStorage.getPeerConfig(peerId);
        // We only need to copy data from tableCFs node to rpc Node the first time hmaster start.
        if (rpc.getTableCFsMap() == null || rpc.getTableCFsMap().isEmpty()) {
          // we copy TableCFs node into PeerNode
          LOG.info("Copy table ColumnFamilies into peer=" + peerId);
          ReplicationProtos.TableCF[] tableCFs =
              ReplicationPeerConfigUtil.parseTableCFs(ZKUtil.getData(this.zookeeper, tableCFsNode));
          if (tableCFs != null && tableCFs.length > 0) {
            rpc.setTableCFsMap(ReplicationPeerConfigUtil.convert2Map(tableCFs));
            peerStorage.updatePeerConfig(peerId, rpc);
          }
        } else {
          LOG.info("No tableCFs in peerNode:" + peerId);
        }
      }
    } catch (KeeperException e) {
      LOG.warn("NOTICE!! Update peerId failed, peerId=" + peerId, e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn("NOTICE!! Update peerId failed, peerId=" + peerId, e);
      return false;
    } catch (IOException e) {
      LOG.warn("NOTICE!! Update peerId failed, peerId=" + peerId, e);
      return false;
    }
    return true;
  }

  private static void printUsageAndExit() {
    System.err.printf(
      "Usage: hbase org.apache.hadoop.hbase.replication.master.ReplicationPeerConfigUpgrader"
          + " [options]");
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help      Show this help and exit.");
    System.err.println("  copyTableCFs  Copy table-cfs to replication peer config");
    System.err.println("  upgrade           Upgrade replication peer config to new format");
    System.err.println();
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      printUsageAndExit();
    }
    if (args[0].equals("-help") || args[0].equals("-h")) {
      printUsageAndExit();
    } else if (args[0].equals("copyTableCFs")) {
      Configuration conf = HBaseConfiguration.create();
      try (ZKWatcher zkw = new ZKWatcher(conf, "ReplicationPeerConfigUpgrader", null)) {
        ReplicationPeerConfigUpgrader tableCFsUpdater =
            new ReplicationPeerConfigUpgrader(zkw, conf);
        tableCFsUpdater.copyTableCFs();
      }
    } else if (args[0].equals("upgrade")) {
      Configuration conf = HBaseConfiguration.create();
      try (ZKWatcher zkw = new ZKWatcher(conf, "ReplicationPeerConfigUpgrader", null)) {
        ReplicationPeerConfigUpgrader upgrader = new ReplicationPeerConfigUpgrader(zkw, conf);
        upgrader.upgrade();
      }
    } else {
      printUsageAndExit();
    }
  }
}
