/*
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

/**
 * This class is used to upgrade TableCFs from HBase 1.0, 1.1, 1.2, 1.3 to HBase 1.4 or 2.x.
 * It will be removed in HBase 3.x. See HBASE-11393
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TableCFsUpdater extends ReplicationStateZKBase {

  private static final Log LOG = LogFactory.getLog(TableCFsUpdater.class);

  public TableCFsUpdater(ZooKeeperWatcher zookeeper,
                         Configuration conf, Abortable abortable) {
    super(zookeeper, conf, abortable);
  }

  public void update() {
    List<String> znodes = null;
    try {
      znodes = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
    } catch (KeeperException e) {
      LOG.error("Failed to get peers znode", e);
    }
    if (znodes != null) {
      for (String peerId : znodes) {
        if (!update(peerId)) {
          LOG.error("upgrade tableCFs failed for peerId=" + peerId);
        }
      }
    }
  }

  public boolean update(String peerId) {
    String tableCFsNode = getTableCFsNode(peerId);
    try {
      if (ZKUtil.checkExists(zookeeper, tableCFsNode) != -1) {
        String peerNode = getPeerNode(peerId);
        ReplicationPeerConfig rpc = getReplicationPeerConig(peerNode);
        // We only need to copy data from tableCFs node to rpc Node the first time hmaster start.
        if (rpc.getTableCFsMap() == null || rpc.getTableCFsMap().size() == 0) {
          // we copy TableCFs node into PeerNode
          LOG.info("copy tableCFs into peerNode:" + peerId);
          ZooKeeperProtos.TableCF[] tableCFs =
                  ReplicationSerDeHelper.parseTableCFs(
                          ZKUtil.getData(this.zookeeper, tableCFsNode));
          rpc.setTableCFsMap(ReplicationSerDeHelper.convert2Map(tableCFs));
          ZKUtil.setData(this.zookeeper, peerNode,
                  ReplicationSerDeHelper.toByteArray(rpc));
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

  private ReplicationPeerConfig getReplicationPeerConig(String peerNode)
          throws KeeperException, InterruptedException {
    byte[] data = null;
    data = ZKUtil.getData(this.zookeeper, peerNode);
    if (data == null) {
      LOG.error("Could not get configuration for " +
              "peer because it doesn't exist. peer=" + peerNode);
      return null;
    }
    try {
      return ReplicationSerDeHelper.parsePeerFrom(data);
    } catch (DeserializationException e) {
      LOG.warn("Failed to parse cluster key from peer=" + peerNode);
      return null;
    }
  }

  private static void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase org.apache.hadoop.hbase.replication.master.TableCFsUpdater [options]");
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help    Show this help and exit.");
    System.err.println("  update      Copy table-cfs to replication peer config");
    System.err.println();
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      printUsageAndExit();
    }
    if (args[0].equals("-help") || args[0].equals("-h")) {
      printUsageAndExit();
    } else if (args[0].equals("update")) {
      Configuration conf = HBaseConfiguration.create();
      ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "TableCFsUpdater", null);
      try {
        TableCFsUpdater tableCFsUpdater = new TableCFsUpdater(zkw, conf, null);
        tableCFsUpdater.update();
      } finally {
        zkw.close();
      }
    } else {
      printUsageAndExit();
    }
  }
}
