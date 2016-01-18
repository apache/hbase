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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

/**
 * Tool to migrate zookeeper data of older hbase versions(&lt;0.95.0) to PB.
 */
public class ZKDataMigrator extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ZKDataMigrator.class);

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
    justification="Intended")
  public int run(String[] as) throws Exception {
    Configuration conf = getConf();
    ZooKeeperWatcher zkw = null;
    try {
      zkw = new ZooKeeperWatcher(getConf(), "Migrate ZK data to PB.",
        new ZKDataMigratorAbortable());
      if (ZKUtil.checkExists(zkw, zkw.baseZNode) == -1) {
        LOG.info("No hbase related data available in zookeeper. returning..");
        return 0;
      }
      List<String> children = ZKUtil.listChildrenNoWatch(zkw, zkw.baseZNode);
      if (children == null) {
        LOG.info("No child nodes to mirgrate. returning..");
        return 0;
      }
      String childPath = null;
      for (String child : children) {
        childPath = ZKUtil.joinZNode(zkw.baseZNode, child);
        if (child.equals(conf.get("zookeeper.znode.rootserver", "root-region-server"))) {
          // -ROOT- region no longer present from 0.95.0, so we can remove this
          // znode
          ZKUtil.deleteNodeRecursively(zkw, childPath);
          // TODO delete root table path from file system.
        } else if (child.equals(conf.get("zookeeper.znode.rs", "rs"))) {
          // Since there is no live region server instance during migration, we
          // can remove this znode as well.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.draining.rs", "draining"))) {
          // If we want to migrate to 0.95.0 from older versions we need to stop
          // the existing cluster. So there wont be any draining servers so we
          // can
          // remove it.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.master", "master"))) {
          // Since there is no live master instance during migration, we can
          // remove this znode as well.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.backup.masters", "backup-masters"))) {
          // Since there is no live backup master instances during migration, we
          // can remove this znode as well.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.state", "shutdown"))) {
          // shutdown node is not present from 0.95.0 onwards. Its renamed to
          // "running". We can delete it.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.unassigned", "unassigned"))) {
          // Any way during clean cluster startup we will remove all unassigned
          // region nodes. we can delete all children nodes as well. This znode
          // is
          // renamed to "regions-in-transition" from 0.95.0 onwards.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.tableEnableDisable", "table"))
            || child.equals(conf.get("zookeeper.znode.masterTableEnableDisable", "table"))) {
          checkAndMigrateTableStatesToPB(zkw);
        } else if (child.equals(conf.get("zookeeper.znode.masterTableEnableDisable92",
          "table92"))) {
          // This is replica of table states from tableZnode so we can remove
          // this.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.splitlog", "splitlog"))) {
          // This znode no longer available from 0.95.0 onwards, we can remove
          // it.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.replication", "replication"))) {
          checkAndMigrateReplicationNodesToPB(zkw);
        } else if (child.equals(conf.get("zookeeper.znode.clusterId", "hbaseid"))) {
          // it will be re-created by master.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION)) {
          // not needed as it is transient.
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        } else if (child.equals(conf.get("zookeeper.znode.acl.parent", "acl"))) {
          // it will be re-created when hbase:acl is re-opened
          ZKUtil.deleteNodeRecursively(zkw, childPath);
        }
      }
    } catch (Exception e) {
      LOG.error("Got exception while updating znodes ", e);
      throw new IOException(e);
    } finally {
      if (zkw != null) {
        zkw.close();
      }
    }
    return 0;
  }

  private void checkAndMigrateTableStatesToPB(ZooKeeperWatcher zkw) throws KeeperException,
      InterruptedException {
    List<String> tables = ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
    if (tables == null) {
      LOG.info("No table present to migrate table state to PB. returning..");
      return;
    }
    for (String table : tables) {
      String znode = ZKUtil.joinZNode(zkw.tableZNode, table);
      // Delete -ROOT- table state znode since its no longer present in 0.95.0
      // onwards.
      if (table.equals("-ROOT-") || table.equals(".META.")) {
        ZKUtil.deleteNode(zkw, znode);
        continue;
      }
      byte[] data = ZKUtil.getData(zkw, znode);
      if (ProtobufUtil.isPBMagicPrefix(data)) continue;
      ZooKeeperProtos.Table.Builder builder = ZooKeeperProtos.Table.newBuilder();
      builder.setState(ZooKeeperProtos.Table.State.valueOf(Bytes.toString(data)));
      data = ProtobufUtil.prependPBMagic(builder.build().toByteArray());
      ZKUtil.setData(zkw, znode, data);
    }
  }

  private void checkAndMigrateReplicationNodesToPB(ZooKeeperWatcher zkw) throws KeeperException,
      InterruptedException {
    String replicationZnodeName = getConf().get("zookeeper.znode.replication", "replication");
    String replicationPath = ZKUtil.joinZNode(zkw.baseZNode, replicationZnodeName);
    List<String> replicationZnodes = ZKUtil.listChildrenNoWatch(zkw, replicationPath);
    if (replicationZnodes == null) {
      LOG.info("No replication related znodes present to migrate. returning..");
      return;
    }
    for (String child : replicationZnodes) {
      String znode = ZKUtil.joinZNode(replicationPath, child);
      if (child.equals(getConf().get("zookeeper.znode.replication.peers", "peers"))) {
        List<String> peers = ZKUtil.listChildrenNoWatch(zkw, znode);
        if (peers == null || peers.isEmpty()) {
          LOG.info("No peers present to migrate. returning..");
          continue;
        }
        checkAndMigratePeerZnodesToPB(zkw, znode, peers);
      } else if (child.equals(getConf().get("zookeeper.znode.replication.state", "state"))) {
        // This is no longer used in >=0.95.x
        ZKUtil.deleteNodeRecursively(zkw, znode);
      } else if (child.equals(getConf().get("zookeeper.znode.replication.rs", "rs"))) {
        List<String> rsList = ZKUtil.listChildrenNoWatch(zkw, znode);
        if (rsList == null || rsList.isEmpty()) continue;
        for (String rs : rsList) {
          checkAndMigrateQueuesToPB(zkw, znode, rs);
        }
      }
    }
  }

  private void checkAndMigrateQueuesToPB(ZooKeeperWatcher zkw, String znode, String rs)
      throws KeeperException, NoNodeException, InterruptedException {
    String rsPath = ZKUtil.joinZNode(znode, rs);
    List<String> peers = ZKUtil.listChildrenNoWatch(zkw, rsPath);
    if (peers == null || peers.isEmpty()) return;
    String peerPath = null;
    for (String peer : peers) {
      peerPath = ZKUtil.joinZNode(rsPath, peer);
      List<String> files = ZKUtil.listChildrenNoWatch(zkw, peerPath);
      if (files == null || files.isEmpty()) continue;
      String filePath = null;
      for (String file : files) {
        filePath = ZKUtil.joinZNode(peerPath, file);
        byte[] data = ZKUtil.getData(zkw, filePath);
        if (data == null || Bytes.equals(data, HConstants.EMPTY_BYTE_ARRAY)) continue;
        if (ProtobufUtil.isPBMagicPrefix(data)) continue;
        ZKUtil.setData(zkw, filePath,
          ZKUtil.positionToByteArray(Long.parseLong(Bytes.toString(data))));
      }
    }
  }

  private void checkAndMigratePeerZnodesToPB(ZooKeeperWatcher zkw, String znode,
      List<String> peers) throws KeeperException, NoNodeException, InterruptedException {
    for (String peer : peers) {
      String peerZnode = ZKUtil.joinZNode(znode, peer);
      byte[] data = ZKUtil.getData(zkw, peerZnode);
      if (!ProtobufUtil.isPBMagicPrefix(data)) {
        migrateClusterKeyToPB(zkw, peerZnode, data);
      }
      String peerStatePath = ZKUtil.joinZNode(peerZnode,
        getConf().get("zookeeper.znode.replication.peers.state", "peer-state"));
      if (ZKUtil.checkExists(zkw, peerStatePath) != -1) {
        data = ZKUtil.getData(zkw, peerStatePath);
        if (ProtobufUtil.isPBMagicPrefix(data)) continue;
        migratePeerStateToPB(zkw, data, peerStatePath);
      }
    }
  }

  private void migrateClusterKeyToPB(ZooKeeperWatcher zkw, String peerZnode, byte[] data)
      throws KeeperException, NoNodeException {
    ReplicationPeer peer = ZooKeeperProtos.ReplicationPeer.newBuilder()
        .setClusterkey(Bytes.toString(data)).build();
    ZKUtil.setData(zkw, peerZnode, ProtobufUtil.prependPBMagic(peer.toByteArray()));
  }

  private void migratePeerStateToPB(ZooKeeperWatcher zkw, byte[] data,
 String peerStatePath)
      throws KeeperException, NoNodeException {
    String state = Bytes.toString(data);
    if (ZooKeeperProtos.ReplicationState.State.ENABLED.name().equals(state)) {
      ZKUtil.setData(zkw, peerStatePath, ReplicationStateZKBase.ENABLED_ZNODE_BYTES);
    } else if (ZooKeeperProtos.ReplicationState.State.DISABLED.name().equals(state)) {
      ZKUtil.setData(zkw, peerStatePath, ReplicationStateZKBase.DISABLED_ZNODE_BYTES);
    }
  }

  public static void main(String args[]) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new ZKDataMigrator(), args));
  }

  static class ZKDataMigratorAbortable implements Abortable {
    private boolean aborted = false;

    @Override
    public void abort(String why, Throwable e) {
      LOG.error("Got aborted with reason: " + why + ", and error: " + e);
      this.aborted = true;
    }

    @Override
    public boolean isAborted() {
      return this.aborted;
    }
  }
}
