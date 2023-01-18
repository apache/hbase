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
package org.apache.hadoop.hbase.chaos.actions;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that tries to restart the active namenode.
 */
public class RestartActiveNameNodeAction extends RestartActionBaseAction {
  private static final Logger LOG = LoggerFactory.getLogger(RestartActiveNameNodeAction.class);

  // Value taken from org.apache.hadoop.ha.ActiveStandbyElector.java, variable :- LOCK_FILENAME
  private static final String ACTIVE_NN_LOCK_NAME = "ActiveStandbyElectorLock";

  // Value taken from org.apache.hadoop.ha.ZKFailoverController.java
  // variable :- ZK_PARENT_ZNODE_DEFAULT and ZK_PARENT_ZNODE_KEY
  private static final String ZK_PARENT_ZNODE_DEFAULT = "/hadoop-ha";
  private static final String ZK_PARENT_ZNODE_KEY = "ha.zookeeper.parent-znode";

  public RestartActiveNameNodeAction(long sleepTime) {
    super(sleepTime);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    getLogger().info("Performing action: Restart active namenode");

    final String hadoopHAZkNode;
    String activeNamenode = null;
    int activeNamenodePort = -1;
    try (final DistributedFileSystem dfs = HdfsActionUtils.createDfs(getConf())) {
      final Configuration conf = dfs.getConf();
      hadoopHAZkNode = conf.get(ZK_PARENT_ZNODE_KEY, ZK_PARENT_ZNODE_DEFAULT);
      final String nameServiceID = DFSUtil.getNamenodeNameServiceId(conf);

      if (!HAUtil.isHAEnabled(conf, nameServiceID)) {
        getLogger().info("HA for HDFS is not enabled; skipping");
        return;
      }
      try (final ZKWatcher zkw = new ZKWatcher(conf, "get-active-namenode", null)) {
        final RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();
        // If hadoopHAZkNode == '/', pass '' instead because then joinZNode will return '//' as a
        // prefix
        // which zk doesn't like as a prefix on the path.
        final String hadoopHAZkNodePath = ZNodePaths.joinZNode(
          (hadoopHAZkNode != null && hadoopHAZkNode.equals("/")) ? "" : hadoopHAZkNode,
          nameServiceID);
        final List<String> subChildren =
          Optional.ofNullable(ZKUtil.listChildrenNoWatch(zkw, hadoopHAZkNodePath))
            .orElse(Collections.emptyList());
        for (final String eachEntry : subChildren) {
          if (!eachEntry.contains(ACTIVE_NN_LOCK_NAME)) {
            continue;
          }
          byte[] data =
            rzk.getData(ZNodePaths.joinZNode(hadoopHAZkNodePath, ACTIVE_NN_LOCK_NAME), false, null);
          ActiveNodeInfo proto = ActiveNodeInfo.parseFrom(data);
          activeNamenode = proto.getHostname();
          activeNamenodePort = proto.getPort();
        }
      }
    }

    if (activeNamenode == null) {
      getLogger().info("No active Name node found in zookeeper under '{}'", hadoopHAZkNode);
      return;
    }

    getLogger().info("Found Active NameNode host: {}", activeNamenode);
    final ServerName activeNNHost = ServerName.valueOf(activeNamenode, activeNamenodePort, -1L);
    getLogger().info("Restarting Active NameNode: {}", activeNamenode);
    restartNameNode(activeNNHost, this.sleepTime);
  }
}
