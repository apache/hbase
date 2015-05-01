/**
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

package org.apache.hadoop.hbase.zookeeper;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;

/**
 * You may add the jaas.conf option
 *    -Djava.security.auth.login.config=/PATH/jaas.conf
 *
 * You may also specify -D to set options
 *    "hbase.zookeeper.quorum"    (it should be in hbase-site.xml)
 *    "zookeeper.znode.parent"    (it should be in hbase-site.xml)
 */
@InterfaceAudience.Private
public class ZkAclReset extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ZkAclReset.class);

  private static final int ZK_SESSION_TIMEOUT_DEFAULT = 5 * 1000;

  private static class ZkWatcher implements Watcher {
    public ZkWatcher() {
    }

    @Override
    public void process(WatchedEvent event) {
      LOG.info("Received ZooKeeper Event, " +
          "type=" + event.getType() + ", " +
          "state=" + event.getState() + ", " +
          "path=" + event.getPath());
    }
  }

  private static void resetAcls(final ZooKeeper zk, final String znode)
      throws Exception {
    List<String> children = zk.getChildren(znode, false);
    if (children != null) {
      for (String child: children) {
        resetAcls(zk, znode + '/' + child);
      }
    }
    LOG.info(" - reset acl for " + znode);
    zk.setACL(znode, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
  }

  private static void resetAcls(final String quorumServers, final int zkTimeout, final String znode)
      throws Exception {
    ZooKeeper zk = new ZooKeeper(quorumServers, zkTimeout, new ZkWatcher());
    try {
      resetAcls(zk, znode);
    } finally {
      zk.close();
    }
  }

  private void resetHBaseAcls(final Configuration conf) throws Exception {
    String quorumServers = conf.get("hbase.zookeeper.quorum", HConstants.LOCALHOST);
    int sessionTimeout = conf.getInt("zookeeper.session.timeout", ZK_SESSION_TIMEOUT_DEFAULT);
    String znode = conf.get("zookeeper.znode.parent", HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    if (quorumServers == null) {
      LOG.error("Unable to load hbase.zookeeper.quorum (try with: -conf hbase-site.xml)");
      return;
    }

    LOG.info("Reset HBase ACLs for " + quorumServers + " " + znode);
    resetAcls(quorumServers, sessionTimeout, znode);
  }


  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    resetHBaseAcls(conf);
    return(0);
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new ZkAclReset(), args));
  }
}
