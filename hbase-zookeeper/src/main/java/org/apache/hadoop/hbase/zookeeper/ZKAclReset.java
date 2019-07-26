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

package org.apache.hadoop.hbase.zookeeper;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * You may add the jaas.conf option
 *    -Djava.security.auth.login.config=/PATH/jaas.conf
 *
 * You may also specify -D to set options
 *    "hbase.zookeeper.quorum"    (it should be in hbase-site.xml)
 *    "zookeeper.znode.parent"    (it should be in hbase-site.xml)
 *
 * Use -set-acls to set the ACLs, no option to erase ACLs
 */
@InterfaceAudience.Private
public class ZKAclReset extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(ZKAclReset.class);

  private static void resetAcls(final ZKWatcher zkw, final String znode,
                                final boolean eraseAcls) throws Exception {
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, znode);
    if (children != null) {
      for (String child: children) {
        resetAcls(zkw, ZNodePaths.joinZNode(znode, child), eraseAcls);
      }
    }

    ZooKeeper zk = zkw.getRecoverableZooKeeper().getZooKeeper();
    if (eraseAcls) {
      LOG.info(" - erase ACLs for " + znode);
      zk.setACL(znode, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
    } else {
      LOG.info(" - set ACLs for " + znode);
      zk.setACL(znode, ZKUtil.createACL(zkw, znode, true), -1);
    }
  }

  private static void resetAcls(final Configuration conf, boolean eraseAcls)
      throws Exception {
    try (ZKWatcher zkw = new ZKWatcher(conf, "ZKAclReset", null)) {
      LOG.info((eraseAcls ? "Erase" : "Set") + " HBase ACLs for " +
              zkw.getQuorum() + " " + zkw.getZNodePaths().baseZNode);
      resetAcls(zkw, zkw.getZNodePaths().baseZNode, eraseAcls);
    }
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: hbase %s [options]%n", getClass().getName());
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help                Show this help and exit.");
    System.err.println("  -set-acls               Setup the hbase znode ACLs for a secure cluster");
    System.err.println();
    System.err.println("Examples:");
    System.err.println("  To reset the ACLs to the unsecure cluster behavior:");
    System.err.println("  hbase " + getClass().getName());
    System.err.println();
    System.err.println("  To reset the ACLs to the secure cluster behavior:");
    System.err.println("  hbase " + getClass().getName() + " -set-acls");
    System.exit(1);
  }

  @Override
  public int run(String[] args) throws Exception {
    boolean eraseAcls = true;

    for (String arg : args) {
      switch (arg) {
        case "-help": {
          printUsageAndExit();
          break;
        }
        case "-set-acls": {
          eraseAcls = false;
          break;
        }
        default: {
          printUsageAndExit();
          break;
        }
      }
    }

    resetAcls(getConf(), eraseAcls);
    return(0);
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new ZKAclReset(), args));
  }
}
