/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.EmptyWatcher;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Internal HBase utility class for ZooKeeper.
 *
 * <p>Contains only static methods and constants.
 *
 * <p>Methods all throw {@link KeeperException} if there is an unexpected
 * zookeeper exception, so callers of these methods must handle appropriately.
 * If ZK is required for the operation, the server will need to be aborted.
 */
public class ZKUtil {
  private static final Log LOG = LogFactory.getLog(ZKUtil.class);

  private ZKUtil() {
  }

  /**
   * Waits for HBase installation's base (parent) znode to become available.
   * @throws IOException on ZK errors
   */
  public static void waitForBaseZNode(Configuration conf) throws IOException {
    LOG.info("Waiting until the base znode is available");
    String parentZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);

    Properties properties = HQuorumPeer.makeZKProps(conf);
    ZooKeeper zk = new ZooKeeper(HQuorumPeer.getZKQuorumServersString(properties),
        conf.getInt(HConstants.ZOOKEEPER_SESSION_TIMEOUT,
        HConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT), EmptyWatcher.instance);

    final int maxTimeMs = 10000;
    final int maxNumAttempts = maxTimeMs / HConstants.SOCKET_RETRY_WAIT_MS;

    KeeperException keeperEx = null;
    try {
      try {
        for (int attempt = 0; attempt < maxNumAttempts; ++attempt) {
          try {
            if (zk.exists(parentZNode, false) != null) {
              LOG.info("Parent znode exists: " + parentZNode);
              keeperEx = null;
              break;
            }
          } catch (KeeperException e) {
            keeperEx = e;
          }
          Threads.sleepWithoutInterrupt(HConstants.SOCKET_RETRY_WAIT_MS);
        }
      } finally {
        zk.close();
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    if (keeperEx != null) {
      throw new IOException(keeperEx);
    }
  }

}
