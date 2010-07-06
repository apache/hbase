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
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Internal HBase utility class for ZooKeeper.
 * 
 * Contains only static methods and constants.
 */
public class ZKUtil {
  private static final Log LOG = LogFactory.getLog(ZKUtil.class);

  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  private static final char ZNODE_PATH_SEPARATOR = '/';
  
  /**
   * Creates a new connection to ZooKeeper, pulling settings and quorum config
   * from the specified configuration object using methods from {@link ZKConfig}.
   * 
   * Sets the connection status monitoring watcher to the specified watcher.
   * 
   * @param conf configuration to pull quorum and other settings from
   * @param watcher watcher to monitor connection changes
   * @return connection to zookeeper
   * @throws IOException if unable to connect to zk or config problem
   */
  public static ZooKeeper connect(Configuration conf, Watcher watcher)
  throws IOException {
    Properties properties = ZKConfig.makeZKProps(conf);
    String quorum = ZKConfig.getZKQuorumServersString(properties);
    if(quorum == null) {
      throw new IOException("Unable to determine ZooKeeper quorum");
    }
    int timeout = conf.getInt("zookeeper.session.timeout", 60 * 1000);
    LOG.debug("Opening connection to ZooKeeper with quorum (" + quorum + ")");
    return new ZooKeeper(quorum, timeout, watcher);
  }

  /**
   * Join the prefix znode name with the suffix znode name to generate a proper
   * full znode name.
   * 
   * Assumes prefix does not end with slash and suffix does not begin with it.
   * 
   * @param prefix beginning of znode name
   * @param suffix ending of znode name
   * @return result of properly joining prefix with suffix
   */
  public static String joinZNode(String prefix, String suffix) {
    return prefix + ZNODE_PATH_SEPARATOR + suffix;
  }

  /**
   * Watch the specified znode for delete/create/change events.  The watcher is
   * set whether or not the node exists.  If the node already exists, the method
   * returns true.  If the node does not exist, the method returns false.
   * 
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return true if znode exists, false if does not exist or error
   */
  public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode) {
    try {
      Stat s = zkw.getZooKeeper().exists(znode, zkw);
      zkw.debug("Set watcher on existing znode (" + znode + ")");
      return s != null ? true : false;
    } catch (KeeperException e) {
      zkw.warn("Unable to set watcher on znode (" + znode + ")", e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      zkw.warn("Unable to set watcher on znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return false;
    }
  }
  
  /**
   * Get the data at the specified znode and set a watch.
   * 
   * Returns the data and sets a watch if the node exists.  Returns null and no
   * watch is set if the node does not exist or there is an exception.
   * 
   * @param zkw zk reference
   * @param znode path of node
   * @return data of the specified znode, or null
   */
  public static byte [] getDataAndWatch(ZooKeeperWatcher zkw, String znode) {
    try {
      byte [] data = zkw.getZooKeeper().getData(znode, zkw, null);
      zkw.debug("Retrieved " + data.length + " bytes of data from znode (" + 
          znode + ") and set a watcher");
      return data;
    } catch (KeeperException.NoNodeException e) {
      zkw.debug("Unable to get data of znode (" + znode + ") " +
          "because node does not exist (not an error)");
      return null;
    } catch (KeeperException e) {
      zkw.warn("Unable to get data of znode (" + znode + ")", e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      zkw.warn("Unable to get data of znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return null;
    }
  }
  
  /**
   * Get the data at the specified znode, deserialize it as an HServerAddress,
   * and set a watch.
   * 
   * Returns the data as a server address and sets a watch if the node exists.
   * Returns null and no watch is set if the node does not exist or there is an
   * exception.
   * 
   * @param zkw zk reference
   * @param znode path of node
   * @return data of the specified node as a server address, or null
   */
  public static HServerAddress getDataAsAddress(ZooKeeperWatcher zkw, 
      String znode) {
    byte [] data = getDataAndWatch(zkw, znode);
    if(data == null) {
      return null;
    }
    String addrString = Bytes.toString(data);
    zkw.debug("Read server address from znode (" + znode + "): " + addrString);
    return new HServerAddress(addrString);
  }
}