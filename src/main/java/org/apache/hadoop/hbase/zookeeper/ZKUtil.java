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
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

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

  //
  // Helper methods
  //

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

  //
  // Existence checks and watches
  //

  /**
   * Watch the specified znode for delete/create/change events.  The watcher is
   * set whether or not the node exists.  If the node already exists, the method
   * returns true.  If the node does not exist, the method returns false.
   *
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return true if znode exists, false if does not exist or error
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
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

  //
  // Znode listings
  //

  /**
   * Lists the children znodes of the specified znode.  Also sets a watch on
   * the specified znode which will capture a NodeDeleted event on the specified
   * znode as well as NodeChildrenChanged if any children of the specified znode
   * are created or deleted.
   *
   * Returns null if the specified node does not exist.  Otherwise returns a
   * list of children of the specified node.  If the node exists but it has no
   * children, an empty list will be returned.
   *
   * @param zkw zk reference
   * @param znode path of node to list and watch children of
   * @returns list of children of the specified node, an empty list if the node
   *          exists but has no children, and null if the node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenAndWatchForNewChildren(
      ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      List<String> children = zkw.getZooKeeper().getChildren(znode, zkw);
      return children;
    } catch(KeeperException.NoNodeException ke) {
      zkw.debug("Unable to list children of znode (" + znode + ") " +
          "because node does not exist (not an error)");
      return null;
    } catch (KeeperException e) {
      zkw.warn("Unable to list children of znode (" + znode + ")", e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      zkw.warn("Unable to list children of znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Checks if the specified znode has any children.  Sets no watches.
   *
   * Returns true if the node exists and has children.  Returns false if the
   * node does not exist or if the node does not have any children.
   *
   * Used during master initialization to determine if the master is a
   * failed-over-to master or the first master during initial cluster startup.
   * If the directory for regionserver ephemeral nodes is empty then this is
   * a cluster startup, if not then it is not cluster startup.
   *
   * @param zkw zk reference
   * @param znode path of node to check for children of
   * @return true if node has children, false if not or node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean nodeHasChildren(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      return !zkw.getZooKeeper().getChildren(znode, null).isEmpty();
    } catch(KeeperException.NoNodeException ke) {
      zkw.debug("Unable to list children of znode (" + znode + ") " +
      "because node does not exist (not an error)");
      return false;
    } catch (KeeperException e) {
      zkw.warn("Unable to list children of znode (" + znode + ")", e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      zkw.warn("Unable to list children of znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return false;
    }
  }

  //
  // Data retrieval
  //

  /**
   * Get the data at the specified znode and set a watch.
   *
   * Returns the data and sets a watch if the node exists.  Returns null and no
   * watch is set if the node does not exist or there is an exception.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @return data of the specified znode, or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte [] getDataAndWatch(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
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
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static HServerAddress getDataAsAddress(ZooKeeperWatcher zkw,
      String znode)
  throws KeeperException {
    byte [] data = getDataAndWatch(zkw, znode);
    if(data == null) {
      return null;
    }
    String addrString = Bytes.toString(data);
    zkw.debug("Read server address from znode (" + znode + "): " + addrString);
    return new HServerAddress(addrString);
  }

  /**
   * Set the specified znode to be an ephemeral node carrying the specified
   * server address.  Used by masters for their ephemeral node and regionservers
   * for their ephemeral node.
   *
   * If the node is created successfully, a watcher is also set on the node.
   *
   * If the node is not created successfully because it already exists, this
   * method will also set a watcher on the node.
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param address server address
   * @return true if address set, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean setAddressAndWatch(ZooKeeperWatcher zkw,
      String znode, HServerAddress address)
  throws KeeperException {
    return createEphemeralNodeAndWatch(zkw, znode,
        Bytes.toBytes(address.toString()));
  }
  /**
   *
   * Set the specified znode to be an ephemeral node carrying the specified
   * data.
   *
   * If the node is created successfully, a watcher is also set on the node.
   *
   * If the node is not created successfully because it already exists, this
   * method will also set a watcher on the node.
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @return true if node created, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean createEphemeralNodeAndWatch(ZooKeeperWatcher zkw,
      String znode, byte [] data)
  throws KeeperException {
    try {
      zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException nee) {
      if(!watchAndCheckExists(zkw, znode)) {
        // It did exist but now it doesn't, try again
        return createEphemeralNodeAndWatch(zkw, znode, data);
      }
      return false;
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
    }
    return true;
  }

  /**
   * Creates the specified node, if the node does not exist.  Does not set a
   * watch and fails silently if the node already exists.
   *
   * The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createIfNotExists(ZooKeeperWatcher zkw,
      String znode)
  throws KeeperException {
    try {
      zkw.getZooKeeper().create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } catch(KeeperException.NodeExistsException nee) {
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }
}